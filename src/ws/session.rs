//! `Feed` の背後にある、再接続するセッション。所有者は 1 つ、タスクなし、
//! ロックなし。
//!
//! 状態機械 (すべて `Session` の中にあり、future のローカル変数には何もない。
//! それが `next` をキャンセル安全にしている):
//!
//! ```text
//!            +--------------------- バックオフ経過 ----------------------+
//!            v                                                          |
//!   Idle -> Dialing(fut) --ok--> Connected{sent} --切断--> Waiting{until}
//!            |                     |                                    ^
//!            +--終端エラー--> Closed <--- close() / 終端エラー            |
//!            +--再試行可能なエラー----------------------------------------+
//! ```
//!
//! # 不変条件
//! - `Connected` の間、`sent.subscribed` はこの接続に書き込みを始め、
//!   サーバに拒否されていない subscribe の集合と等しい。再接続のたびに
//!   空から始まる。
//! - コマンドは、同じポーリングの中で `Pacer::take` が通った直後にだけ書く。
//!   ペーサの予約は持たないので、キャンセルで失われるものも、遅れて使われる
//!   ものもない。
//! - `sent.last` が残るのは、その書き込みから `REJECTION_WINDOW` を過ぎた後に、
//!   ソケットに未読のフレームがないことを `next` がまだ観測していない間だけ
//!   である。

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::codec::{Codec, Decoded};
use super::lease::{Auth, LeaseSchedule, TokenLease, TokenSource};
use super::{Disconnect, DisconnectReason, FeedChannel, Pacer, PrivateChannel, PublicChannel};
use crate::client::{Authenticated, Client, Inner, Limits};
use crate::error::{
    ApiGroup, ContractViolation, Delivery, Error, ErrorCode, Phase, ProtocolError,
    ProtocolErrorKind, Result, TransportError, TransportErrorKind,
};
use crate::transport::ws::{self, WsConnectError, WsStream};

/// エラーに載せる操作名。ハンドシェイクのパス。トークンは決して含まない。
pub(super) const OPERATION: &str = "/v1";

/// 拒否の窓の長さ。書き込みからこの時間を過ぎた後に、ソケットに未読の
/// フレームがないことを `next` が観測したら、窓を閉じる。それ以後に読む
/// 拒否は、どのコマンドのものともしない。取引所は不正なコマンドにすぐ答え、
/// コマンドはこの間隔でしか送らないので、これより古いコマンドへの返答は
/// もう来ない。静かなチャネル (たとえば `orderEvents`) の subscribe は
/// データで裏付けられるまで長く待つので、窓を閉じないと、後のサーバ発の
/// 拒否がその購読を落とす。
///
/// 窓は、拒否が届いた時刻ではなく、フィードが観測できることで閉じる。
/// フィードは `next` の中でしかソケットを読まないので、`next` が呼ばれて
/// いない間に届いたフレームの到着時刻は分からない。未読のフレームが残って
/// いる限り、その中の拒否は窓の中に届いたかもしれないので、窓を閉じない。
pub(crate) const REJECTION_WINDOW: Duration = Pacer::INTERVAL;

/// 1 回の接続試行。`State::Dialing` に保存され、キャンセルの後も続きから
/// 進む。
type DialFuture = Pin<Box<dyn Future<Output = Dialed> + Send>>;

/// 接続試行の結果。
struct Dialed {
    result: core::result::Result<WsStream, DialFailure>,
    token: TokenSource,
}

enum DialFailure {
    /// フィードを終える: `Api`、`Contract`、メンテナンス以外の `Protocol`、
    /// この試行で発行したトークンのハンドシェイクでの拒否。
    Terminal(Error),
    /// `Disconnected` を返してバックオフ後に再試行する。
    Retry(DisconnectReason),
    /// 再利用したトークンをハンドシェイクが 401/403/404 で拒んだ。リースは
    /// 死んだものとして捨て、次の試行が取り直す。新しいトークンの拒否は
    /// 接続試行の中で `Terminal` になるので、ここには来ない。
    ReusedTokenRejected { http_status: u16 },
}

pub(crate) struct Session<C: FeedChannel> {
    inner: Arc<Inner>,
    auth: Auth,
    /// 呼び出し側の意図。`Limits::max_subscriptions` で有界。
    desired: BTreeMap<<C as Codec>::Key, C>,
    state: State<C>,
    /// 最後に維持された接続以降の失敗した試行の数。`disconnected` でだけ
    /// 増えるので、接続成功時に 0 でなければ直前に `Disconnected` を返して
    /// おり、`Reconnected` を返す番である。
    attempt: u32,
}

enum State<C: FeedChannel> {
    Idle,
    Dialing(DialFuture),
    /// tokio-tungstenite の接続は大きいので Box に入れる。
    Connected(Box<Connection>, Sent<C>),
    Waiting {
        until: Instant,
    },
    Closed,
}

/// この接続に伝えた内容。新しい接続のたびに空にリセットされ、それが
/// 再接続で望ましい集合を再生する仕組みである。
struct Sent<C: FeedChannel> {
    subscribed: BTreeMap<<C as Codec>::Key, C>,
    /// 最後に書いたコマンドと書いた時刻のうち、まだ返答で裏付けられておらず、
    /// 拒否の窓 (`REJECTION_WINDOW`) が閉じていないもの。これがある間に
    /// 読んだサーバの拒否は、これに帰属する。新しいキーの subscribe はその
    /// キーの最初のデータイベントで裏付けられる。拒否は 1 つのコマンドに
    /// しか帰属しない。
    last: Option<(Instant, Written<C>)>,
}

impl<C: FeedChannel> Sent<C> {
    fn empty() -> Self {
        Self {
            subscribed: BTreeMap::new(),
            last: None,
        }
    }

    /// `key` のデータイベントが届いた。新しいキーの subscribe ならそれで
    /// 通ったと分かる。置き換えは分からない: 古い購読のデータが同じキーで
    /// 流れ続けるからである。
    fn confirmed(&mut self, key: &<C as Codec>::Key) {
        if let Some((
            _,
            Written::Subscribe {
                channel,
                replaced: false,
            },
        )) = &self.last
            && channel.key() == *key
        {
            self.last = None;
        }
    }

    /// 読んだ拒否を、窓が開いている最後のコマンドに帰属させ、拒否された
    /// subscribe を取り消す。`desired` からは、そのキーがまだ同じチャネルを
    /// 望んでいるときだけ外す。別のオプションで望み直した subscribe はまだ
    /// 送っていない別のコマンドだからである。そのようなコマンドがなければ
    /// `None` を返し、何も変えない。読んだ時刻は見ない。窓を閉じるのは
    /// `step` の `RejectionWindowClosed` だけである。
    fn rejected(&mut self, desired: &mut BTreeMap<<C as Codec>::Key, C>) -> Option<C> {
        let (_, command) = self.last.take()?;
        match command {
            Written::Subscribe { channel, .. } => {
                let key = channel.key();
                self.subscribed.remove(&key);
                if desired.get(&key) == Some(&channel) {
                    desired.remove(&key);
                }
                Some(channel)
            }
            Written::Unsubscribe(channel) => Some(channel),
        }
    }
}

/// この接続に書いた 1 つのコマンド。
enum Written<C> {
    Subscribe {
        channel: C,
        /// この接続で同じキーの subscribe を置き換えた。
        replaced: bool,
    },
    Unsubscribe(C),
}

pub(crate) struct Connection {
    /// tokio-tungstenite の接続。フレーミング、ping への pong、サイズ上限は
    /// すべてこの中にある。書き込みは `poll_ready`、`start_send`、`poll_flush`
    /// で行う。`SinkExt::send` はキャンセル安全でなく、`select!` の中で
    /// 打ち切られると書きかけのフレームを失うからである。
    stream: WsStream,
    connected_at: Instant,
    last_frame_at: Instant,
    /// 最初に `poll_flush` が `Pending` を返した時刻。`ws_write` を超えたら切る。
    flush_since: Option<Instant>,
    /// サーバの close フレームを受けた。tungstenite が積んだ返答の close を
    /// flush してから、この理由で捨てる。
    closing: Option<DisconnectReason>,
}

/// `Connected` で 1 回のポーリングが観測したこと。
enum Step {
    Message(Message),
    Failed(TransportErrorKind),
    /// ペーサの時刻が来て、ソケットが書き込みを受け付けられる。
    WriteReady,
    /// ペーサの「次に書いてよい時刻」に達した。
    SlotReached,
    Idle,
    WriteTimedOut,
    ExtendDue,
    Extended(Instant, Result<()>),
    /// 拒否の窓の期限を過ぎ、ソケットに未読のフレームがない。
    RejectionWindowClosed,
}

impl Connection {
    /// 1 回のポーリング: まず flush (キャンセル前に積んだフレームと、
    /// tungstenite が読みながら積んだ pong を押し出す)、次に書きたければ
    /// 書き込み準備、最後に読む。
    ///
    /// 書き込みの期限は flush が `Pending` を返した直後に、ここで検査する。
    /// 期限の状態を知るのは flush だけなので、タイマーの腕で先に検査すると
    /// 完了済みの flush を期限切れと誤り、後に検査すると読める限りソケットが
    /// タイマーを飢えさせる。
    fn poll_socket(
        &mut self,
        cx: &mut Context<'_>,
        want_write: bool,
        write_bound: Duration,
        limits: &Limits,
    ) -> Poll<Step> {
        match self.stream.poll_flush_unpin(cx) {
            Poll::Ready(Ok(())) => self.flush_since = None,
            Poll::Ready(Err(e)) => return Poll::Ready(Step::Failed(ws::error_kind(&e, limits))),
            Poll::Pending => {
                let since = *self.flush_since.get_or_insert_with(Instant::now);
                if since + write_bound <= Instant::now() {
                    return Poll::Ready(Step::WriteTimedOut);
                }
            }
        }
        if want_write {
            match self.stream.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => return Poll::Ready(Step::WriteReady),
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Step::Failed(ws::error_kind(&e, limits)));
                }
                Poll::Pending => {}
            }
        }
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(message))) => {
                self.last_frame_at = Instant::now();
                Poll::Ready(Step::Message(message))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Step::Failed(ws::error_kind(&e, limits))),
            Poll::Ready(None) => Poll::Ready(Step::Failed(TransportErrorKind::ClosedEarly)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 望ましい集合と送った集合の最初の差: 足りない (または置き換えられた)
/// subscribe が先、それがなければ余った unsubscribe。
fn next_command<C: FeedChannel>(
    desired: &BTreeMap<<C as Codec>::Key, C>,
    sent: &BTreeMap<<C as Codec>::Key, C>,
) -> Option<(C, bool)> {
    desired
        .iter()
        .find(|(key, channel)| sent.get(key) != Some(channel))
        .map(|(_, channel)| (channel.clone(), true))
        .or_else(|| {
            sent.iter()
                .find(|(key, _)| !desired.contains_key(key))
                .map(|(_, channel)| (channel.clone(), false))
        })
}

fn protocol_error(api: ApiGroup, kind: ProtocolErrorKind) -> Error {
    Error::Protocol(ProtocolError {
        api,
        operation: OPERATION,
        kind,
    })
}

fn transport_error(api: ApiGroup, kind: TransportErrorKind, delivery: Delivery) -> TransportError {
    TransportError {
        api,
        operation: OPERATION,
        kind,
        delivery,
    }
}

fn live_failure(api: ApiGroup, kind: TransportErrorKind) -> DisconnectReason {
    DisconnectReason::Transport(transport_error(api, kind, Delivery::MaybeSent))
}

/// ハンドシェイクの失敗を再試行の理由に写す。トークンの拒否 (Private の
/// 401/403/404) は出所で意味が変わるので、接続試行が先に扱う。
fn dial_failure(api: ApiGroup, e: WsConnectError) -> DialFailure {
    DialFailure::Retry(match e {
        WsConnectError::Transport(kind) => {
            DisconnectReason::Transport(transport_error(api, kind, Delivery::NotSent))
        }
        WsConnectError::Rejected { http_status: 503 } => DisconnectReason::Maintenance,
        WsConnectError::Rejected { http_status } => {
            DisconnectReason::HandshakeRejected { http_status }
        }
    })
}

/// 取引所がトークンを拒んだとみなす HTTP 状態。
fn rejects_token(http_status: u16) -> bool {
    matches!(http_status, 401 | 403 | 404)
}

/// `POST ws-auth` の失敗。再試行するのは、メンテナンス (ERR-5201/5202/5203、
/// またはエンベロープのない 503)、一時的なコード (ERR-554、626、5003、5008、
/// 5009)、エンベロープのない 5xx、ネットワーク。それ以外 (キーの拒否
/// ERR-5011/5012、形の違反、契約違反) は終端。
fn acquire_failure(e: Error) -> DialFailure {
    const MAINTENANCE: [ErrorCode; 3] = [
        ErrorCode::SCHEDULED_MAINTENANCE,
        ErrorCode::EMERGENCY_MAINTENANCE,
        ErrorCode::PRE_OPEN,
    ];
    const TRANSIENT: [ErrorCode; 5] = [
        ErrorCode::SERVER_UNAVAILABLE,
        ErrorCode::SERVER_BUSY,
        ErrorCode::RATE_LIMIT,
        ErrorCode::TIMESTAMP_TOO_LATE,
        ErrorCode::TIMESTAMP_TOO_EARLY,
    ];
    let has_any =
        |api: &crate::error::ApiError, codes: &[ErrorCode]| codes.iter().any(|code| api.has(*code));
    match e {
        Error::Api(api) if has_any(&api, &MAINTENANCE) => {
            DialFailure::Retry(DisconnectReason::Maintenance)
        }
        Error::Api(api) if has_any(&api, &TRANSIENT) => {
            DialFailure::Retry(DisconnectReason::Api(api))
        }
        Error::Protocol(p) if *p.kind() == ProtocolErrorKind::HttpStatus(503) => {
            DialFailure::Retry(DisconnectReason::Maintenance)
        }
        Error::Protocol(p) if matches!(p.kind(), ProtocolErrorKind::HttpStatus(500..=599)) => {
            DialFailure::Retry(DisconnectReason::Protocol(p))
        }
        Error::Transport(t) => DialFailure::Retry(DisconnectReason::Transport(t)),
        other => DialFailure::Terminal(other),
    }
}

/// 理由ごとの、バックオフの梯子を先に進める段数。呼び出し上限 (ERR-5003)
/// は 2 段 (4 倍): 1 秒の窓を確実に越え、上限を叩き続けない。
fn extra_backoff_steps(reason: &DisconnectReason) -> u32 {
    match reason {
        DisconnectReason::Api(api) if api.has(ErrorCode::RATE_LIMIT) => 2,
        _ => 0,
    }
}

fn sleep_until(at: Instant) -> tokio::time::Sleep {
    tokio::time::sleep_until(tokio::time::Instant::from_std(at))
}

impl<C: FeedChannel> Session<C> {
    pub(crate) fn desire(&mut self, channel: C) -> core::result::Result<(), ContractViolation> {
        let max = self.inner.config.limits.max_subscriptions;
        let key = channel.key();
        if !self.desired.contains_key(&key) && self.desired.len() >= usize::from(max) {
            return Err(ContractViolation::TooManySubscriptions { max });
        }
        self.desired.insert(key, channel);
        Ok(())
    }

    pub(crate) fn undesire(&mut self, channel: &C) -> bool {
        self.desired.remove(&channel.key()).is_some()
    }

    /// 契約は `Feed::next` を参照。
    pub(crate) async fn next(&mut self) -> Result<<C as Codec>::Event> {
        loop {
            match &mut self.state {
                State::Closed => return Err(Error::FeedClosed),
                State::Idle => self.state = State::Dialing(self.dial()),
                State::Waiting { until } => {
                    sleep_until(*until).await;
                    self.state = State::Idle;
                }
                State::Dialing(fut) => {
                    let dialed = fut.as_mut().await;
                    if let Some(event) = self.dialed(dialed) {
                        return event;
                    }
                }
                State::Connected(..) => {
                    if let Some(event) = self.step().await {
                        return event;
                    }
                }
            }
        }
    }

    /// 接続試行の結果を状態に写す。`None` なら呼び出し側はループを続ける。
    fn dialed(&mut self, dialed: Dialed) -> Option<Result<<C as Codec>::Event>> {
        self.auth.adopt(dialed.token);
        match dialed.result {
            Ok(stream) => {
                let now = Instant::now();
                let connection = Connection {
                    stream,
                    connected_at: now,
                    last_frame_at: now,
                    flush_since: None,
                    closing: None,
                };
                self.state = State::Connected(Box::new(connection), Sent::empty());
                if self.attempt > 0 {
                    Some(Ok(C::reconnected()))
                } else {
                    None
                }
            }
            Err(DialFailure::Terminal(e)) => {
                self.state = State::Closed;
                Some(Err(e))
            }
            Err(DialFailure::ReusedTokenRejected { http_status }) => {
                if let Auth::Private { lease, .. } = &mut self.auth {
                    *lease = None;
                }
                Some(Ok(self.disconnected(DisconnectReason::HandshakeRejected {
                    http_status,
                })))
            }
            Err(DialFailure::Retry(reason)) => Some(Ok(self.disconnected(reason))),
        }
    }

    /// `Waiting` に移り、`Disconnected` を作る。進行中の延長は捨てる。
    fn disconnected(&mut self, reason: DisconnectReason) -> <C as Codec>::Event {
        let step = self.attempt.saturating_add(extra_backoff_steps(&reason));
        let delay = self.inner.config.reconnect.delay(step);
        self.attempt = self.attempt.saturating_add(1);
        self.state = State::Waiting {
            until: Instant::now() + delay,
        };
        self.auth.abandon_extend();
        C::disconnected(Disconnect {
            reason,
            retry_in: delay,
        })
    }

    /// 生きていた接続を捨てる。バックオフの最上段 (`Backoff` の `max`。
    /// `delay` は 16 段で飽和するので、`initial` が `max / 65536` より
    /// 小さい設定でだけ `max` を下回る) の間維持されていたら試行回数を戻す。
    fn drop_connection(&mut self, reason: DisconnectReason) -> <C as Codec>::Event {
        if let State::Connected(conn, _) = &self.state
            && conn.connected_at.elapsed() >= self.inner.config.reconnect.delay(u32::MAX)
        {
            self.attempt = 0;
        }
        self.disconnected(reason)
    }

    /// `Connected` での 1 歩。`None` なら何も返すものがなく、ループを続ける。
    async fn step(&mut self) -> Option<Result<<C as Codec>::Event>> {
        let Self {
            inner,
            auth,
            desired,
            state,
            ..
        } = self;
        let State::Connected(conn, sent) = state else {
            unreachable!("step runs only in Connected");
        };
        let pacer = match C::API {
            ApiGroup::PrivateWs => &inner.private_ws_pacer,
            _ => &inner.public_ws_pacer,
        };
        let timeouts = inner.config.timeouts;
        if conn.closing.is_some() {
            let _ = tokio::time::timeout(timeouts.ws_write.get(), conn.stream.flush()).await;
            let reason = conn.closing.take().expect("checked just above");
            return Some(Ok(self.drop_connection(reason)));
        }
        let now = Instant::now();

        let command = next_command::<C>(desired, &sent.subscribed);
        let slot = command.as_ref().map(|_| pacer.next_free());
        let want_write = slot.is_some_and(|at| at <= now);
        let idle_at = conn.last_frame_at + timeouts.ws_idle.get();
        let write_deadline = conn
            .flush_since
            .map(|since| since + timeouts.ws_write.get());
        let extend_at = auth.extend_due_at();
        let mut extend = auth.extend_in_flight();
        let window_closes_at = sent
            .last
            .as_ref()
            .map(|(written_at, _)| *written_at + REJECTION_WINDOW);

        let step = tokio::select! {
            biased;
            (sent_at, result) = poll_fn(|cx| match extend.as_mut() {
                Some(fut) => fut.as_mut().poll(cx),
                None => Poll::Pending,
            }) => Step::Extended(sent_at, result),
            _ = sleep_until(extend_at.unwrap_or(now)), if extend_at.is_some() => Step::ExtendDue,
            _ = sleep_until(slot.unwrap_or(now)), if slot.is_some() && !want_write => Step::SlotReached,
            step = poll_fn(|cx| conn.poll_socket(cx, want_write, timeouts.ws_write.get(), &inner.config.limits)) => step,
            _ = sleep_until(window_closes_at.unwrap_or(now)), if window_closes_at.is_some() => Step::RejectionWindowClosed,
            _ = sleep_until(write_deadline.unwrap_or(now)), if write_deadline.is_some() => Step::WriteTimedOut,
            _ = sleep_until(idle_at) => Step::Idle,
        };

        match step {
            Step::Message(Message::Text(text)) => match C::decode(&text) {
                Ok(Decoded::Event(event)) => match C::event_key(&event) {
                    Some(key) if !desired.contains_key(&key) => None,
                    Some(key) => {
                        sent.confirmed(&key);
                        Some(Ok(event))
                    }
                    None => Some(Ok(event)),
                },
                Ok(Decoded::Rejected(text)) => {
                    let channel = sent.rejected(desired);
                    Some(Ok(C::rejected(channel, text)))
                }
                Err(kind) => {
                    self.state = State::Closed;
                    Some(Err(protocol_error(C::API, kind)))
                }
            },
            Step::Message(Message::Binary(_)) => Some(Ok(self.drop_connection(live_failure(
                C::API,
                TransportErrorKind::Framing("websocket binary frame"),
            )))),
            Step::Message(Message::Close(frame)) => {
                let code = frame.map(|f| u16::from(f.code));
                conn.closing = Some(DisconnectReason::ServerClosed { code });
                None
            }
            Step::Message(_) => None,
            Step::Failed(kind) => Some(Ok(self.drop_connection(live_failure(C::API, kind)))),
            Step::WriteReady => {
                let (channel, subscribe) = command.expect("want_write implies a command");
                if pacer.take().is_err() {
                    return None;
                }
                let frame = Message::text(channel.command(subscribe));
                if let Err(e) = conn.stream.start_send_unpin(frame) {
                    let kind = ws::error_kind(&e, &inner.config.limits);
                    return Some(Ok(self.drop_connection(live_failure(C::API, kind))));
                }
                let key = channel.key();
                let written = if subscribe {
                    let replaced = sent.subscribed.insert(key, channel.clone()).is_some();
                    Written::Subscribe { channel, replaced }
                } else {
                    sent.subscribed.remove(&key);
                    Written::Unsubscribe(channel)
                };
                sent.last = Some((Instant::now(), written));
                conn.flush_since.get_or_insert_with(Instant::now);
                None
            }
            Step::SlotReached => None,
            Step::RejectionWindowClosed => {
                sent.last = None;
                None
            }
            Step::Idle => Some(Ok(self.drop_connection(live_failure(
                C::API,
                TransportErrorKind::Timeout(Phase::WsIdle),
            )))),
            Step::WriteTimedOut => match conn.flush_since {
                Some(since) if since + timeouts.ws_write.get() <= Instant::now() => Some(Ok(self
                    .drop_connection(live_failure(
                        C::API,
                        TransportErrorKind::Timeout(Phase::WsWrite),
                    )))),
                _ => None,
            },
            Step::ExtendDue => {
                self.auth.start_extend();
                None
            }
            Step::Extended(sent_at, result) => {
                self.auth.extended(sent_at, result);
                None
            }
        }
    }

    /// 1 回の接続試行を自己完結した future として始める。`State::Dialing` に
    /// 保存でき、キャンセルの後に再開できる。
    ///
    /// Private: 生きているリースがあればそのトークンで接続する。なければ
    /// `POST ws-auth` で取得し、新しいリースを結果に載せる。ハンドシェイクが
    /// トークンを拒んだら (401/403/404):
    /// - この試行で発行したトークンなら、取引所が発行したばかりのものを
    ///   拒んでいる。設定の問題なので `DELETE` してから `Terminal`
    ///   (`Protocol(WsHandshake)`) で終える。`DELETE` の失敗は報告する
    ///   エラーを変えない。トークンは 60 分以内に失効する。
    /// - 再利用したトークンなら `ReusedTokenRejected`。セッションがリースを
    ///   捨て、次の試行が取り直す。
    fn dial(&mut self) -> DialFuture {
        let inner = self.inner.clone();
        match &self.auth {
            Auth::Public => Box::pin(async move {
                let result = ws::connect(
                    &inner.connector,
                    inner.endpoints.public_ws.base(),
                    OPERATION,
                    &inner.config.timeouts,
                    &inner.config.limits,
                )
                .await
                .map_err(|e| dial_failure(ApiGroup::PublicWs, e));
                Dialed {
                    result,
                    token: TokenSource::None,
                }
            }),
            Auth::Private {
                client,
                lease,
                schedule,
                ..
            } => {
                let client = client.clone();
                let schedule = *schedule;
                let reused = lease
                    .as_ref()
                    .filter(|l| l.is_live(Instant::now()))
                    .map(TokenLease::path);
                Box::pin(async move {
                    let (path, source) = match reused {
                        Some(path) => (path, TokenSource::Reused),
                        None => {
                            let sent_at = Instant::now();
                            match client.acquire_ws_token().await {
                                Ok(token) => {
                                    let lease = TokenLease::new(token, sent_at, schedule);
                                    (lease.path(), TokenSource::Fresh(lease))
                                }
                                Err(e) => {
                                    return Dialed {
                                        result: Err(acquire_failure(e)),
                                        token: TokenSource::None,
                                    };
                                }
                            }
                        }
                    };
                    let result = ws::connect(
                        &inner.connector,
                        inner.endpoints.private_ws.base(),
                        &path,
                        &inner.config.timeouts,
                        &inner.config.limits,
                    )
                    .await;
                    match (result, source) {
                        (Ok(stream), token) => Dialed {
                            result: Ok(stream),
                            token,
                        },
                        (
                            Err(WsConnectError::Rejected { http_status }),
                            TokenSource::Fresh(lease),
                        ) if rejects_token(http_status) => {
                            let _ = client.delete_ws_token(lease.token).await;
                            Dialed {
                                result: Err(DialFailure::Terminal(protocol_error(
                                    ApiGroup::PrivateWs,
                                    ProtocolErrorKind::WsHandshake { http_status },
                                ))),
                                token: TokenSource::None,
                            }
                        }
                        (Err(WsConnectError::Rejected { http_status }), token)
                            if rejects_token(http_status) =>
                        {
                            Dialed {
                                result: Err(DialFailure::ReusedTokenRejected { http_status }),
                                token,
                            }
                        }
                        (Err(e), token) => Dialed {
                            result: Err(dial_failure(ApiGroup::PrivateWs, e)),
                            token,
                        },
                    }
                })
            }
        }
    }

    /// close フレーム、エコーの有界な待機、Private フィードでは `DELETE ws-auth`。
    /// 進行中の接続試行は待ってから閉じる。試行の中で発行されたトークンを
    /// 取り戻して削除するためである。
    pub(crate) async fn close(mut self) -> Result<()> {
        let timeouts = self.inner.config.timeouts;
        let stream = match std::mem::replace(&mut self.state, State::Closed) {
            State::Connected(conn, _) => Some(conn.stream),
            State::Dialing(fut) => {
                let dialed = fut.await;
                self.auth.adopt(dialed.token);
                dialed.result.ok()
            }
            State::Idle | State::Waiting { .. } | State::Closed => None,
        };
        let mut outcome = Ok(());
        if let Some(mut stream) = stream {
            outcome = close_handshake(
                &mut stream,
                timeouts.ws_write.get(),
                &self.inner.config.limits,
            )
            .await
            .map_err(|kind| Error::Transport(transport_error(C::API, kind, Delivery::MaybeSent)));
        }
        if let Auth::Private {
            client,
            lease: Some(lease),
            ..
        } = self.auth
            && lease.is_live(Instant::now())
        {
            let deleted = client.delete_ws_token(lease.token).await;
            outcome = outcome.and(deleted);
        }
        outcome
    }
}

/// close フレームを送り、サーバのエコーまで (または `bound` まで) 読む。
async fn close_handshake(
    stream: &mut WsStream,
    bound: Duration,
    limits: &Limits,
) -> core::result::Result<(), TransportErrorKind> {
    let handshake = async {
        stream.close(None).await?;
        while let Some(item) = stream.next().await {
            item?;
        }
        Ok(())
    };
    match tokio::time::timeout(bound, handshake).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(ws::error_kind(&e, limits)),
        Err(_elapsed) => Err(TransportErrorKind::Timeout(Phase::WsWrite)),
    }
}

impl Session<PublicChannel> {
    pub(crate) fn public(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            auth: Auth::Public,
            desired: BTreeMap::new(),
            state: State::Idle,
            attempt: 0,
        }
    }
}

impl Session<PrivateChannel> {
    pub(crate) fn private(client: Client<Authenticated>) -> Self {
        Self::private_with(client, LeaseSchedule::default())
    }

    pub(crate) fn private_with(client: Client<Authenticated>, schedule: LeaseSchedule) -> Self {
        let inner = client.inner.clone();
        Self {
            inner,
            auth: Auth::Private {
                client,
                lease: None,
                schedule,
                extend: None,
            },
            desired: BTreeMap::new(),
            state: State::Idle,
            attempt: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Symbol;

    #[test]
    fn the_first_difference_is_a_missing_subscribe_then_a_stale_unsubscribe() {
        let mut desired = BTreeMap::new();
        let mut sent = BTreeMap::new();
        assert!(next_command::<PublicChannel>(&desired, &sent).is_none());

        let ticker = PublicChannel::Ticker(Symbol::BTC);
        desired.insert(ticker.key(), ticker.clone());
        assert_eq!(
            next_command::<PublicChannel>(&desired, &sent),
            Some((ticker.clone(), true))
        );
        sent.insert(ticker.key(), ticker.clone());
        assert!(next_command::<PublicChannel>(&desired, &sent).is_none());

        let replaced = PublicChannel::Trades {
            symbol: Symbol::BTC,
            taker_only: true,
        };
        sent.insert(
            replaced.key(),
            PublicChannel::Trades {
                symbol: Symbol::BTC,
                taker_only: false,
            },
        );
        desired.insert(replaced.key(), replaced.clone());
        assert_eq!(
            next_command::<PublicChannel>(&desired, &sent),
            Some((replaced.clone(), true)),
            "a changed option is re-sent as a subscribe"
        );
        sent.insert(replaced.key(), replaced);

        desired.remove(&ticker.key());
        assert_eq!(
            next_command::<PublicChannel>(&desired, &sent),
            Some((ticker, false))
        );
    }

    #[test]
    fn a_token_is_rejected_only_by_the_documented_statuses() {
        assert!([401, 403, 404].into_iter().all(rejects_token));
        assert!(![400, 429, 500, 503].into_iter().any(rejects_token));
        assert!(matches!(
            dial_failure(
                ApiGroup::PrivateWs,
                WsConnectError::Rejected { http_status: 503 }
            ),
            DialFailure::Retry(DisconnectReason::Maintenance)
        ));
        assert!(matches!(
            dial_failure(
                ApiGroup::PublicWs,
                WsConnectError::Rejected { http_status: 404 }
            ),
            DialFailure::Retry(DisconnectReason::HandshakeRejected { http_status: 404 })
        ));
    }

    /// 本番の Public WebSocket に不正な subscribe を 1 つ送り、生の返答を
    /// `tests/fixtures/live/ws_public_error.json` に保存する。読み取り専用。
    /// 直前の 1 秒間に他のコマンドを送っていないこと (別の呼び出しで走らせる)。
    #[tokio::test]
    #[ignore = "live"]
    async fn probe_the_public_error_shape() {
        use crate::client::Config;
        use crate::endpoint::Endpoints;
        use crate::transport::Connector;

        let endpoints = Endpoints::production();
        let config = Config::default();
        let mut stream = ws::connect(
            &Connector::new(),
            endpoints.public_ws.base(),
            OPERATION,
            &config.timeouts,
            &config.limits,
        )
        .await
        .expect("handshake");
        stream
            .send(Message::text(
                r#"{"command":"subscribe","channel":"nonexistent","symbol":"BTC"}"#,
            ))
            .await
            .expect("send");
        let reply = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(item) = stream.next().await {
                if let Message::Text(text) = item.expect("read") {
                    return Some(text.to_string());
                }
            }
            None
        })
        .await;
        let _ = stream.close(None).await;
        match reply {
            Ok(Some(text)) => {
                let path = format!(
                    "{}/tests/fixtures/live/ws_public_error.json",
                    env!("CARGO_MANIFEST_DIR")
                );
                std::fs::write(&path, format!("{text}\n")).expect("write fixture");
                println!("reply: {text}");
            }
            Ok(None) => println!("the server closed without a text reply"),
            Err(_) => println!("no reply within 5 seconds"),
        }
    }
}
