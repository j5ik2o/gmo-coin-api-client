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
//! - `sent.last` がある間は、次のコマンドを書かない。先にソケットを読む。
//!   未読の拒否を読む前に次を書くと、その拒否が後のコマンドに付いてしまう
//!   (issue #14)。窓が閉じるまで (期限を過ぎた後は `REJECTION_DRAIN_LIMIT`
//!   フレームまで) 読み、それでもソケットが空にならない (受信が途切れない)
//!   ときは、帰属をあきらめて書く。書き込みが永久に止まらないようにするため
//!   である。あきらめている間は書き込みを止めず、拒否はどのコマンドにも
//!   結び付けない (`Rejected { channel: None }`)。あきらめている間に書いた
//!   コマンドも `last` に残す。
//! - `sent.last` が残るのは、その書き込みから `REJECTION_WINDOW` を過ぎた後に、
//!   ソケットに未読のフレームがないことを `next` がまだ観測していない間だけ
//!   である。あきらめている場合は、空を観測しただけでは消さない。空の観測と、
//!   `last` (あきらめている間に書いた最後のコマンド) の窓の期限が揃って初めて
//!   帰属を再開する。揃うまでに届いた拒否は `None` のままにする。

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
use crate::client::{Authenticated, Backoff, Client, Inner, Limits};
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
/// ただし、呼び出し側がランタイムのスレッドを塞いでいた場合は、tokio が最初の
/// ポーリングで返すソケットの準備状況が古く、読み込みが `Pending` を返して窓が
/// 閉じることがある。`next` の外で別のものを `await` していた場合だけが、この
/// 約束の対象である。
/// 窓を閉じるのは、期限を過ぎてから始めたポーリングで読み込みが `Pending`
/// を返し、しかもその読み込みが協調スケジューリングの予算切れで止められたの
/// ではないと分かったとき (`Connection::poll_socket`) だけである。予算切れの
/// `Pending` は、読めるデータが残っていても返るので、空のソケットの証拠に
/// ならない。期限のタイマーはタスクを起こすだけで、窓を閉じない。
pub(crate) const REJECTION_WINDOW: Duration = Pacer::INTERVAL;

/// 拒否の窓の期限を過ぎてから、次のコマンドを待たせたまま読むフレーム数の
/// 上限。受信が途切れないソケットでは空を観測できないので、この数に達したら
/// 前のコマンドへの帰属をあきらめて書く。ここで待ち続けると、受信が途切れない
/// 間、書き込みが永久に止まる。取引所は不正なコマンドにすぐ答えるので、
/// 拒否は普通この数より手前に並ぶ。
///
/// あきらめた帰属は、空のソケットを観測しただけでは戻さない。あきらめている
/// 間に書いた最後のコマンドの窓の期限も過ぎている必要がある。そのコマンドへの
/// 返答はまだ届いていないかもしれず、戻した直後に読む拒否を後のコマンドに
/// 付けてしまうからである。
pub(crate) const REJECTION_DRAIN_LIMIT: u32 = 64;

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
    /// しか帰属しない。これがある間は次のコマンドを書かず、先にソケットを
    /// 読む。
    last: Option<(Instant, Written<C>)>,
    /// `last` の窓の期限を過ぎてから、次に書くコマンドを待たせたまま読んだ
    /// フレームの数。`REJECTION_DRAIN_LIMIT` に達したら `last` の帰属を
    /// あきらめる。
    read_after_deadline: u32,
    /// 期限までにソケットを空にできず、帰属をあきらめた。未読の拒否が残って
    /// いるかもしれないので、どの拒否も新しいコマンドに結び付けず、`None` で
    /// 知らせる。書き込みは止めない。あきらめている間に書いたコマンドは `last`
    /// に残す。帰属を再開するのは、ソケットが空だと観測し、かつ `last` の窓の
    /// 期限も過ぎたときである。それまでは、遅れて届いた拒否がどのコマンドの
    /// ものか決められない。
    attribution_abandoned: bool,
}

impl<C: FeedChannel> Sent<C> {
    fn empty() -> Self {
        Self {
            subscribed: BTreeMap::new(),
            last: None,
            read_after_deadline: 0,
            attribution_abandoned: false,
        }
    }

    /// `key` のデータイベントが届いた。新しいキーの subscribe ならそれで
    /// 通ったと分かる。置き換えは分からない: 古い購読のデータが同じキーで
    /// 流れ続けるからである。
    ///
    /// 帰属をあきらめている間は `last` を残す。`last` の書き込み時刻が、帰属を
    /// 再開してよいかを決める窓の起点になる。データで裏付けられたコマンドへの
    /// 拒否はもう来ないが、それより前に送ったコマンドへの未読の拒否は残り得る。
    fn confirmed(&mut self, key: &<C as Codec>::Key) {
        if self.attribution_abandoned {
            return;
        }
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
            self.read_after_deadline = 0;
        }
    }

    /// 読んだ拒否を、窓が開いている最後のコマンドに帰属させ、拒否された
    /// subscribe を取り消す。`desired` からは、そのキーがまだ同じチャネルを
    /// 望んでいるときだけ外す。別のオプションで望み直した subscribe はまだ
    /// 送っていない別のコマンドだからである。そのようなコマンドがなければ
    /// `None` を返し、何も変えない。読んだ時刻は見ない。窓を閉じるのは
    /// ソケットが空だと観測したときの `Step::RejectionWindowClosed` だけである。
    ///
    /// 帰属をあきらめている間 (`attribution_abandoned`) は、未読の拒否がどの
    /// コマンドのものか決められないので、何も変えずに `None` を返す。`last`
    /// は帰属を再開してよいかの窓の起点として残す。
    fn rejected(&mut self, desired: &mut BTreeMap<<C as Codec>::Key, C>) -> Option<C> {
        if self.attribution_abandoned {
            return None;
        }
        let (_, command) = self.last.take()?;
        self.read_after_deadline = 0;
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
    /// 拒否の窓の期限を過ぎてから、予算の残った読み込みが `Pending` を
    /// 返した。ソケットに未読のフレームがない。拒否の窓を閉じ、あきらめて
    /// いた帰属も再開する (空の観測と `last` の期限が揃ったときだけである)。
    /// `poll_socket` だけが返す。
    RejectionWindowClosed,
    /// 拒否の窓の期限に達した。タスクを起こすだけで状態は変えない。窓を
    /// 閉じるかどうかは、次のポーリングの `poll_socket` が決める。
    RejectionWindowDue,
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
    ///
    /// 拒否の窓も同じように、読み込みが `Pending` を返した直後にここで閉じる。
    /// ソケットが空だと知るのは読み込みだけだからである。`window_elapsed` は、
    /// このポーリングを始めた時点で窓の期限を過ぎていたことを表す。帰属を
    /// あきらめている間も、窓の起点は `last` (あきらめている間に書いた最後の
    /// コマンド) なので、期限の後に空を観測したときだけ帰属を再開する。
    /// ただし、予算切れの `Pending` は空のソケットの証拠にならないので、
    /// 読み込みの前後で予算が残っていたかを確かめる。
    fn poll_socket(
        &mut self,
        cx: &mut Context<'_>,
        want_write: bool,
        window_elapsed: bool,
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
        // tokio は 1 回のタスクのポーリングに使える作業量 (協調スケジューリングの
        // 予算) を決めており、使い切ったタスクの読み込みには、読めるデータが
        // 残っていても `Pending` を返す。予算切れの `Pending` を空のソケットと
        // 取り違えないよう、読み込みの前後で予算が残っていたかを見る。両方で
        // 残っていれば、`Pending` はソケットが空だという意味になる。
        let budget_before = tokio::task::coop::has_budget_remaining();
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(message))) => {
                self.last_frame_at = Instant::now();
                Poll::Ready(Step::Message(message))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Step::Failed(ws::error_kind(&e, limits))),
            Poll::Ready(None) => Poll::Ready(Step::Failed(TransportErrorKind::ClosedEarly)),
            Poll::Pending
                if window_elapsed && budget_before && tokio::task::coop::has_budget_remaining() =>
            {
                Poll::Ready(Step::RejectionWindowClosed)
            }
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

/// セッションがこの失敗のあとに使う、バックオフの梯子の段数。`Backoff::cap`
/// と `Backoff::delay` にはこの段数を渡す。
fn backoff_step(attempt: u32, reason: &DisconnectReason) -> u32 {
    attempt.saturating_add(extra_backoff_steps(reason))
}

fn sleep_until(at: Instant) -> tokio::time::Sleep {
    tokio::time::sleep_until(tokio::time::Instant::from_std(at))
}

/// 生きていた接続がバックオフの梯子を最初に戻すか。閾値は、乱数で引く
/// `delay` ではなく、上限を決める [`Backoff::cap`] そのものである。乱数の
/// 値が変わっても判定は変わらない。
fn resets_ladder(backoff: &Backoff, connected_for: Duration) -> bool {
    connected_for >= backoff.cap(u32::MAX)
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
        let step = backoff_step(self.attempt, &reason);
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
    /// `cap` は 16 段で飽和するので、`initial` が `max / 65536` より
    /// 小さい設定でだけ `max` を下回る) の間維持されていたら試行回数を戻す。
    fn drop_connection(&mut self, reason: DisconnectReason) -> <C as Codec>::Event {
        if let State::Connected(conn, _) = &self.state
            && resets_ladder(&self.inner.config.reconnect, conn.connected_at.elapsed())
        {
            self.attempt = 0;
        }
        self.disconnected(reason)
    }

    /// `Connected` での 1 歩。`None` なら何も返すものがなく、ループを続ける。
    #[expect(
        clippy::too_many_lines,
        reason = "1 つの select! の待ち受けと、その結果ごとの処理を 1 か所で読めるようにしている。分けると状態の受け渡しが増える"
    )]
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
            if let Some(reason) = conn.closing.take() {
                return Some(Ok(self.drop_connection(reason)));
            }
        }
        let now = Instant::now();

        let command = next_command::<C>(desired, &sent.subscribed);
        let slot = command.as_ref().map(|_| pacer.next_free());
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
        let window_elapsed = window_closes_at.is_some_and(|at| at <= now);
        // 期限を過ぎてもソケットが空にならない (受信が途切れない) ときは、
        // 前のコマンドへの帰属をあきらめて書き込みに進む。ここで待ち続けると、
        // 受信が途切れない間、書き込みが永久に止まる。`last` は残す: 帰属を
        // 再開してよいかを決める窓の起点であり、あきらめている間に書く次の
        // コマンドの書き込み時刻で置き換わる。
        if window_elapsed && command.is_some() && sent.read_after_deadline >= REJECTION_DRAIN_LIMIT
        {
            sent.attribution_abandoned = true;
            sent.read_after_deadline = 0;
        }
        // 前のコマンドへの拒否を読む前に次を書くと、その拒否が後のコマンドに
        // 付いてしまう (issue #14)。`last` がある間は書かず、まず読む。
        // あきらめている間だけは、書き込みを止めないためにそのまま書く。
        let gated = sent.last.is_some() && !sent.attribution_abandoned;
        let want_write = !gated && slot.is_some_and(|at| at <= now);
        let draining = command.is_some() && gated;

        let step = tokio::select! {
            biased;
            (sent_at, result) = poll_fn(|cx| match extend.as_mut() {
                Some(fut) => fut.as_mut().poll(cx),
                None => Poll::Pending,
            }) => Step::Extended(sent_at, result),
            () = sleep_until(extend_at.unwrap_or(now)), if extend_at.is_some() => Step::ExtendDue,
            () = sleep_until(slot.unwrap_or(now)), if slot.is_some_and(|at| at > now) => Step::SlotReached,
            step = poll_fn(|cx| conn.poll_socket(cx, want_write, window_elapsed, timeouts.ws_write.get(), &inner.config.limits)) => step,
            () = sleep_until(window_closes_at.unwrap_or(now)), if window_closes_at.is_some() && !window_elapsed => Step::RejectionWindowDue,
            () = sleep_until(write_deadline.unwrap_or(now)), if write_deadline.is_some() => Step::WriteTimedOut,
            () = sleep_until(idle_at) => Step::Idle,
        };

        // 次のコマンドを待たせている間は、読んだフレームを数える。期限の後に
        // 上限まで読んでソケットが空にならなければ、帰属をあきらめる。
        if matches!(step, Step::Message(_)) && draining && window_elapsed {
            sent.read_after_deadline = sent.read_after_deadline.saturating_add(1);
        }

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
            Step::Failed(kind) => Some(Ok(self.drop_connection(live_failure(C::API, kind)))),
            Step::WriteReady => {
                #[expect(
                    clippy::expect_used,
                    reason = "want_write は command が Some のときだけ真になり、poll_socket は want_write が真のときだけ WriteReady を返す"
                )]
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
                // あきらめている間も、このコマンドを `last` に残す。`last` の
                // 書き込み時刻が、帰属を再開してよいかを決める窓の起点になる。
                // 拒否は再開までどのコマンドにも結び付かない。
                sent.last = Some((Instant::now(), written));
                sent.read_after_deadline = 0;
                conn.flush_since.get_or_insert_with(Instant::now);
                None
            }
            Step::Message(_) | Step::SlotReached | Step::RejectionWindowDue => None,
            Step::RejectionWindowClosed => {
                // 空のソケットと `last` の窓の期限が揃った。あきらめていた
                // 帰属もここで再開する。それまでに読んだ拒否は、どのコマンドの
                // ものか決められないので `None` のままにしておく。
                sent.last = None;
                sent.attribution_abandoned = false;
                sent.read_after_deadline = 0;
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
                self.auth.extended(sent_at, &result);
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
        let inner = Arc::clone(&self.inner);
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
                    let (path, source) = if let Some(path) = reused {
                        (path, TokenSource::Reused)
                    } else {
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
        let inner = Arc::clone(&client.inner);
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

    /// 帰属をあきらめている間は、未読の拒否が前のコマンドのものかもしれない。
    /// 拒否はどのコマンドにも結び付かず、望ましい集合も変わらない。
    #[test]
    fn an_abandoned_attribution_names_no_channel_and_drops_nothing() {
        let ticker = PublicChannel::Ticker(Symbol::ETH);
        let mut desired = BTreeMap::new();
        desired.insert(ticker.key(), ticker.clone());
        let mut sent = Sent::<PublicChannel>::empty();
        sent.subscribed.insert(ticker.key(), ticker.clone());
        sent.last = Some((
            Instant::now(),
            Written::Subscribe {
                channel: ticker.clone(),
                replaced: false,
            },
        ));
        sent.attribution_abandoned = true;

        assert_eq!(sent.rejected(&mut desired), None);
        assert!(
            desired.contains_key(&ticker.key()),
            "the rejected subscribe stayed in the desired set"
        );
        assert!(sent.subscribed.contains_key(&ticker.key()));
        assert!(
            sent.last.is_some(),
            "the write time stays as the anchor for resuming attribution"
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

    /// 梯子を戻す判定は、乱数で引いた待ち時間ではなく `cap` を閾値にする。
    /// 乱数がいちばん小さい値 (`cap/2`) を返しても、失敗して `cap` を返しても、
    /// 境界は同じである。待ち時間そのものを閾値にしていたら、引いた値によって
    /// 境界が動き、このテストは落ちる。
    #[test]
    fn the_ladder_reset_threshold_does_not_depend_on_the_random_draw() {
        use crate::client::Timeout;

        let secs = |n| Timeout::new(Duration::from_secs(n)).unwrap();
        let backoff = Backoff::new(secs(1), secs(60)).unwrap();
        let threshold = backoff.cap(u32::MAX);
        assert_eq!(threshold, Duration::from_secs(60));
        let just_below = threshold.checked_sub(Duration::from_millis(1)).unwrap();

        for (label, drawn) in [
            ("smallest", backoff.delay_with(u32::MAX, |_| Some(0))),
            ("failed", backoff.delay_with(u32::MAX, |_| None)),
        ] {
            assert!(
                !resets_ladder(&backoff, just_below),
                "{label}: the reset threshold moved below cap (drawn {drawn:?})"
            );
            assert!(
                resets_ladder(&backoff, threshold),
                "{label}: a connection kept for cap must reset the ladder"
            );
        }
    }

    /// ERR-5003 (`POST ws-auth` の呼び出し上限) の失敗。
    fn rate_limited_reason() -> DisconnectReason {
        use crate::error::{ApiError, ApiMessage};

        DisconnectReason::Api(ApiError {
            operation: "POST /v1/ws-auth",
            status: 1,
            messages: vec![ApiMessage {
                code: ErrorCode::RATE_LIMIT,
                text: "Requests are too many.".into(),
            }],
            response_time: None,
        })
    }

    /// 既定の設定では、ERR-5003 のあとに引く待ち時間は、乱数がいちばん小さい
    /// 値を返しても 1 秒の購読コマンドの枠より長い。full jitter (下限 0) に
    /// 変えると、ここが 0 秒近くまで縮んで落ちる。
    #[test]
    fn a_rate_limit_waits_past_the_one_second_window_even_with_the_smallest_draw() {
        let step = backoff_step(0, &rate_limited_reason());
        assert_eq!(step, 2, "ERR-5003 starts two rungs down the ladder");
        let backoff = crate::client::Config::default().reconnect;
        let smallest = backoff.delay_with(step, |_| Some(0));
        assert!(
            smallest > Duration::from_secs(1),
            "the smallest wait after ERR-5003 is {smallest:?}"
        );
    }

    /// ERR-5003 の経路では、セッションが次に使う段数は 2 段先で、その cap は
    /// 結合テスト (`tests/ws_feed.rs` の `fast_client`) の設定 (20 ms..80 ms) で
    /// ちょうど 80 ms になる。段数が 1 でも 3 でも、乱数に関係なくここで落ちる。
    ///
    /// 段数そのものも確かめるのは、この設定では 2 段目以降が `max` に飽和し、
    /// cap だけでは 2 段と 3 段を区別できないからである。
    #[test]
    fn a_rate_limit_uses_the_second_rung_whose_cap_is_eighty_milliseconds() {
        use crate::client::Timeout;

        let step = backoff_step(0, &rate_limited_reason());
        assert_eq!(step, 2, "ERR-5003 starts two rungs down the ladder");
        let ms = |n| Timeout::new(Duration::from_millis(n)).unwrap();
        let backoff = Backoff::new(ms(20), ms(80)).unwrap();
        assert_eq!(
            backoff.cap(step),
            Duration::from_millis(80),
            "the cap of the rung the session will use next"
        );
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
