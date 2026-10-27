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
//! - `Connected` の間、`sent.subscribed` はこの接続に書き込みを始めた
//!   コマンドの集合と等しい。再接続のたびに空から始まる。
//! - `Connected` の間、取得済みのペーサのスロットは高々 1 つで、コマンドを
//!   書くか、書くべきコマンドがなくなるまで `Connection::slot` に残る。
//! - Private の `TokenLease` は `extend_at <= expires_at` を保ち、どちらも
//!   取得または延長の要求を「送った」時刻から計算するので過大にならない。

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_tungstenite::tungstenite::Message;
use zeroize::Zeroizing;

use super::{
    Disconnect, DisconnectReason, FeedChannel, Pacer, PrivateChannel, PrivateEvent, PublicChannel,
    PublicEvent,
};
use crate::client::{Authenticated, Client, Inner, Limits};
use crate::error::{
    ApiGroup, ContractViolation, Delivery, Error, ErrorCode, Phase, ProtocolError,
    ProtocolErrorKind, Result, TransportError, TransportErrorKind,
};
use crate::secret::AccessToken;
use crate::transport::ws::{self, WsConnectError, WsStream};

/// エラーに載せる操作名。ハンドシェイクのパス。トークンは決して含まない。
const OPERATION: &str = "/v1";

/// セッションが必要とする、チャネル固有の知識。封印されている (非公開
/// モジュール) ので、これらのメソッドは公開 API の一部ではない。
pub(crate) mod codec {
    use super::*;

    pub trait Codec: Sized + PartialEq {
        type Event;
        /// 置き換えのキー: 同じキーへの後の subscribe が先のものを置き換える。
        type Key: Ord + Clone + Send;
        const API: ApiGroup;
        fn key(&self) -> Self::Key;
        /// `{"command":"subscribe","channel":"ticker","symbol":"BTC"}` の類。
        /// チャネルに `option` があればそれも含む。
        fn command(&self, subscribe: bool) -> String;
        /// テキストメッセージ 1 つをデコードする。
        ///
        /// # エラー
        /// `channel` が既知のチャネルを指すのにペイロードが一致しないとき
        /// `WsMessage` (フェイルクローズド)。`error` フィールドを持つ
        /// メッセージは `Rejected` になる。それ以外の形も `WsMessage`。
        fn decode(text: &str) -> core::result::Result<Self::Event, ProtocolErrorKind>;
        /// データイベントのキー。`Disconnected`、`Reconnected`、`Rejected` は `None`。
        fn event_key(event: &Self::Event) -> Option<Self::Key>;
        fn disconnected(d: Disconnect) -> Self::Event;
        fn reconnected() -> Self::Event;
    }
}

use codec::Codec;

/// subscribe / unsubscribe コマンドの本文。
#[derive(Serialize)]
struct Command<'a> {
    command: &'static str,
    channel: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    option: Option<&'static str>,
}

impl Command<'_> {
    fn render(&self) -> String {
        serde_json::to_string(self).expect("a command is a plain object of strings")
    }
}

fn command_word(subscribe: bool) -> &'static str {
    if subscribe {
        "subscribe"
    } else {
        "unsubscribe"
    }
}

/// 受信メッセージの先頭だけ: どのチャネルか、または拒否か。
#[derive(serde::Deserialize)]
struct Head {
    channel: Option<String>,
    error: Option<serde_json::Value>,
}

fn ws_message(e: &serde_json::Error) -> ProtocolErrorKind {
    ProtocolErrorKind::WsMessage {
        line: e.line(),
        column: e.column(),
    }
}

fn head(text: &str) -> core::result::Result<Head, ProtocolErrorKind> {
    serde_json::from_str(text).map_err(|e| ws_message(&e))
}

fn payload<T: DeserializeOwned>(text: &str) -> core::result::Result<T, ProtocolErrorKind> {
    serde_json::from_str(text).map_err(|e| ws_message(&e))
}

/// サーバの拒否文。文字列ならそのまま、それ以外の JSON 値ならその表記。
fn rejection(error: serde_json::Value) -> Box<str> {
    match error {
        serde_json::Value::String(text) => text.into_boxed_str(),
        other => other.to_string().into_boxed_str(),
    }
}

/// 既知のチャネル名を持たないメッセージ。位置は先頭を指す。
const UNKNOWN_SHAPE: ProtocolErrorKind = ProtocolErrorKind::WsMessage { line: 1, column: 1 };

impl Codec for PublicChannel {
    type Event = PublicEvent;
    type Key = (u8, crate::domain::Symbol);
    const API: ApiGroup = ApiGroup::PublicWs;
    fn key(&self) -> Self::Key {
        match self {
            PublicChannel::Ticker(s) => (0, *s),
            PublicChannel::OrderBooks(s) => (1, *s),
            PublicChannel::Trades { symbol, .. } => (2, *symbol),
        }
    }
    fn command(&self, subscribe: bool) -> String {
        let (channel, symbol, option) = match self {
            PublicChannel::Ticker(s) => ("ticker", s, None),
            PublicChannel::OrderBooks(s) => ("orderbooks", s, None),
            PublicChannel::Trades { symbol, taker_only } => {
                ("trades", symbol, taker_only.then_some("TAKER_ONLY"))
            }
        };
        Command {
            command: command_word(subscribe),
            channel,
            symbol: Some(symbol.as_str()),
            option,
        }
        .render()
    }
    fn decode(text: &str) -> core::result::Result<PublicEvent, ProtocolErrorKind> {
        let head = head(text)?;
        if let Some(error) = head.error {
            return Ok(PublicEvent::Rejected(rejection(error)));
        }
        match head.channel.as_deref() {
            Some("ticker") => payload(text).map(PublicEvent::Ticker),
            Some("orderbooks") => payload(text).map(PublicEvent::OrderBook),
            Some("trades") => payload(text).map(PublicEvent::Trade),
            _ => Err(UNKNOWN_SHAPE),
        }
    }
    fn event_key(event: &PublicEvent) -> Option<Self::Key> {
        match event {
            PublicEvent::Ticker(t) => Some((0, t.symbol)),
            PublicEvent::OrderBook(b) => Some((1, b.symbol)),
            PublicEvent::Trade(t) => Some((2, t.symbol)),
            _ => None,
        }
    }
    fn disconnected(d: Disconnect) -> PublicEvent {
        PublicEvent::Disconnected(d)
    }
    fn reconnected() -> PublicEvent {
        PublicEvent::Reconnected
    }
}

impl Codec for PrivateChannel {
    type Event = PrivateEvent;
    type Key = u8;
    const API: ApiGroup = ApiGroup::PrivateWs;
    fn key(&self) -> u8 {
        match self {
            PrivateChannel::ExecutionEvents => 0,
            PrivateChannel::OrderEvents => 1,
            PrivateChannel::PositionEvents => 2,
            PrivateChannel::PositionSummaryEvents { .. } => 3,
        }
    }
    fn command(&self, subscribe: bool) -> String {
        let (channel, option) = match self {
            PrivateChannel::ExecutionEvents => ("executionEvents", None),
            PrivateChannel::OrderEvents => ("orderEvents", None),
            PrivateChannel::PositionEvents => ("positionEvents", None),
            PrivateChannel::PositionSummaryEvents { periodic } => {
                ("positionSummaryEvents", periodic.then_some("PERIODIC"))
            }
        };
        Command {
            command: command_word(subscribe),
            channel,
            symbol: None,
            option,
        }
        .render()
    }
    fn decode(text: &str) -> core::result::Result<PrivateEvent, ProtocolErrorKind> {
        let head = head(text)?;
        if let Some(error) = head.error {
            return Ok(PrivateEvent::Rejected(rejection(error)));
        }
        match head.channel.as_deref() {
            Some("executionEvents") => payload(text).map(PrivateEvent::Execution),
            Some("orderEvents") => payload(text).map(PrivateEvent::Order),
            Some("positionEvents") => payload(text).map(PrivateEvent::Position),
            Some("positionSummaryEvents") => payload(text).map(PrivateEvent::PositionSummary),
            _ => Err(UNKNOWN_SHAPE),
        }
    }
    fn event_key(event: &PrivateEvent) -> Option<u8> {
        match event {
            PrivateEvent::Execution(_) => Some(0),
            PrivateEvent::Order(_) => Some(1),
            PrivateEvent::Position(_) => Some(2),
            PrivateEvent::PositionSummary(_) => Some(3),
            _ => None,
        }
    }
    fn disconnected(d: Disconnect) -> PrivateEvent {
        PrivateEvent::Disconnected(d)
    }
    fn reconnected() -> PrivateEvent {
        PrivateEvent::Reconnected
    }
}

/// 1 回の接続試行。`State::Dialing` に保存され、キャンセルの後も続きから
/// 進む。
type DialFuture = Pin<Box<dyn Future<Output = Dialed> + Send>>;
/// 進行中の `PUT ws-auth`。送った時刻と結果を返す。
type ExtendFuture = Pin<Box<dyn Future<Output = (Instant, Result<()>)> + Send>>;

/// 接続試行の結果。`acquired` は試行の中で新しく発行されたトークンで、
/// 接続の成否にかかわらずセッションに引き取られる (401/403/404 を除く)。
struct Dialed {
    result: core::result::Result<WsStream, DialFailure>,
    acquired: Option<TokenLease>,
}

enum DialFailure {
    /// フィードを終える: `Api`、`Contract`、メンテナンス以外の `Protocol`。
    Terminal(Error),
    /// `Disconnected` を返してバックオフ後に再試行する。
    Retry {
        reason: DisconnectReason,
        token_dead: bool,
    },
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

enum Auth {
    Public,
    /// トークンは接続より長生きする: 生きている間は再接続が再利用するので、
    /// 発行 (別プロセスのトークンを追い出し得る) はまれにしか起きない。
    Private {
        client: Client<Authenticated>,
        lease: Option<TokenLease>,
        schedule: LeaseSchedule,
        /// 進行中の延長。接続が落ちたら捨て、スケジュールが改めて起動する。
        extend: Option<ExtendFuture>,
    },
}

/// # 不変条件
/// `extend_at <= expires_at`。両方とも取得または延長の要求を「送った」時刻
/// から計算するので、決して過大にならない。`extend_at == expires_at` は
/// 「もう延長しない」を意味し、次の接続でトークンを取り直す。
struct TokenLease {
    /// 接続試行と延長の future が借りるので `Arc`。複製はされない。
    token: Arc<AccessToken>,
    extend_at: Instant,
    expires_at: Instant,
}

/// トークンの寿命の刻み。既定は取引所の 60 分に対して 45 分で延長、失敗なら
/// 1 分後に再試行。テストは短くする。
#[derive(Clone, Copy, Debug)]
pub(crate) struct LeaseSchedule {
    pub(crate) extend_after: Duration,
    pub(crate) retry_after: Duration,
    pub(crate) expires_after: Duration,
}

impl Default for LeaseSchedule {
    fn default() -> Self {
        Self {
            extend_after: Duration::from_secs(45 * 60),
            retry_after: Duration::from_secs(60),
            expires_after: Duration::from_secs(60 * 60),
        }
    }
}

impl TokenLease {
    fn new(token: Arc<AccessToken>, sent_at: Instant, schedule: LeaseSchedule) -> Self {
        Self {
            token,
            extend_at: sent_at + schedule.extend_after,
            expires_at: sent_at + schedule.expires_after,
        }
    }

    fn is_live(&self, now: Instant) -> bool {
        now < self.expires_at
    }
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
}

pub(crate) struct Connection {
    /// tokio-tungstenite の接続。フレーミング、ping への pong、サイズ上限は
    /// すべてこの中にある。書き込みは `poll_ready`、`start_send`、`poll_flush`
    /// で行い、`SinkExt::send` を `select!` の中に置かない (SYNTHESIS を参照)。
    stream: WsStream,
    connected_at: Instant,
    last_frame_at: Instant,
    /// 最初に `poll_flush` が `Pending` を返した時刻。`ws_write` を超えたら切る。
    flush_since: Option<Instant>,
    /// 次のコマンドのために取得したペーサのスロット。
    slot: Option<Instant>,
}

/// `Connected` で 1 回のポーリングが観測したこと。
enum Step {
    Message(Message),
    Failed(TransportErrorKind),
    /// スロットが来て、ソケットが書き込みを受け付けられる。
    WriteReady,
    SlotReached,
    Idle,
    WriteTimedOut,
    ExtendDue,
    Extended(Instant, Result<()>),
}

impl Connection {
    /// 1 回のポーリング: まず flush (キャンセル前に積んだフレームと、
    /// tungstenite が読みながら積んだ pong を押し出す)、次に書きたければ
    /// 書き込み準備、最後に読む。
    fn poll_socket(
        &mut self,
        cx: &mut Context<'_>,
        want_write: bool,
        limits: &Limits,
    ) -> Poll<Step> {
        match self.stream.poll_flush_unpin(cx) {
            Poll::Ready(Ok(())) => self.flush_since = None,
            Poll::Ready(Err(e)) => return Poll::Ready(Step::Failed(ws::error_kind(&e, limits))),
            Poll::Pending => {
                self.flush_since.get_or_insert_with(Instant::now);
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

/// ハンドシェイクの失敗を再試行の理由に写す。
fn dial_failure(api: ApiGroup, e: WsConnectError) -> DialFailure {
    match e {
        WsConnectError::Transport(kind) => DialFailure::Retry {
            reason: DisconnectReason::Transport(transport_error(api, kind, Delivery::NotSent)),
            token_dead: false,
        },
        WsConnectError::Rejected { http_status: 503 } => DialFailure::Retry {
            reason: DisconnectReason::Maintenance,
            token_dead: false,
        },
        WsConnectError::Rejected { http_status } => DialFailure::Retry {
            reason: DisconnectReason::HandshakeRejected { http_status },
            token_dead: api == ApiGroup::PrivateWs && matches!(http_status, 401 | 403 | 404),
        },
    }
}

/// `POST ws-auth` の失敗。メンテナンス (ERR-5201/5202/5203、または
/// エンベロープのない 503) とネットワークは再試行、それ以外は終端。
fn acquire_failure(e: Error) -> DialFailure {
    const MAINTENANCE: [ErrorCode; 3] = [
        ErrorCode::SCHEDULED_MAINTENANCE,
        ErrorCode::EMERGENCY_MAINTENANCE,
        ErrorCode::PRE_OPEN,
    ];
    let maintenance = DialFailure::Retry {
        reason: DisconnectReason::Maintenance,
        token_dead: false,
    };
    match e {
        Error::Api(api) if MAINTENANCE.iter().any(|code| api.has(*code)) => maintenance,
        Error::Protocol(p) if *p.kind() == ProtocolErrorKind::HttpStatus(503) => maintenance,
        Error::Transport(t) => DialFailure::Retry {
            reason: DisconnectReason::Transport(t),
            token_dead: false,
        },
        other => DialFailure::Terminal(other),
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

    fn pacer(&self) -> &Pacer {
        match C::API {
            ApiGroup::PrivateWs => &self.inner.private_ws_pacer,
            _ => &self.inner.public_ws_pacer,
        }
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
        if let (Auth::Private { lease, .. }, Some(acquired)) = (&mut self.auth, dialed.acquired) {
            *lease = Some(acquired);
        }
        match dialed.result {
            Ok(stream) => {
                let now = Instant::now();
                let connection = Connection {
                    stream,
                    connected_at: now,
                    last_frame_at: now,
                    flush_since: None,
                    slot: None,
                };
                self.state = State::Connected(
                    Box::new(connection),
                    Sent {
                        subscribed: BTreeMap::new(),
                    },
                );
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
            Err(DialFailure::Retry { reason, token_dead }) => {
                if token_dead && let Auth::Private { lease, .. } = &mut self.auth {
                    *lease = None;
                }
                Some(Ok(self.disconnected(reason)))
            }
        }
    }

    /// `Waiting` に移り、`Disconnected` を作る。進行中の延長は捨てる。
    fn disconnected(&mut self, reason: DisconnectReason) -> <C as Codec>::Event {
        let delay = self.inner.config.reconnect.delay(self.attempt);
        self.attempt = self.attempt.saturating_add(1);
        self.state = State::Waiting {
            until: Instant::now() + delay,
        };
        if let Auth::Private { extend, .. } = &mut self.auth {
            *extend = None;
        }
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
        let now = Instant::now();

        let command = next_command::<C>(desired, &sent.subscribed);
        if command.is_none() {
            conn.slot = None;
        } else if conn.slot.is_none() {
            conn.slot = Some(pacer.claim());
        }
        let slot = conn.slot;
        let want_write = slot.is_some_and(|at| at <= now);
        let idle_at = conn.last_frame_at + timeouts.ws_idle.get();
        let write_deadline = conn
            .flush_since
            .map(|since| since + timeouts.ws_write.get());
        let extend_at = match auth {
            Auth::Private {
                lease: Some(lease),
                extend: None,
                ..
            } if lease.extend_at < lease.expires_at => Some(lease.extend_at),
            _ => None,
        };
        let mut extend = match auth {
            Auth::Private { extend, .. } => extend.as_mut(),
            Auth::Public => None,
        };

        let step = tokio::select! {
            biased;
            step = poll_fn(|cx| conn.poll_socket(cx, want_write, &inner.config.limits)) => step,
            (sent_at, result) = poll_fn(|cx| match extend.as_mut() {
                Some(fut) => fut.as_mut().poll(cx),
                None => Poll::Pending,
            }) => Step::Extended(sent_at, result),
            _ = sleep_until(slot.unwrap_or(now)), if slot.is_some() && !want_write => Step::SlotReached,
            _ = sleep_until(write_deadline.unwrap_or(now)), if write_deadline.is_some() => Step::WriteTimedOut,
            _ = sleep_until(extend_at.unwrap_or(now)), if extend_at.is_some() => Step::ExtendDue,
            _ = sleep_until(idle_at) => Step::Idle,
        };

        match step {
            Step::Message(Message::Text(text)) => match C::decode(&text) {
                Ok(event) => match C::event_key(&event) {
                    Some(key) if !desired.contains_key(&key) => None,
                    _ => Some(Ok(event)),
                },
                Err(kind) => {
                    self.state = State::Closed;
                    Some(Err(Error::Protocol(ProtocolError {
                        api: C::API,
                        operation: OPERATION,
                        kind,
                    })))
                }
            },
            Step::Message(Message::Binary(_)) => Some(Ok(self.drop_connection(live_failure(
                C::API,
                TransportErrorKind::Framing("websocket binary frame"),
            )))),
            Step::Message(Message::Close(frame)) => {
                let code = frame.map(|f| u16::from(f.code));
                Some(Ok(
                    self.drop_connection(DisconnectReason::ServerClosed { code })
                ))
            }
            Step::Message(_) => None,
            Step::Failed(kind) => Some(Ok(self.drop_connection(live_failure(C::API, kind)))),
            Step::WriteReady => {
                let (channel, subscribe) = command.expect("want_write implies a command");
                let frame = Message::text(channel.command(subscribe));
                if let Err(e) = conn.stream.start_send_unpin(frame) {
                    let kind = ws::error_kind(&e, &inner.config.limits);
                    return Some(Ok(self.drop_connection(live_failure(C::API, kind))));
                }
                let key = channel.key();
                if subscribe {
                    sent.subscribed.insert(key, channel);
                } else {
                    sent.subscribed.remove(&key);
                }
                conn.slot = None;
                conn.flush_since.get_or_insert(now);
                None
            }
            Step::SlotReached => None,
            Step::Idle => Some(Ok(self.drop_connection(live_failure(
                C::API,
                TransportErrorKind::Timeout(Phase::WsIdle),
            )))),
            // タイマーは select の前の `flush_since` から組んだ。同じ select の
            // 中で flush が完了していれば古いので、今の状態で判定し直す。
            Step::WriteTimedOut => match conn.flush_since {
                Some(since) if since + timeouts.ws_write.get() <= Instant::now() => Some(Ok(self
                    .drop_connection(live_failure(
                        C::API,
                        TransportErrorKind::Timeout(Phase::WsWrite),
                    )))),
                _ => None,
            },
            Step::ExtendDue => {
                self.start_extend();
                None
            }
            Step::Extended(sent_at, result) => {
                self.extended(sent_at, result);
                None
            }
        }
    }

    /// 1 回の接続試行を自己完結した future として始める。`State::Dialing` に
    /// 保存でき、キャンセルの後に再開できる。
    ///
    /// Private: 生きているリースがあればそのトークンで接続する。なければ
    /// `POST ws-auth` で取得し、新しいリースを結果に載せる。
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
                    acquired: None,
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
                let live = lease
                    .as_ref()
                    .filter(|l| l.is_live(Instant::now()))
                    .map(|l| l.token.clone());
                Box::pin(async move {
                    let (token, acquired) = match live {
                        Some(token) => (token, None),
                        None => {
                            let sent_at = Instant::now();
                            match client.acquire_ws_token().await {
                                Ok(token) => {
                                    let token = Arc::new(token);
                                    let lease = TokenLease::new(token.clone(), sent_at, schedule);
                                    (token, Some(lease))
                                }
                                Err(e) => {
                                    return Dialed {
                                        result: Err(acquire_failure(e)),
                                        acquired: None,
                                    };
                                }
                            }
                        }
                    };
                    let path = Zeroizing::new(format!("{OPERATION}/{}", token.reveal()));
                    let result = ws::connect(
                        &inner.connector,
                        inner.endpoints.private_ws.base(),
                        &path,
                        &inner.config.timeouts,
                        &inner.config.limits,
                    )
                    .await
                    .map_err(|e| dial_failure(ApiGroup::PrivateWs, e));
                    Dialed { result, acquired }
                })
            }
        }
    }

    /// `PUT ws-auth` を future として始め、`Auth::Private::extend` に置く。
    /// 冪等: キャンセルされた延長はスケジュールが改めて始める。
    fn start_extend(&mut self) {
        let Auth::Private {
            client,
            lease: Some(lease),
            extend,
            ..
        } = &mut self.auth
        else {
            return;
        };
        let client = client.clone();
        let token = lease.token.clone();
        let sent_at = Instant::now();
        *extend = Some(Box::pin(async move {
            (sent_at, client.extend_ws_token(&token).await)
        }));
    }

    /// 延長の結果。成功なら `extend_at` と `expires_at` は送った時刻から
    /// 45/60 分後に動く。失敗ならリースは保ち、1 分後 (ただし `expires_at`
    /// まで) に再試行する。その後は次の接続が新しいトークンを取得する。
    fn extended(&mut self, sent_at: Instant, result: Result<()>) {
        let Auth::Private {
            lease: Some(lease),
            schedule,
            extend,
            ..
        } = &mut self.auth
        else {
            return;
        };
        *extend = None;
        match result {
            Ok(()) => {
                lease.extend_at = sent_at + schedule.extend_after;
                lease.expires_at = sent_at + schedule.expires_after;
            }
            Err(_) => {
                lease.extend_at = (Instant::now() + schedule.retry_after).min(lease.expires_at);
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
                if let (Auth::Private { lease, .. }, Some(acquired)) =
                    (&mut self.auth, dialed.acquired)
                {
                    *lease = Some(acquired);
                }
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
            extend,
            ..
        } = self.auth
            && lease.is_live(Instant::now())
        {
            // 延長の future がトークンを借りていれば先に手放す。
            drop(extend);
            if let Ok(token) = Arc::try_unwrap(lease.token) {
                let deleted = client.delete_ws_token(token).await;
                outcome = outcome.and(deleted);
            }
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
    use serde_json::{Value, json};

    use super::*;
    use crate::domain::Symbol;
    use crate::mock::{Api, MockExchange, fixture};

    fn parsed(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn public_commands_carry_channel_symbol_and_option() {
        assert_eq!(
            parsed(&PublicChannel::Ticker(Symbol::BTC).command(true)),
            json!({"command": "subscribe", "channel": "ticker", "symbol": "BTC"})
        );
        assert_eq!(
            parsed(&PublicChannel::OrderBooks(Symbol::ETH).command(false)),
            json!({"command": "unsubscribe", "channel": "orderbooks", "symbol": "ETH"})
        );
        assert_eq!(
            parsed(
                &PublicChannel::Trades {
                    symbol: Symbol::XRP,
                    taker_only: true
                }
                .command(true)
            ),
            json!({"command": "subscribe", "channel": "trades", "symbol": "XRP", "option": "TAKER_ONLY"})
        );
        assert_eq!(
            parsed(
                &PublicChannel::Trades {
                    symbol: Symbol::XRP,
                    taker_only: false
                }
                .command(true)
            ),
            json!({"command": "subscribe", "channel": "trades", "symbol": "XRP"})
        );
    }

    #[test]
    fn private_commands_have_no_symbol() {
        assert_eq!(
            parsed(&PrivateChannel::ExecutionEvents.command(true)),
            json!({"command": "subscribe", "channel": "executionEvents"})
        );
        assert_eq!(
            parsed(&PrivateChannel::PositionSummaryEvents { periodic: true }.command(true)),
            json!({"command": "subscribe", "channel": "positionSummaryEvents", "option": "PERIODIC"})
        );
        assert_eq!(
            parsed(&PrivateChannel::OrderEvents.command(false)),
            json!({"command": "unsubscribe", "channel": "orderEvents"})
        );
    }

    #[test]
    fn a_known_channel_with_a_wrong_payload_fails_closed_with_a_position() {
        let err = PublicChannel::decode(r#"{"channel":"ticker","ask":"1"}"#).unwrap_err();
        assert!(
            matches!(err, ProtocolErrorKind::WsMessage { .. }),
            "{err:?}"
        );
        let err = PrivateChannel::decode(r#"{"channel":"orderEvents","orderId":"x"}"#).unwrap_err();
        assert!(
            matches!(err, ProtocolErrorKind::WsMessage { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn an_unknown_channel_or_a_non_object_fails_closed() {
        for text in [r#"{"channel":"klines"}"#, "[1,2]", "", r#"{"hello":1}"#] {
            assert!(
                matches!(
                    PublicChannel::decode(text),
                    Err(ProtocolErrorKind::WsMessage { .. })
                ),
                "{text:?}"
            );
            assert!(
                matches!(
                    PrivateChannel::decode(text),
                    Err(ProtocolErrorKind::WsMessage { .. })
                ),
                "{text:?}"
            );
        }
    }

    #[test]
    fn an_error_field_becomes_rejected_with_its_text() {
        match PublicChannel::decode(r#"{"error":"Requests are too many."}"#).unwrap() {
            PublicEvent::Rejected(text) => assert_eq!(&*text, "Requests are too many."),
            other => panic!("{other:?}"),
        }
        match PrivateChannel::decode(r#"{"error":{"code":1}}"#).unwrap() {
            PrivateEvent::Rejected(text) => assert_eq!(&*text, r#"{"code":1}"#),
            other => panic!("{other:?}"),
        }
    }

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
    fn a_dead_private_token_is_only_the_documented_statuses() {
        for status in [401, 403, 404] {
            assert!(matches!(
                dial_failure(
                    ApiGroup::PrivateWs,
                    WsConnectError::Rejected {
                        http_status: status
                    }
                ),
                DialFailure::Retry {
                    token_dead: true,
                    reason: DisconnectReason::HandshakeRejected { .. }
                }
            ));
        }
        assert!(matches!(
            dial_failure(
                ApiGroup::PrivateWs,
                WsConnectError::Rejected { http_status: 503 }
            ),
            DialFailure::Retry {
                token_dead: false,
                reason: DisconnectReason::Maintenance
            }
        ));
        assert!(matches!(
            dial_failure(
                ApiGroup::PublicWs,
                WsConnectError::Rejected { http_status: 404 }
            ),
            DialFailure::Retry {
                token_dead: false,
                ..
            }
        ));
    }

    /// 45 分を待てないので、スケジュールを縮めて延長が `PUT ws-auth` を
    /// 送ることを確かめる。キャンセルを繰り返しても PUT は 1 回だけ。
    #[tokio::test]
    async fn the_token_is_extended_on_schedule_even_under_cancellation() {
        let mock = MockExchange::start().await;
        let ws = mock.ws();
        let client = mock.client().authenticate(MockExchange::credentials());
        client.unpaced_ws_commands_for_loopback().unwrap();
        let schedule = LeaseSchedule {
            extend_after: Duration::from_millis(150),
            retry_after: Duration::from_millis(50),
            expires_after: Duration::from_secs(60),
        };
        let mut session = Session::private_with(client, schedule);
        session.desire(PrivateChannel::OrderEvents).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut extended = Vec::new();
        while extended.is_empty() && Instant::now() < deadline {
            let _ = tokio::time::timeout(Duration::from_millis(3), session.next()).await;
            extended = mock.with_tokens(|t| t.extended.clone());
        }
        let issued = mock.with_tokens(|t| t.issued.clone());
        assert_eq!(issued.len(), 1, "one token for the whole run");
        assert_eq!(extended, issued, "the live token was extended");
        assert_eq!(ws.handshakes(Api::Private), 1, "no reconnect was needed");

        let settle = Instant::now() + Duration::from_millis(100);
        while Instant::now() < settle {
            let _ = tokio::time::timeout(Duration::from_millis(3), session.next()).await;
        }
        let puts = mock.requests().iter().filter(|r| r.method == "PUT").count();
        assert_eq!(
            puts, 1,
            "a cancelled extend is not retried while it is in flight"
        );
        session.close().await.unwrap();
        mock.assert_done();
    }

    #[test]
    fn every_ws_fixture_decodes_to_its_event() {
        let public = |name: &str| PublicChannel::decode(&fixture(name)).unwrap();
        assert!(
            matches!(public("ws_public_ticker.json"), PublicEvent::Ticker(t) if t.symbol == Symbol::BTC)
        );
        assert!(
            matches!(public("ws_public_orderbooks.json"), PublicEvent::OrderBook(b) if b.asks.len() == 2)
        );
        assert!(
            matches!(public("ws_public_trades.json"), PublicEvent::Trade(t) if t.symbol == Symbol::BTC)
        );
        let private = |name: &str| PrivateChannel::decode(&fixture(name)).unwrap();
        assert!(
            matches!(private("ws_private_executionEvents.json"), PrivateEvent::Execution(e) if e.symbol == Symbol::BTC_JPY)
        );
        assert!(matches!(
            private("ws_private_orderEvents.json"),
            PrivateEvent::Order(_)
        ));
        assert!(matches!(
            private("ws_private_positionEvents.json"),
            PrivateEvent::Position(_)
        ));
        assert!(matches!(
            private("ws_private_positionSummaryEvents.json"),
            PrivateEvent::PositionSummary(_)
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
