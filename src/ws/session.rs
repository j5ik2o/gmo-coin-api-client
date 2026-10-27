//! The reconnecting session behind `Feed`. One owner, no tasks, no locks.
//!
//! State machine (all of it in `Session`, none in a future's locals, which
//! is what makes `next` cancel-safe):
//!
//! ```text
//!            +--------------------- backoff elapsed --------------------+
//!            v                                                          |
//!   Idle -> Dialing(fut) --ok--> Connected{sent, outbox} --drop--> Waiting{until}
//!            |                     |                                    ^
//!            +--terminal err--> Closed <--- close() / terminal err      |
//!            +--retryable err-------------------------------------------+
//! ```

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use super::{Disconnect, FeedChannel, PrivateChannel, PrivateEvent, PublicChannel, PublicEvent};
use crate::client::{Authenticated, Client, Inner};
use crate::error::{ApiGroup, ContractViolation, Error, ProtocolErrorKind, Result};
use crate::secret::AccessToken;
use crate::transport::ws::WsStream;

/// Channel-specific knowledge the session needs. Sealed (private module), so
/// these methods are not part of the public API.
pub(crate) mod codec {
    use super::*;

    pub trait Codec: Sized {
        type Event;
        /// Replacement key: a later subscribe with the same key replaces.
        type Key: Ord + Clone + Send;
        const API: ApiGroup;
        fn key(&self) -> Self::Key;
        /// `{"command":"subscribe","channel":"ticker","symbol":"BTC"}` and
        /// friends, with `option` where the channel has one.
        fn command(&self, subscribe: bool) -> String;
        /// Decodes one text message.
        ///
        /// # Errors
        /// `WsMessage` when `channel` names a known channel but the payload
        /// does not match (fail closed). A message with an `error` field
        /// becomes `Rejected`. Any other shape is also `WsMessage`.
        fn decode(text: &str) -> core::result::Result<Self::Event, ProtocolErrorKind>;
        fn disconnected(d: Disconnect) -> Self::Event;
        fn reconnected() -> Self::Event;
    }
}

use codec::Codec;

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
        let _ = subscribe;
        todo!()
    }
    fn decode(text: &str) -> core::result::Result<PublicEvent, ProtocolErrorKind> {
        // TODO peek {"channel": ..} (or {"error": ..} => Rejected), then decode
        // the matching struct from the same text: "ticker" => rest::Ticker,
        // "orderbooks" => OrderBookUpdate, "trades" => TradeEvent.
        let _ = text;
        todo!()
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
        let _ = subscribe;
        todo!()
    }
    fn decode(text: &str) -> core::result::Result<PrivateEvent, ProtocolErrorKind> {
        let _ = text;
        todo!()
    }
    fn disconnected(d: Disconnect) -> PrivateEvent {
        PrivateEvent::Disconnected(d)
    }
    fn reconnected() -> PrivateEvent {
        PrivateEvent::Reconnected
    }
}

type DialFuture = Pin<Box<dyn Future<Output = Result<Connection>> + Send>>;

pub(crate) struct Session<C: FeedChannel> {
    inner: Arc<Inner>,
    auth: Auth,
    /// Caller intent. Bounded by `Limits::max_subscriptions`.
    desired: BTreeMap<<C as Codec>::Key, C>,
    state: State<C>,
    /// Failed attempts since the last connection that stayed up.
    attempt: u32,
    /// Whether a connection has ever been up (decides `Reconnected`).
    ever_connected: bool,
}

enum Auth {
    Public,
    /// The token outlives connections: reconnects reuse it while it is live,
    /// so issuance (which can evict another process's token) stays rare.
    Private {
        client: Client<Authenticated>,
        lease: Option<TokenLease>,
    },
}

/// # Invariants
/// `extend_at < expires_at`. `expires_at` is computed from the time the
/// acquire/extend request was *sent*, so it never overestimates.
struct TokenLease {
    token: AccessToken,
    extend_at: Instant,
    expires_at: Instant,
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

/// What this connection has been told. Reset to empty on every new
/// connection, which is how a reconnect replays the desired set.
struct Sent<C: FeedChannel> {
    subscribed: BTreeMap<<C as Codec>::Key, C>,
}

pub(crate) struct Connection {
    /// tokio-tungstenite の接続。フレーミング、ping への pong、サイズ上限は
    /// すべてこの中にある。書き込みは `poll_ready`、`start_send`、`poll_flush`
    /// で行い、`SinkExt::send` を `select!` の中に置かない (SYNTHESIS を参照)。
    stream: WsStream,
    last_frame_at: Instant,
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

    /// See `Feed::next` for the contract.
    pub(crate) async fn next(&mut self) -> Result<<C as Codec>::Event> {
        // TODO loop over self.state:
        //
        // Closed => return Err(Error::FeedClosed)
        //
        // Idle => self.state = Dialing(self.dial())
        //
        // Waiting { until } => sleep_until(until).await; state = Idle
        //     (cancel-safe: `until` is in self; re-entering sleeps the remainder)
        //
        // Dialing(fut) => match fut.as_mut().await {
        //     Ok(conn) => { state = Connected(conn, Sent::empty()); attempt = 0 after
        //                   it stays up for reconnect.max;
        //                   if ever_connected { return Ok(C::reconnected()) } ever_connected = true }
        //     Err(Api) / Err(Contract) => { state = Closed; return Err(e) }      // terminal
        //     Err(retryable) => { delay = config.reconnect.delay(attempt); attempt += 1;
        //                         state = Waiting { now + delay };
        //                         return Ok(C::disconnected(Disconnect { reason, retry_in: delay })) }
        // }
        //
        // Connected(conn, sent) =>
        //   1. if conn.written < conn.outbox.len(): write_from(outbox, &mut written)
        //      within ws_write (timeout => drop connection as Transport(Timeout(WsWrite)))
        //   2. diff = first key where desired != sent (subscribe missing or replaced,
        //      unsubscribe removed); if any, slot = pacer.claim()
        //   3. select! {
        //        n = conn.stream.read_some(..) => append, then loop decoder.decode:
        //            Ping(p) => queue pong(p)
        //            Close{code} => drop connection, Disconnected(ServerClosed{code})
        //            Text(t) => match C::decode(&t) { Ok(ev) => return Ok(ev),
        //                                             Err(k) => { state = Closed; return Err(Protocol(k)) } }
        //            Binary(_) => treat as framing error => reconnect
        //        _ = sleep_until(slot), if diff => queue command frame; record in sent
        //        _ = sleep_until(conn.last_frame_at + ws_idle) => Disconnected(Transport(Timeout(WsIdle)))
        //        _ = sleep_until(lease.extend_at), if private => self.extend_token()
        //      }
        //   Data for a key no longer in `desired` (unsubscribe in flight) is dropped.
        let _ = (&self.attempt, self.ever_connected);
        todo!()
    }

    /// Starts one connection attempt as a self-contained future, so it can be
    /// stored in `State::Dialing` and resumed after cancellation.
    fn dial(&mut self) -> DialFuture {
        // TODO public: connector.connect(public_ws base) within ws_handshake,
        //   handshake to `prefix + "/v1"`.
        // TODO private:
        //   if lease is None or now >= lease.expires_at:
        //       sent_at = now; token = client.acquire_ws_token().await?;
        //       lease = TokenLease { token, extend_at: sent_at + 45 min, expires_at: sent_at + 60 min }
        //   path = Zeroizing(format!("{prefix}/v1/{}", token.reveal()))
        //   handshake; HandshakeOutcome::Rejected{401|403|404} => lease = None (token dead),
        //   retryable. Maintenance => retryable with reason Maintenance.
        //   The lease lives in Auth (self), so the future captures a clone of
        //   the client and returns the new lease alongside the connection.
        let _ = (&self.auth, &self.inner);
        todo!()
    }

    /// `PUT ws-auth`. On success `extend_at`/`expires_at` move to sent-time
    /// plus 45/60 minutes. On failure the lease is kept and retried in one
    /// minute until `expires_at`; after that the next dial acquires a new
    /// token. Idempotent: a cancelled extend just runs again.
    async fn extend_token(&mut self) -> Result<()> {
        todo!()
    }

    /// Close frame, bounded wait for the echo, then `DELETE ws-auth` for a
    /// private feed. A token the exchange already dropped is not an error.
    pub(crate) async fn close(self) -> Result<()> {
        let _ = Error::FeedClosed;
        todo!()
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
            ever_connected: false,
        }
    }
}

impl Session<PrivateChannel> {
    pub(crate) fn private(client: Client<Authenticated>) -> Self {
        let inner = client.inner.clone();
        Self {
            inner,
            auth: Auth::Private {
                client,
                lease: None,
            },
            desired: BTreeMap::new(),
            state: State::Idle,
            attempt: 0,
            ever_connected: false,
        }
    }
}
