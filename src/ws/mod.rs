//! WebSocket feeds.
//!
//! A [`Feed`] is one logical subscription stream that survives disconnects.
//! It is caller-driven: no background task, no channel. All I/O (connect,
//! handshake, subscribe commands, pongs, token extension, reconnect) happens
//! inside [`Feed::next`], so backpressure is the socket's own and there is
//! no hidden buffer to bound.
//!
//! Subscriptions are desired state, not commands. `subscribe` and
//! `unsubscribe` edit a set and return at once. `next` reconciles the set
//! with what the current connection has been sent, one command per pacer
//! slot. After a reconnect the "sent" side is empty, so the same
//! reconciliation replays every subscription. Subscribing twice is a no-op;
//! a reconnect never double-subscribes.
//!
//! No cargo feature gates this module: the framing is in `wire::ws_frame`
//! and needs no crate beyond what REST already uses.

mod events;
mod session;

pub use events::{
    ExecutionEvent, OrderBookUpdate, OrderEvent, PositionEvent, PositionSummaryEvent, TradeEvent,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::client::{Access, Authenticated, Client};
use crate::domain::Symbol;
use crate::error::{ContractViolation, Result, TransportError};
use crate::rest::Ticker;

/// A channel on the public or private WebSocket API: [`PublicChannel`] or
/// [`PrivateChannel`]. Sealed: the supertrait lives in a private module, so
/// no other type can implement it and its methods are not callable outside
/// the crate. `Feed::<C>::next` yields `C::Event` ([`PublicEvent`] or
/// [`PrivateEvent`]).
pub trait FeedChannel: session::codec::Codec + Clone + Send + 'static {}

/// Public channels. The key for replacement is (channel, symbol): the docs
/// say a later subscribe for the same symbol replaces the earlier one, so
/// `Trades { taker_only: true }` replaces `Trades { taker_only: false }`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PublicChannel {
    Ticker(Symbol),
    OrderBooks(Symbol),
    /// `taker_only` sends `option: "TAKER_ONLY"`.
    Trades {
        symbol: Symbol,
        taker_only: bool,
    },
}

/// Private channels. One of each per connection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PrivateChannel {
    ExecutionEvents,
    OrderEvents,
    PositionEvents,
    /// `periodic` sends `option: "PERIODIC"` (a summary every 5 seconds).
    PositionSummaryEvents {
        periodic: bool,
    },
}

impl FeedChannel for PublicChannel {}
impl FeedChannel for PrivateChannel {}

#[derive(Debug)]
#[non_exhaustive]
pub enum PublicEvent {
    Ticker(Ticker),
    OrderBook(OrderBookUpdate),
    Trade(TradeEvent),
    /// The connection dropped; nothing arrives until `Reconnected`. Data
    /// derived from the stream (a local book) is stale from here.
    Disconnected(Disconnect),
    /// A new connection is up and the subscriptions are being replayed.
    /// Messages sent while disconnected are lost: resynchronize via REST.
    Reconnected,
    /// The server rejected a command, with its text as sent (for example a
    /// subscribe over the per-IP limit from another process).
    Rejected(Box<str>),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum PrivateEvent {
    Execution(ExecutionEvent),
    Order(OrderEvent),
    Position(PositionEvent),
    PositionSummary(PositionSummaryEvent),
    /// As for public. Fills during the gap are not replayed: call
    /// `latest_executions` and `active_orders` after `Reconnected`.
    Disconnected(Disconnect),
    Reconnected,
    Rejected(Box<str>),
}

/// Why the connection dropped and when the next attempt starts.
#[derive(Debug)]
pub struct Disconnect {
    pub reason: DisconnectReason,
    pub retry_in: Duration,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// Network, TLS, framing, or a timeout (including `ws_idle`).
    Transport(TransportError),
    /// The server sent a close frame.
    ServerClosed { code: Option<u16> },
    /// HTTP 503 at the handshake: the documented maintenance answer.
    Maintenance,
}

/// One subscription stream. Built lazily: no I/O until the first `next`.
///
/// # Usage contract
/// - Poll `next` at least every two minutes. Pongs are sent from inside
///   `next`; the server drops a connection after three unanswered pings
///   (one per minute). A feed that is not polled reconnects when it is.
/// - `next` is cancel-safe. All progress (a half-written frame, a dial in
///   flight, the subscription diff) lives in `self`, so using `next` in a
///   `tokio::select!` loses nothing.
/// - Call `close` to end the feed. Dropping it closes the socket without the
///   close handshake and, for a private feed, leaves the token to expire
///   (within 60 minutes) instead of deleting it.
pub struct Feed<C: FeedChannel> {
    session: session::Session<C>,
}

impl<C: FeedChannel> Feed<C> {
    /// Adds `channel` to the desired set, replacing one with the same key.
    ///
    /// # Preconditions
    /// None beyond `C`'s own invariants.
    ///
    /// # Postconditions
    /// The next `next` call that has a pacer slot sends the subscribe.
    ///
    /// # Errors
    /// `Contract(TooManySubscriptions)` when the set already holds
    /// `Limits::max_subscriptions` keys.
    pub fn subscribe(&mut self, channel: C) -> Result<(), ContractViolation> {
        self.session.desire(channel)
    }

    /// Removes the channel with `channel`'s key. Returns whether it was there.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// Returns whether the key was desired. If the current connection had been told, a later `next` sends the unsubscribe at the next pacer slot.
    ///
    /// # Errors
    /// Infallible.
    pub fn unsubscribe(&mut self, channel: &C) -> bool {
        self.session.undesire(channel)
    }

    /// The next event.
    ///
    /// # Preconditions
    /// The usage contract on [`Feed`]: poll at least every two minutes.
    ///
    /// # Postconditions
    /// - Data events arrive only for channels in the desired set at the time
    ///   they were sent.
    /// - Every `Disconnected` is followed by `Reconnected` before any data
    ///   event, unless `next` returns `Err`.
    ///
    /// # Errors
    /// Terminal; the feed is closed afterwards and further calls return
    /// `FeedClosed`:
    /// - `Api` from `ws-auth` (for example ERR-5012, a rejected key).
    /// - `Protocol(WsMessage)` when a message on a subscribed channel does
    ///   not decode. Fails closed: the model no longer matches the exchange.
    ///
    /// Network failures are not errors here. They are `Disconnected` events,
    /// followed by reconnect with `Config::reconnect` backoff.
    pub async fn next(&mut self) -> Result<C::Event> {
        self.session.next().await
    }

    /// Sends the close handshake (bounded by `ws_write`) and, for a private
    /// feed, deletes the token. Idempotent in effect: a token already gone
    /// is not an error.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// The socket is closed. For a private feed the token is deleted, or, if the DELETE failed, it expires on its own within 60 minutes.
    ///
    /// # Errors
    /// `Transport` or `Api` if the close handshake or the DELETE failed. The socket is closed either way.
    pub async fn close(self) -> Result<()> {
        self.session.close().await
    }
}

impl<A: Access> Client<A> {
    /// A feed on the public WebSocket API.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// No I/O. The feed connects on its first `next`.
    ///
    /// # Errors
    /// Infallible.
    pub fn public_feed(&self) -> Feed<PublicChannel> {
        Feed {
            session: session::Session::public(self.inner.clone()),
        }
    }
}

impl Client<Authenticated> {
    /// A feed on the private WebSocket API. The token is acquired on the
    /// first `next`, extended before it expires, and deleted by `close`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// No I/O. The token is acquired on the first `next`.
    ///
    /// # Errors
    /// Infallible.
    pub fn private_feed(&self) -> Feed<PrivateChannel> {
        Feed {
            session: session::Session::private(self.clone()),
        }
    }
}

/// The per-IP command budget: one subscribe or unsubscribe per second.
///
/// Shared by every feed from one `Client` (and its clones) because the limit
/// is per IP, not per connection. The shared state is one atomic "next free
/// slot", claimed by compare-and-swap; there is no lock to hold across an
/// `.await`. A slot claimed by a command that is then cancelled is wasted,
/// which only ever makes the pace slower, never faster.
pub(crate) struct Pacer {
    origin: Instant,
    next_free_nanos: AtomicU64,
}

impl Pacer {
    pub(crate) const INTERVAL: Duration = Duration::from_millis(1_050);

    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
            next_free_nanos: AtomicU64::new(0),
        }
    }

    /// Claims the next slot and returns when it starts.
    pub(crate) fn claim(&self) -> Instant {
        // TODO loop { cur = load; start = max(cur, now - origin);
        //   if cas(cur, start + INTERVAL) succeeds { return origin + start } }
        let _ = (&self.origin, self.next_free_nanos.load(Ordering::Relaxed));
        todo!()
    }
}
