//! The client handle and its configuration.

use core::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crate::domain::{InvalidValue, Primitive, Violation};
use crate::endpoint::Endpoints;
use crate::secret::Credentials;
use crate::wire::Connector;
use crate::wire::http::Pool;
use crate::ws::Pacer;

/// One handle for all four APIs.
///
/// The type parameter decides what may be called, at compile time:
///
/// - `Client<Anonymous>` has the public REST methods and `public_feed`.
/// - `Client<Authenticated>` has those plus every private REST method and
///   `private_feed`.
///
/// There is no runtime "missing credentials" error, because a private call
/// on an anonymous client does not compile.
///
/// `Clone` is cheap (one `Arc`). Clones share the connection pools and the
/// WebSocket command pacer. That sharing is deliberate: the subscribe limit is
/// per IP, not per connection.
#[derive(Clone)]
pub struct Client<A: Access = Anonymous> {
    pub(crate) inner: Arc<Inner>,
    pub(crate) access: A,
}

/// Sealed: the two states below are the only ones.
pub trait Access: sealed::Sealed + Clone + Send + Sync + 'static {}

mod sealed {
    pub trait Sealed {}
}

/// No credentials. Public APIs only.
#[derive(Clone, Debug)]
pub struct Anonymous(());

/// Holds credentials. `Clone` shares the one `Credentials` through `Arc`;
/// the secret itself is never copied.
#[derive(Clone, Debug)]
pub struct Authenticated(pub(crate) Arc<Credentials>);

impl sealed::Sealed for Anonymous {}
impl sealed::Sealed for Authenticated {}
impl Access for Anonymous {}
impl Access for Authenticated {}

pub(crate) struct Inner {
    pub(crate) endpoints: Endpoints,
    pub(crate) config: Config,
    pub(crate) connector: Connector,
    pub(crate) public_pool: Pool,
    pub(crate) private_pool: Pool,
    pub(crate) public_ws_pacer: Pacer,
    pub(crate) private_ws_pacer: Pacer,
}

impl Client<Anonymous> {
    /// # Postconditions
    /// No I/O happens here. Connections open lazily on the first call.
    ///
    /// # Preconditions
    /// None: `Endpoints` and `Config` are valid by construction.
    ///
    /// # Errors
    /// Infallible.
    pub fn new(endpoints: Endpoints, config: Config) -> Self {
        // TODO Connector::new(rustls ClientConfig with the ring provider,
        // webpki-roots, TLS 1.2 and 1.3, ALPN "http/1.1"), two empty pools
        // sized config.limits.max_idle_connections, two pacers.
        let _ = (endpoints, config);
        todo!()
    }

    /// Production endpoints, default configuration.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// Same as `Client::new(Endpoints::production(), Config::default())`. No I/O.
    ///
    /// # Errors
    /// Infallible.
    pub fn production() -> Self {
        Self::new(Endpoints::production(), Config::default())
    }

    /// Adds credentials. Consumes `self` so there is one way to reach the
    /// private API: through the returned client.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// The returned client shares this client's pools and pacers. `self` is consumed, so the credentials are reachable only through the result.
    ///
    /// # Errors
    /// Infallible.
    pub fn authenticate(self, credentials: Credentials) -> Client<Authenticated> {
        Client {
            inner: self.inner,
            access: Authenticated(Arc::new(credentials)),
        }
    }
}

/// Everything tunable, all bounded. There is no way to express "no timeout"
/// or "no limit".
#[derive(Clone, Debug)]
pub struct Config {
    pub timeouts: Timeouts,
    pub limits: Limits,
    pub reconnect: Backoff,
}

impl Default for Config {
    fn default() -> Self {
        todo!()
    }
}

/// Defaults: connect 5 s, request 10 s, WebSocket handshake 10 s, WebSocket
/// write 5 s, WebSocket idle 150 s (the server pings every 60 s, so 150 s of
/// silence means two pings are gone).
#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    pub connect: Timeout,
    /// Whole REST exchange: write, wait, read the full body.
    pub request: Timeout,
    pub ws_handshake: Timeout,
    pub ws_write: Timeout,
    pub ws_idle: Timeout,
}

/// # Invariants
/// `1 ms <= duration <= 10 min`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeout(Duration);

impl Timeout {
    pub const MAX: Duration = Duration::from_secs(600);

    /// A timeout.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `1 ms <= get() <= 10 min`.
    ///
    /// # Errors
    /// `Semantics("timeout out of range")`.
    pub fn new(d: Duration) -> Result<Self, InvalidValue> {
        if d < Duration::from_millis(1) || d > Self::MAX {
            return Err(InvalidValue {
                primitive: Primitive::Config,
                violation: Violation::Semantics("timeout out of range"),
            });
        }
        Ok(Self(d))
    }

    pub fn get(self) -> Duration {
        self.0
    }
}

/// Defaults: response body 8 MiB, WebSocket message 1 MiB, 8 idle
/// connections per REST base, 64 subscriptions per feed.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Checked against `Content-Length` or the running chunk total before
    /// the bytes are buffered.
    pub max_response_bytes: ByteLimit,
    /// Checked against the frame header's length field before the payload is
    /// read, and against the running total of a fragmented message.
    pub max_ws_message_bytes: ByteLimit,
    pub max_idle_connections: u8,
    pub max_subscriptions: u8,
}

/// # Invariants
/// `1 <= bytes <= 64 MiB`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteLimit(NonZeroU32);

impl ByteLimit {
    pub const MAX: u32 = 64 * 1024 * 1024;

    /// A byte limit.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `1 <= get() <= 64 MiB`.
    ///
    /// # Errors
    /// `Semantics("byte limit out of range")`.
    pub fn new(bytes: u32) -> Result<Self, InvalidValue> {
        match NonZeroU32::new(bytes) {
            Some(n) if bytes <= Self::MAX => Ok(Self(n)),
            _ => Err(InvalidValue {
                primitive: Primitive::Config,
                violation: Violation::Semantics("byte limit out of range"),
            }),
        }
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Reconnect delay for feeds: `initial`, doubling per failed attempt, capped
/// at `max`, reset after a connection stays up for `max`. Defaults 1 s, 60 s.
///
/// # Invariants
/// `initial <= max`.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    initial: Timeout,
    max: Timeout,
}

impl Backoff {
    /// A backoff schedule.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `delay(n)` never decreases as `n` grows and never exceeds `max`.
    ///
    /// # Errors
    /// `Semantics("initial > max")`.
    pub fn new(initial: Timeout, max: Timeout) -> Result<Self, InvalidValue> {
        if initial.get() > max.get() {
            return Err(InvalidValue {
                primitive: Primitive::Config,
                violation: Violation::Semantics("initial > max"),
            });
        }
        Ok(Self { initial, max })
    }

    /// Delay before attempt `attempt` (0-based).
    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        debug_assert!(
            self.initial.get() <= self.max.get(),
            "Backoff::new keeps initial <= max"
        );
        let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
        self.initial
            .get()
            .saturating_mul(factor)
            .min(self.max.get())
    }
}
