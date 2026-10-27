//! Where the four APIs live, and how they may be reached.
//!
//! The transport rule is a type, not a comment: a [`Transport::Plain`] can
//! only be built from a [`LoopbackIp`], and `LoopbackIp` can only be built
//! from an address for which `IpAddr::is_loopback()` holds. The hostname
//! `localhost` is rejected for plain schemes, because what it resolves to is
//! the resolver's decision, and checking it would put a DNS lookup inside a
//! constructor. The mock server binds `127.0.0.1`.

use core::fmt;
use std::net::{IpAddr, SocketAddr};

use tokio_rustls::rustls::pki_types::ServerName;

use crate::domain::{InvalidValue, Primitive, Violation, check_size_and_lexical};

/// Base URLs for all four APIs. Each must be overridable so tests can point
/// the client at the mock exchange.
#[derive(Clone, Debug)]
pub struct Endpoints {
    pub public_rest: RestBase,
    pub private_rest: RestBase,
    pub public_ws: WsBase,
    pub private_ws: WsBase,
}

impl Endpoints {
    /// `https://api.coin.z.com/public`, `https://api.coin.z.com/private`,
    /// `wss://api.coin.z.com/ws/public`, `wss://api.coin.z.com/ws/private`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// All four bases use TLS.
    ///
    /// # Errors
    /// Infallible.
    pub fn production() -> Self {
        todo!()
    }

    /// The mock-server layout: one loopback port, four path prefixes
    /// (`/public`, `/private`, `/ws/public`, `/ws/private`), plain transport.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, all four bases are plain transport to `addr`.
    ///
    /// # Errors
    /// `Semantics("not loopback")` when `addr.ip()` is not a loopback address.
    pub fn loopback(addr: SocketAddr) -> Result<Self, InvalidValue> {
        let _ = LoopbackIp::new(addr.ip())?;
        todo!()
    }
}

/// A REST base URL: `https://host[:port][/prefix]`, or
/// `http://<loopback-ip>[:port][/prefix]`.
#[derive(Clone)]
pub struct RestBase(Base);

/// A WebSocket base URL: `wss://host[:port][/prefix]`, or
/// `ws://<loopback-ip>[:port][/prefix]`. The private feed appends
/// `/v1/{token}` at dial time; the token is never stored here.
#[derive(Clone)]
pub struct WsBase(Base);

impl RestBase {
    /// Parses a REST base URL.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, a plain `http` base always names a loopback IP literal.
    /// # Errors
    /// Validation runs size (at most 2048 bytes), lexical (visible ASCII),
    /// syntax (`scheme://authority[/prefix]`, no query, no fragment, no
    /// userinfo, no `..`), then semantics (scheme is `https`, or `http` with
    /// a loopback IP literal).
    pub fn parse(url: &str) -> Result<Self, InvalidValue> {
        Base::parse(url, "https", "http").map(Self)
    }
}

impl WsBase {
    /// Parses a WebSocket base URL.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, a plain `ws` base always names a loopback IP literal.
    /// # Errors
    /// As [`RestBase::parse`], with schemes `wss` and `ws`.
    pub fn parse(url: &str) -> Result<Self, InvalidValue> {
        Base::parse(url, "wss", "ws").map(Self)
    }
}

impl RestBase {
    pub(crate) fn base(&self) -> &Base {
        &self.0
    }
}

impl WsBase {
    pub(crate) fn base(&self) -> &Base {
        &self.0
    }
}

/// # Invariants
/// `prefix` is empty or `/seg(/seg)*` with segments of `A-Z a-z 0-9 - _`;
/// it never ends in `/`. Request paths are `prefix + SignedPath`.
#[derive(Clone)]
pub(crate) struct Base {
    pub(crate) transport: Transport,
    pub(crate) port: u16,
    pub(crate) prefix: Box<str>,
}

impl Base {
    fn parse(url: &str, tls_scheme: &str, plain_scheme: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::BaseUrl, url, 1, 2048, |b| b.is_ascii_graphic())?;
        // TODO syntax: split "scheme://", authority up to the first '/',
        // optional ":port" (1..=65535), remainder is the prefix (checked against
        // the segment grammar). Reject '?', '#', '@', "..", "%".
        // TODO semantics:
        //   scheme == tls_scheme   => Transport::Tls(ServerName::try_from(host)?)
        //   scheme == plain_scheme => Transport::Plain(LoopbackIp::new(host.parse::<IpAddr>()?)?)
        //   anything else          => Semantics("scheme")
        let _ = (tls_scheme, plain_scheme);
        todo!()
    }

    /// `Host:` header value.
    pub(crate) fn host_header(&self) -> String {
        todo!()
    }
}

/// How a connection is made. The only plain variant requires a loopback IP.
#[derive(Clone)]
pub(crate) enum Transport {
    /// TLS 1.2+ via rustls, server name verified against the webpki roots.
    Tls(ServerName<'static>),
    /// Plain TCP, loopback only. Exists for the mock exchange.
    Plain(LoopbackIp),
}

/// # Invariants
/// `ip.is_loopback()` (127.0.0.0/8 or ::1). Private constructor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LoopbackIp(IpAddr);

impl LoopbackIp {
    pub(crate) fn new(ip: IpAddr) -> Result<Self, InvalidValue> {
        if ip.is_loopback() {
            Ok(Self(ip))
        } else {
            Err(InvalidValue {
                primitive: Primitive::BaseUrl,
                violation: Violation::Semantics("not loopback"),
            })
        }
    }

    pub(crate) fn ip(self) -> IpAddr {
        self.0
    }
}

impl fmt::Debug for RestBase {
    /// Base URLs hold no secrets, so they print in full.
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl fmt::Debug for WsBase {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
