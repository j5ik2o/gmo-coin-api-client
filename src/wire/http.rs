//! HTTP/1.1, client side, one request per connection at a time.
//!
//! Scope is exactly what the exchange needs: `GET`, `POST`, `PUT`, `DELETE`
//! with a JSON body, identity encoding (no `Accept-Encoding` is sent), and
//! responses framed by `Content-Length`, `Transfer-Encoding: chunked`, or
//! connection close. No pipelining, no `Expect: 100-continue`, no HTTP/2.

use std::sync::Mutex;
use std::time::Instant;

use zeroize::Zeroizing;

use crate::client::{Config, Limits};
use crate::endpoint::Base;
use crate::error::{ApiGroup, Delivery, TransportError, TransportErrorKind};
use crate::secret::AuthHeaders;
use crate::wire::{Connector, Stream};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl Method {
    /// Upper case, as signed (`timestamp + METHOD + path + body`).
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
        }
    }

    /// Safe to resend after a stale pooled connection fails. Only GET:
    /// PUT and DELETE are idempotent in HTTP but the cost of guessing wrong
    /// on a money API is not worth the saved round trip.
    pub(crate) const fn retry_on_stale_connection(self) -> bool {
        matches!(self, Method::Get)
    }
}

/// A request ready to serialize.
pub(crate) struct Request<'a> {
    pub(crate) method: Method,
    /// `prefix + signed path + ?query`. Built only from validated primitives,
    /// so it contains no byte that needs escaping and no CR/LF.
    pub(crate) target: &'a str,
    pub(crate) auth: Option<AuthHeaders<'a>>,
    /// The exact bytes that were signed. Empty for GET.
    pub(crate) body: &'a [u8],
}

/// Serializes the request head and body into `out`.
///
/// Headers, in order: `Host`, `User-Agent`, `Accept: application/json`,
/// `Content-Type: application/json` and `Content-Length` (when the method
/// carries a body; DELETE ws-auth does), then `API-KEY`, `API-TIMESTAMP`,
/// `API-SIGN` when `auth` is present.
///
/// # Postconditions
/// `out` ends with `body` byte for byte, so the signed bytes are the sent
/// bytes. `out` is `Zeroizing` because it holds `API-KEY` and `API-SIGN`.
pub(crate) fn encode_request(host: &str, req: &Request<'_>, out: &mut Zeroizing<Vec<u8>>) {
    let _ = (host, req, out);
    todo!()
}

/// A fully read response.
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
    /// False when the server sent `Connection: close` or framed the body by EOF.
    pub(crate) reusable: bool,
}

/// Incremental response decoder.
///
/// # Invariants
/// Never buffers more than `MAX_HEAD` bytes of head or
/// `limits.max_response_bytes` of body. Limits are checked against the
/// declared length (`Content-Length`, each chunk-size line) before reading
/// the bytes it announces.
pub(crate) struct ResponseDecoder {
    max_body: u32,
    state: DecodeState,
}

enum DecodeState {
    Head { buf: Vec<u8> },
    Fixed { status: u16, reusable: bool, remaining: usize, body: Vec<u8> },
    Chunked { status: u16, reusable: bool, body: Vec<u8>, chunk: ChunkState },
    UntilClose { status: u16, body: Vec<u8> },
}

enum ChunkState {
    SizeLine,
    Data { remaining: usize },
    DataCrlf,
    Trailers,
}

pub(crate) enum Progress {
    /// Consumed everything given; feed more.
    NeedMore,
    /// Complete. `leftover` bytes after the response are a protocol error on
    /// a non-pipelined connection and make it non-reusable.
    Done { response: Response, leftover: usize },
}

impl ResponseDecoder {
    pub(crate) const MAX_HEAD: usize = 16 * 1024;
    pub(crate) const MAX_HEADERS: usize = 64;

    pub(crate) fn new(limits: &Limits) -> Self {
        Self { max_body: limits.max_response_bytes.get(), state: DecodeState::Head { buf: Vec::new() } }
    }

    /// Feeds bytes read from the socket.
    ///
    /// # Errors
    /// - `TooLarge` when head or body would exceed its bound.
    /// - `Framing` for: a status line that is not `HTTP/1.1 <3 digits>`, both
    ///   `Content-Length` and `Transfer-Encoding`, a `Transfer-Encoding` other
    ///   than exactly `chunked`, repeated or non-numeric `Content-Length`, a
    ///   malformed chunk-size line, a missing CRLF after chunk data, a 1xx
    ///   status (none is expected because none is requested).
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Progress, TransportErrorKind> {
        // TODO Head: append up to MAX_HEAD; httparse::Response::parse with
        // [EMPTY_HEADER; MAX_HEADERS]; on Complete(n) pick framing:
        //   HEAD-less statuses 204/304 => empty body
        //   Transfer-Encoding: chunked => Chunked (httparse::parse_chunk_size
        //     for each size line; size > max_body - body.len() => TooLarge)
        //   Content-Length: n          => n > max_body => TooLarge, else Fixed
        //   neither                    => UntilClose (reusable = false)
        //   reusable = !(Connection: close)
        // then re-feed bytes[n..] into the body state.
        // TODO EOF handling lives in finish(): UntilClose => Done; other states => ClosedEarly.
        let _ = (bytes, &self.max_body, &self.state);
        let _ = (ChunkState::SizeLine, ChunkState::Data { remaining: 0 }, ChunkState::DataCrlf, ChunkState::Trailers);
        todo!()
    }

    /// The peer closed the connection.
    pub(crate) fn finish(self) -> Result<Response, TransportErrorKind> {
        todo!()
    }
}

/// Idle connections for one REST base.
///
/// Ownership, not sharing: a request takes a connection out, owns it
/// exclusively for the whole exchange, and puts it back only if the response
/// was fully read and `reusable`. No connection is ever used by two requests
/// at once. The `Mutex` guards only the idle stack's push and pop; it is a
/// `std` mutex that is never held across an `.await`.
///
/// # Invariants
/// `idle.len() <= max_idle`. Entries older than `IDLE_TTL` are discarded on
/// checkout, never reused.
pub(crate) struct Pool {
    idle: Mutex<Vec<(Stream, Instant)>>,
    max_idle: u8,
}

impl Pool {
    /// Below common load-balancer idle timeouts, so a pooled connection is
    /// rarely already closed by the server when it is reused.
    pub(crate) const IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(15);

    pub(crate) fn new(max_idle: u8) -> Self {
        Self { idle: Mutex::new(Vec::new()), max_idle }
    }

    fn checkout(&self) -> Option<Stream> {
        todo!()
    }

    fn checkin(&self, stream: Stream) {
        let _ = (stream, self.max_idle, &self.idle);
        todo!()
    }
}

/// Which pool and base a request uses, and how errors name it.
pub(crate) struct Route<'a> {
    pub(crate) api: ApiGroup,
    pub(crate) operation: &'static str,
    pub(crate) method: Method,
    pub(crate) base: &'a Base,
    pub(crate) pool: &'a Pool,
}

/// One request and its full response, within `config.timeouts.request`.
///
/// # Postconditions
/// - `Err` with `Delivery::NotSent` only when the last request byte was never
///   handed to the socket. Everything else is `MaybeSent`.
/// - A GET that fails on a reused connection before any response byte
///   arrived is retried once on a fresh connection, re-signed by the caller's
///   `sign` closure (the timestamp is part of the signature). Other methods
///   are never retried here: that is what `Error::outcome_unknown` is for.
pub(crate) async fn round_trip(
    connector: &Connector,
    route: &Route<'_>,
    config: &Config,
    encode: &mut (dyn FnMut(&mut Zeroizing<Vec<u8>>) + Send),
) -> Result<Response, TransportError> {
    // TODO
    // let reused = route.pool.checkout();
    // let (mut stream, was_reused) = match reused { Some(s) => (s, true), None => (connector.connect(..)?, false) };
    // let mut bytes = Zeroizing::new(Vec::new()); encode(&mut bytes);
    // timeout(timeouts.request, async {
    //     write_from(&bytes, &mut written)          // written == bytes.len() flips NotSent -> MaybeSent
    //     loop { n = read_some(buf); if n == 0 { decoder.finish() } else { decoder.push(&buf[..n]) } }
    // })
    // on error before any response byte && was_reused && route.method.retry_on_stale_connection():
    //     retry once with a fresh connection and a fresh encode()
    // on Done { response, leftover: 0 } && response.reusable => route.pool.checkin(stream)
    let _ = (connector, route, config, encode, Delivery::NotSent, Stream::read_some);
    todo!()
}
