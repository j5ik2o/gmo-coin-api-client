//! Bytes on the network. Private to the crate.
//!
//! The protocol framing is owned here and written sans-IO: `http` and
//! `ws_frame` are pure encoders and incremental decoders over byte slices,
//! testable without a socket. The I/O shell is this module's [`Stream`] plus
//! the thin loops in `http::round_trip` and `ws::session`.
//!
//! Vetted crates do the parts where a home-grown version is a security risk:
//! TLS (rustls), certificate roots (webpki-roots), HMAC, SHA-1, and
//! randomness (ring), HTTP head parsing (httparse). What is left here is
//! request serialization, body framing (`Content-Length` and chunked), and
//! RFC 6455 framing, each bounded by `Limits` before any buffer grows.

pub(crate) mod base64;
pub(crate) mod http;
pub(crate) mod ws_frame;

use std::io;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::client::Timeouts;
use crate::endpoint::{Base, Transport};
use crate::error::{Phase, TransportErrorKind};

/// Opens connections. Holds the one rustls configuration for the process.
pub(crate) struct Connector {
    tls: TlsConnector,
}

impl Connector {
    pub(crate) fn new(tls_config: Arc<tokio_rustls::rustls::ClientConfig>) -> Self {
        Self {
            tls: TlsConnector::from(tls_config),
        }
    }

    /// TCP connect plus, for `Transport::Tls`, the TLS handshake, both inside
    /// `timeouts.connect`.
    ///
    /// # Postconditions
    /// A `Stream::Plain` is returned only for `Transport::Plain`, which only
    /// exists for loopback addresses. No DNS lookup happens for plain.
    pub(crate) async fn connect(
        &self,
        base: &Base,
        timeouts: &Timeouts,
    ) -> Result<Stream, TransportErrorKind> {
        // TODO tokio::time::timeout(timeouts.connect.get(), async {
        //   match &base.transport {
        //     Transport::Plain(ip) => TcpStream::connect((ip.ip(), base.port)) => Stream::Plain,
        //     Transport::Tls(name) => TcpStream::connect((host_of(name), base.port)),
        //        set_nodelay(true), self.tls.connect(name.clone(), tcp) => Stream::Tls,
        //   }
        // }) ; elapsed => TransportErrorKind::Timeout(Phase::Connect)
        let _ = (&self.tls, base, timeouts, Phase::Connect);
        let _: Option<&Transport> = None;
        todo!()
    }
}

/// One connection. Owned by exactly one request or one feed at a time.
pub(crate) enum Stream {
    Tls(Box<TlsStream<TcpStream>>),
    Plain(TcpStream),
}

impl Stream {
    /// Cancel-safe: on cancellation no bytes have been consumed from the
    /// socket into a lost buffer, because `buf` is the caller's.
    pub(crate) async fn read_some(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let _ = buf;
        todo!()
    }

    /// Writes from `buf[*written..]`, advancing `written` as bytes go out, so
    /// a cancelled call resumes where it stopped instead of re-sending or
    /// dropping part of a frame.
    pub(crate) async fn write_from(&mut self, buf: &[u8], written: &mut usize) -> io::Result<()> {
        let _ = (buf, written);
        todo!()
    }
}
