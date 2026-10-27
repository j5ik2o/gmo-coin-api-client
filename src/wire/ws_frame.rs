//! RFC 6455 WebSocket framing, client side. Sans-IO.
//!
//! Rules enforced by the decoder (server to client):
//! - Server frames must not be masked; a masked frame is a protocol error.
//! - RSV1..3 must be zero (no extension is negotiated).
//! - Opcodes: continuation, text, binary, close, ping, pong. Others are errors.
//! - Control frames: payload at most 125 bytes, never fragmented, and may
//!   arrive between the fragments of a data message.
//! - The 7/16/64-bit length field is checked against the message limit
//!   before the payload is buffered; a fragmented message's running total is
//!   checked per fragment.
//! - A text message must be valid UTF-8 once complete.
//!
//! Rules applied by the encoder (client to server):
//! - Every frame is masked with a fresh 4-byte key from `ring::rand`.
//! - Pongs echo the ping payload.
//! - Close sends status 1000 and waits (bounded by `ws_write`) for the echo.

use crate::error::TransportErrorKind;

/// A complete message or control frame from the server.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Incoming {
    Text(String),
    /// The exchange documents only text messages; binary is surfaced so the
    /// session can treat it as a protocol violation.
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close { code: Option<u16> },
}

/// Incremental frame decoder.
///
/// # Invariants
/// `partial.len() <= max_message` at all times.
pub(crate) struct FrameDecoder {
    max_message: u32,
    partial: Option<(Opcode, Vec<u8>)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Opcode {
    Text,
    Binary,
}

impl FrameDecoder {
    pub(crate) fn new(max_message: u32) -> Self {
        Self { max_message, partial: None }
    }

    /// Decodes at most one `Incoming` from the front of `buf`, draining the
    /// bytes it used. `Ok(None)` means more bytes are needed.
    ///
    /// # Errors
    /// `TooLarge` (declared length over the limit) or `Framing(..)` for any
    /// rule in the module docs.
    pub(crate) fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Incoming>, TransportErrorKind> {
        // TODO header: b0 = FIN|RSV|opcode, b1 = MASK|len7; len7 126 => u16, 127 => u64 (MSB 0).
        // MASK set => Framing("masked server frame"). RSV != 0 => Framing("rsv").
        // control opcode && (!FIN || len > 125) => Framing("control frame").
        // data: running = partial.len() + len; running > max_message => TooLarge.
        // need header + len bytes in buf, else Ok(None) without consuming.
        // text FIN => String::from_utf8(payload) or Framing("utf-8").
        let _ = (buf, self.max_message, &self.partial);
        todo!()
    }
}

/// Appends one masked client frame (FIN set) to `out`.
pub(crate) fn encode_text(payload: &str, mask: [u8; 4], out: &mut Vec<u8>) {
    let _ = (payload, mask, out);
    todo!()
}

pub(crate) fn encode_pong(payload: &[u8], mask: [u8; 4], out: &mut Vec<u8>) {
    let _ = (payload, mask, out);
    todo!()
}

pub(crate) fn encode_close(code: u16, mask: [u8; 4], out: &mut Vec<u8>) {
    let _ = (code, mask, out);
    todo!()
}

/// A fresh masking key. ring's `SystemRandom`, which rustls already uses.
pub(crate) fn mask_key() -> [u8; 4] {
    todo!()
}

/// The upgrade request. `path` may hold the access token; the caller builds
/// it in a `Zeroizing` buffer and passes it only here.
///
/// `Sec-WebSocket-Key` is 16 random bytes, base64 encoded.
pub(crate) fn handshake_request(host: &str, path: &str, key_b64: &str, out: &mut Vec<u8>) {
    let _ = (host, path, key_b64, out);
    todo!()
}

/// `base64(SHA-1(key_b64 + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))`, the
/// value the server must echo in `Sec-WebSocket-Accept`. SHA-1 comes from
/// `ring::digest::SHA1_FOR_LEGACY_USE_ONLY`; no hash is hand-rolled.
pub(crate) fn expected_accept(key_b64: &str) -> String {
    let _ = key_b64;
    todo!()
}

/// Parses the handshake response head with httparse.
///
/// # Errors
/// `Framing` unless the status is 101, `Upgrade: websocket`,
/// `Connection: Upgrade`, and `Sec-WebSocket-Accept` equals `expected`.
/// A 503 is reported separately so the session can treat it as maintenance.
pub(crate) fn check_handshake(head: &[u8], expected: &str) -> Result<HandshakeOutcome, TransportErrorKind> {
    let _ = (head, expected);
    todo!()
}

pub(crate) enum HandshakeOutcome {
    /// Upgraded. Bytes after the head belong to the first frame.
    Upgraded { head_len: usize },
    /// HTTP 503: the documented maintenance answer for the WebSocket APIs.
    Maintenance,
    /// Any other status, for example 401/404 for a dead private token.
    Rejected { http_status: u16 },
}
