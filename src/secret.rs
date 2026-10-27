//! Credentials and request signing.
//!
//! The three secret-bearing values (API key, API secret, WebSocket access
//! token) share these rules, each enforced by the type rather than a comment:
//!
//! - `Debug` prints `<redacted>`. No `Display`, no `Serialize`.
//! - No `Clone`. A client shares one copy through `Arc`.
//! - The buffer is zeroed on drop with `zeroize` (already in the tree through
//!   rustls, so it costs no crate).
//! - The API secret has no accessor at all. Its one operation is signing,
//!   and that lives in this module.
//!
//! `ring::hmac::Key` holds the HMAC key schedule, which is as sensitive as
//! the secret, and ring neither zeroizes it nor makes it `!Clone`. So no
//! `hmac::Key` is stored. One is built on the stack per signature (two
//! SHA-256 blocks of work) and dropped at the end of the call.

use core::fmt;

use zeroize::Zeroizing;

use crate::domain::{InvalidValue, Primitive, check_size_and_lexical};
use crate::wire::http::Method;

/// `API-KEY`.
///
/// # Invariants
/// 1 to 128 bytes of visible ASCII (`0x21..=0x7E`). This excludes CR and LF,
/// so the key cannot inject a header line.
pub struct ApiKey(Zeroizing<String>);

impl ApiKey {
    /// Takes ownership so the caller's buffer becomes the zeroized one.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, the key lives in a zeroizing buffer. On `Err`, the input has been zeroed.
    ///
    /// # Errors
    /// `Size` or `Lexical`. The error never contains the input.
    pub fn new(key: String) -> Result<Self, InvalidValue> {
        let key = Zeroizing::new(key);
        check_size_and_lexical(Primitive::ApiKey, &key, 1, 128, |b| b.is_ascii_graphic())?;
        Ok(Self(key))
    }

    pub(crate) fn header_value(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

/// The API secret. Use-only: it can sign, and nothing else.
///
/// # Invariants
/// 1 to 256 bytes of visible ASCII. Zeroed on drop. Never readable.
pub struct ApiSecret(Zeroizing<String>);

impl ApiSecret {
    /// Takes ownership, so the caller's allocation becomes the zeroized one
    /// and this call leaves no copy behind. The value is wrapped before it is
    /// validated, so a rejected secret is zeroed too.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, the secret lives in a zeroizing buffer and can only sign. On `Err`, the input has been zeroed.
    ///
    /// # Errors
    /// `Size` or `Lexical`. The error never contains the input.
    pub fn new(secret: String) -> Result<Self, InvalidValue> {
        let secret = Zeroizing::new(secret);
        check_size_and_lexical(Primitive::ApiSecret, &secret, 1, 256, |b| {
            b.is_ascii_graphic()
        })?;
        Ok(Self(secret))
    }

    /// `hex(HMAC-SHA256(secret, message))`.
    fn sign(&self, message: &[&[u8]]) -> Signature {
        // TODO let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, self.0.as_bytes());
        // let mut ctx = ring::hmac::Context::with_key(&key);
        // for part in message { ctx.update(part) }   // no concatenated copy
        // hex-encode ctx.sign().as_ref() (32 bytes) into Signature([u8; 64]).
        let _ = message;
        todo!()
    }
}

impl fmt::Debug for ApiSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiSecret(<redacted>)")
    }
}

/// An API key and its secret.
pub struct Credentials {
    key: ApiKey,
    secret: ApiSecret,
}

impl Credentials {
    /// Pairs a key with its secret.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// Owns both values. Neither is readable through `Credentials`.
    ///
    /// # Errors
    /// Infallible.
    pub fn new(key: ApiKey, secret: ApiSecret) -> Self {
        Self { key, secret }
    }

    /// Reads `GMO_API_KEY` and `GMO_API_SECRET`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, as [`Credentials::new`].
    ///
    /// # Errors
    /// `InvalidValue` with `Size { actual: 0, .. }` when a variable is unset,
    /// otherwise as [`ApiKey::new`] / [`ApiSecret::new`].
    pub fn from_env() -> Result<Self, InvalidValue> {
        todo!()
    }

    /// Signs one private REST request.
    ///
    /// # Preconditions
    /// `body` is the exact byte sequence that will be written to the socket
    /// (empty for GET). `path` is the `/v1/...` path without `/private` and
    /// without the query string; [`SignedPath`] makes any other form
    /// unrepresentable.
    ///
    /// # Postconditions
    /// The returned headers are valid only for `timestamp_ms`; a retry signs again.
    pub(crate) fn sign_request(
        &self,
        timestamp_ms: u64,
        method: Method,
        path: SignedPath,
        body: &[u8],
    ) -> AuthHeaders<'_> {
        let timestamp = timestamp_ms.to_string();
        let sign = self.secret.sign(&[
            timestamp.as_bytes(),
            method.as_str().as_bytes(),
            path.as_str().as_bytes(),
            body,
        ]);
        AuthHeaders {
            key: &self.key,
            timestamp,
            sign,
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials(<redacted>)")
    }
}

/// The three private-API headers for one request. Written straight into the
/// request buffer; never stored, never logged.
pub(crate) struct AuthHeaders<'a> {
    pub(crate) key: &'a ApiKey,
    pub(crate) timestamp: String,
    pub(crate) sign: Signature,
}

/// 64 lowercase hex digits. Redacted: a fresh signature is replayable until
/// the exchange's timestamp window closes.
pub(crate) struct Signature([u8; 64]);

impl Signature {
    pub(crate) fn as_str(&self) -> &str {
        core::str::from_utf8(&self.0).unwrap_or_default()
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Signature(<redacted>)")
    }
}

/// The path that is both signed and appended to the base URL's prefix.
///
/// # Invariants
/// Starts with `/v1/`, has no `?`, and is `'static`. Every endpoint declares
/// its path as a constant, so a bad one fails the build in `SignedPath::new`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SignedPath(&'static str);

impl SignedPath {
    pub(crate) const fn new(path: &'static str) -> Self {
        let b = path.as_bytes();
        assert!(b.len() > 4 && b[0] == b'/' && b[1] == b'v' && b[2] == b'1' && b[3] == b'/');
        let mut i = 0;
        while i < b.len() {
            assert!(b[i] != b'?', "query string is not part of the signed path");
            i += 1;
        }
        Self(path)
    }

    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}

/// The private WebSocket access token. Lives only inside a `Feed`.
///
/// # Invariants
/// 1 to 256 bytes of URL-unreserved characters (`A-Z a-z 0-9 - . _ ~`), so
/// it can sit in a URL path segment without escaping and cannot add a path
/// segment or query. Zeroed on drop. Never in an error: errors carry
/// `&'static str` operation names, and the dial path is built in a local
/// buffer that is zeroized after the handshake request is written.
pub(crate) struct AccessToken(Zeroizing<String>);

impl AccessToken {
    pub(crate) fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::AccessToken, text, 1, 256, |b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
        })?;
        Ok(Self(Zeroizing::new(text.to_owned())))
    }

    /// Only callers: the WebSocket dial path and the `ws-auth` PUT/DELETE bodies.
    pub(crate) fn reveal(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AccessToken(<redacted>)")
    }
}
