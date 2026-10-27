//! Signature verification, as the exchange does it.

use ring::hmac;

use super::{SignatureCheck, TEST_SECRET};

/// Recomputes `HMAC-SHA256(secret, timestamp + METHOD + path + body)` and
/// compares in constant time (`ring::hmac::verify`).
///
/// `request_path` is the path as received (`/private/v1/order`); the signed
/// path drops the `/private` prefix and any query. `body` is the raw bytes
/// received, not a re-serialization, so a client that signs one byte string
/// and sends another fails here.
pub fn check(
    timestamp: Option<&str>,
    sign_hex: Option<&str>,
    method: &str,
    request_path: &str,
    body: &[u8],
) -> SignatureCheck {
    let (Some(timestamp), Some(sign_hex)) = (timestamp, sign_hex) else {
        return SignatureCheck::Absent;
    };
    let signed_path = request_path
        .strip_prefix("/private")
        .unwrap_or(request_path);
    let signed_path = signed_path.split('?').next().unwrap_or(signed_path);
    let Some(tag) = decode_hex(sign_hex) else {
        return SignatureCheck::Invalid;
    };
    let mut message =
        Vec::with_capacity(timestamp.len() + method.len() + signed_path.len() + body.len());
    message.extend_from_slice(timestamp.as_bytes());
    message.extend_from_slice(method.as_bytes());
    message.extend_from_slice(signed_path.as_bytes());
    message.extend_from_slice(body);
    let key = hmac::Key::new(hmac::HMAC_SHA256, TEST_SECRET.as_bytes());
    match hmac::verify(&key, &message, &tag) {
        Ok(()) => SignatureCheck::Valid,
        Err(_) => SignatureCheck::Invalid,
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if text.len() != 64 {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}
