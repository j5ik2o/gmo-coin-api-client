//! 取引所と同じ方法での認証検査。

use ring::hmac;

use super::{SignatureCheck, TEST_KEY, TEST_SECRET, TIMESTAMP_WINDOW};

/// 1 リクエスト分の認証ヘッダ。
#[derive(Clone, Copy, Default)]
pub struct AuthHeaders<'a> {
    pub key: Option<&'a str>,
    pub timestamp: Option<&'a str>,
    pub sign: Option<&'a str>,
}

impl AuthHeaders<'_> {
    pub fn is_absent(&self) -> bool {
        self.key.is_none() && self.timestamp.is_none() && self.sign.is_none()
    }
}

/// 取引所の順序で検査し、失敗したエラーコードを返す。
///
/// 1. `API-KEY` がない: ERR-5011。モックのキーと違う: ERR-5012。
/// 2. `API-TIMESTAMP` がない、または数値でない: ERR-5007。
/// 3. 現在時刻より `TIMESTAMP_WINDOW` 以上遅い: ERR-5008。早い: ERR-5009。
/// 4. `API-SIGN` が `hex(HMAC-SHA256(secret, timestamp + METHOD + path + body))`
///    と一致しない (`ring::hmac::verify` による定数時間比較): ERR-5010。
///
/// `request_path` は受信したままのパス (`/private/v1/order`)。署名対象のパスは
/// `/private` プレフィックスとクエリを落としたもの。`body` は受信した生の
/// バイト列であり、再シリアライズではない。したがって、あるバイト列に署名して
/// 別のバイト列を送るクライアントはここで失敗する。
pub fn check(
    now_ms: u64,
    headers: AuthHeaders<'_>,
    method: &str,
    request_path: &str,
    body: &[u8],
) -> Result<(), &'static str> {
    let key = headers.key.ok_or("ERR-5011")?;
    if key != TEST_KEY {
        return Err("ERR-5012");
    }
    let timestamp = headers.timestamp.ok_or("ERR-5007")?;
    let ts: u64 = timestamp.parse().map_err(|_| "ERR-5007")?;
    let window = u64::try_from(TIMESTAMP_WINDOW.as_millis()).expect("fits");
    if ts > now_ms + window {
        return Err("ERR-5008");
    }
    if ts + window < now_ms {
        return Err("ERR-5009");
    }
    let sign_hex = headers.sign.ok_or("ERR-5010")?;
    let tag = decode_hex(sign_hex).ok_or("ERR-5010")?;
    let signed_path = request_path
        .strip_prefix("/private")
        .unwrap_or(request_path);
    let signed_path = signed_path.split('?').next().unwrap_or(signed_path);
    let mut message =
        Vec::with_capacity(timestamp.len() + method.len() + signed_path.len() + body.len());
    message.extend_from_slice(timestamp.as_bytes());
    message.extend_from_slice(method.as_bytes());
    message.extend_from_slice(signed_path.as_bytes());
    message.extend_from_slice(body);
    let key = hmac::Key::new(hmac::HMAC_SHA256, TEST_SECRET.as_bytes());
    hmac::verify(&key, &message, &tag).map_err(|_| "ERR-5010")
}

/// `check` の結果を記録用の 3 値にまとめる。
pub fn classify(headers: AuthHeaders<'_>, outcome: &Result<(), &'static str>) -> SignatureCheck {
    if headers.is_absent() {
        SignatureCheck::Absent
    } else if outcome.is_ok() {
        SignatureCheck::Valid
    } else {
        SignatureCheck::Invalid
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

/// 現在時刻のミリ秒 Unix 時刻。
pub fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("after 1970")
            .as_millis(),
    )
    .expect("fits")
}
