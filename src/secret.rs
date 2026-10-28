//! 認証情報とリクエスト署名。
//!
//! 秘密を持つ 3 つの値 (API キー、API シークレット、WebSocket アクセストークン)
//! は次の規則を共有し、それぞれコメントではなく型によって強制される。
//!
//! - `Debug` は `<redacted>` を出力する。`Display` も `Serialize` もない。
//! - `Clone` しない。クライアントは `Arc` で 1 つの複製を共有する。
//! - バッファは drop 時に `zeroize` でゼロ埋めされる (rustls 経由で既に
//!   ツリーにあるので、クレートは増えない)。
//! - API シークレットにはアクセサが一切ない。唯一の操作は署名で、それは
//!   このモジュールの中にある。
//!
//! `ring::hmac::Key` は HMAC の鍵スケジュールを持ち、シークレットと同じく
//! 機微である。ring はそれをゼロ埋めせず `Clone` も禁止しないので、
//! `hmac::Key` は保存しない。署名ごとにスタック上で作り (SHA-256 ブロック
//! 2 つ分の計算)、呼び出しの終わりで捨てる。
//!
//! # 残存リスク
//! 署名された値を hyper に渡すとき、`API-KEY` と `API-SIGN` は hyper の
//! `HeaderMap` に複製され、その複製はゼロ埋めされない。詳細は
//! `transport` モジュールのドキュメントを参照。

use core::fmt;

use zeroize::Zeroizing;

use crate::domain::{InvalidValue, Primitive, Violation, check_size_and_lexical};
use crate::transport::http::Method;

/// `API-KEY`。
///
/// # 不変条件
/// 1 から 128 バイトの可視 ASCII (`0x21..=0x7E`)。CR と LF を含まないので、
/// キーがヘッダ行を注入することはできない。
pub struct ApiKey(Zeroizing<String>);

impl ApiKey {
    /// 所有権を受け取るので、呼び出し側のバッファがそのままゼロ埋め対象になる。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` ならキーはゼロ埋めされるバッファの中にある。`Err` でも入力はゼロ埋めされている。
    ///
    /// # エラー
    /// `Size` または `Lexical`。エラーは入力を決して含まない。
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

/// API シークレット。使うことしかできない: 署名はできるが、それ以外は何もできない。
///
/// # 不変条件
/// 1 から 256 バイトの可視 ASCII。drop 時にゼロ埋め。決して読み出せない。
pub struct ApiSecret(Zeroizing<String>);

impl ApiSecret {
    /// 所有権を受け取るので、呼び出し側の割り当てがそのままゼロ埋め対象になり、
    /// この呼び出しは複製を残さない。値は検証の前に包まれるので、拒否された
    /// シークレットもゼロ埋めされる。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` ならシークレットはゼロ埋めされるバッファの中にあり、署名にしか使えない。
    /// `Err` でも入力はゼロ埋めされている。
    ///
    /// # エラー
    /// `Size` または `Lexical`。エラーは入力を決して含まない。
    pub fn new(secret: String) -> Result<Self, InvalidValue> {
        let secret = Zeroizing::new(secret);
        check_size_and_lexical(Primitive::ApiSecret, &secret, 1, 256, |b| {
            b.is_ascii_graphic()
        })?;
        Ok(Self(secret))
    }

    /// `hex(HMAC-SHA256(secret, message))`。`message` の各部分は連結した
    /// 複製を作らずに順に取り込む。
    fn sign(&self, message: &[&[u8]]) -> Signature {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, self.0.as_bytes());
        let mut ctx = ring::hmac::Context::with_key(&key);
        for part in message {
            ctx.update(part);
        }
        let tag = ctx.sign();
        let mut hex = Zeroizing::new([0u8; 64]);
        crate::domain::write_hex(&mut *hex, tag.as_ref());
        Signature(hex)
    }
}

impl fmt::Debug for ApiSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiSecret(<redacted>)")
    }
}

/// API キーとそのシークレットの組。
pub struct Credentials {
    key: ApiKey,
    secret: ApiSecret,
}

impl Credentials {
    /// キーとシークレットを組にする。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 両方の値を所有する。どちらも `Credentials` を通して読み出せない。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn new(key: ApiKey, secret: ApiSecret) -> Self {
        Self { key, secret }
    }

    /// 環境変数 `GMO_API_KEY` と `GMO_API_SECRET` を読む。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら [`Credentials::new`] と同じ。
    ///
    /// # エラー
    /// 変数が未設定のとき `Size { actual: 0, .. }` の `InvalidValue`。
    /// それ以外は [`ApiKey::new`] / [`ApiSecret::new`] と同じ。
    pub fn from_env() -> Result<Self, InvalidValue> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// `from_env` の純粋な本体。`lookup` が変数名から値を引く。
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, InvalidValue> {
        let read = |name: &str, primitive: Primitive, max: usize| {
            lookup(name).map(Zeroizing::new).ok_or(InvalidValue {
                primitive,
                violation: Violation::Size {
                    min: 1,
                    max,
                    actual: 0,
                },
            })
        };
        let key = read("GMO_API_KEY", Primitive::ApiKey, 128)?;
        let secret = read("GMO_API_SECRET", Primitive::ApiSecret, 256)?;
        Ok(Self::new(
            ApiKey::new(key.to_string())?,
            ApiSecret::new(secret.to_string())?,
        ))
    }

    /// Private REST リクエストを 1 つ署名する。
    ///
    /// # 事前条件
    /// `body` はソケットに書き込まれるバイト列そのもの (GET なら空)。`path` は
    /// `/private` を含まず、クエリ文字列も含まない `/v1/...` のパス。
    /// [`SignedPath`] が他の形を表現できなくしている。
    ///
    /// # 事後条件
    /// 返されたヘッダは `timestamp_ms` に対してだけ有効。再試行は再署名する。
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

/// 1 リクエスト分の Private API ヘッダ 3 つ。そのままリクエストに書き込まれ、
/// 保存もログ出力もされない。
pub(crate) struct AuthHeaders<'a> {
    pub(crate) key: &'a ApiKey,
    pub(crate) timestamp: String,
    pub(crate) sign: Signature,
}

/// 小文字 16 進数 64 桁。秘匿対象: 新しい署名は取引所のタイムスタンプ窓が
/// 閉じるまで再生可能である。drop 時にゼロ埋めされる。
pub(crate) struct Signature(Zeroizing<[u8; 64]>);

impl Signature {
    pub(crate) fn as_str(&self) -> &str {
        core::str::from_utf8(&*self.0).unwrap_or_default()
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Signature(<redacted>)")
    }
}

/// 署名され、かつベース URL のプレフィックスに連結されるパス。
///
/// # 不変条件
/// `/v1/` で始まり、`?` を含まず、`'static` である。各エンドポイントは
/// パスを定数として宣言するので、不正なものは `SignedPath::new` でビルドが失敗する。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SignedPath(&'static str);

impl SignedPath {
    pub(crate) const fn new(path: &'static str) -> Self {
        let b = path.as_bytes();
        assert!(matches!(b, [b'/', b'v', b'1', b'/', _, ..]));
        let mut remaining = b;
        while let [byte, rest @ ..] = remaining {
            assert!(*byte != b'?', "query string is not part of the signed path");
            remaining = rest;
        }
        Self(path)
    }

    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Private WebSocket のアクセストークン。`Feed` の中にだけ存在する。
///
/// # 不変条件
/// 1 から 256 バイトの URL 非予約文字 (`A-Z a-z 0-9 - . _ ~`)。したがって
/// エスケープなしで URL のパスセグメントに置け、セグメントやクエリを追加
/// できない。drop 時にゼロ埋め。エラーには決して入らない: エラーが持つのは
/// `&'static str` の操作名だけで、接続先パスは接続時にローカルのバッファで
/// 組み立てられる。
pub(crate) struct AccessToken(Zeroizing<String>);

impl AccessToken {
    pub(crate) fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::AccessToken, text, 1, 256, |b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
        })?;
        Ok(Self(Zeroizing::new(text.to_owned())))
    }

    /// 呼び出し元は WebSocket の接続先パスと `ws-auth` の PUT/DELETE ボディだけ。
    pub(crate) fn reveal(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AccessToken(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Vectors {
        secret: String,
        cases: Vec<Case>,
    }

    #[derive(serde::Deserialize)]
    struct Case {
        timestamp: String,
        method: String,
        path: String,
        body: String,
        sign: String,
    }

    #[test]
    fn signatures_match_the_reference_vectors() {
        let vectors: Vectors =
            serde_json::from_str(include_str!("../tests/fixtures/signing_vectors.json")).unwrap();
        let credentials = Credentials::new(
            ApiKey::new("k".to_owned()).unwrap(),
            ApiSecret::new(vectors.secret).unwrap(),
        );
        assert_eq!(vectors.cases.len(), 6);
        for case in vectors.cases {
            let method = match case.method.as_str() {
                "GET" => Method::Get,
                "POST" => Method::Post,
                "PUT" => Method::Put,
                "DELETE" => Method::Delete,
                other => panic!("unknown method {other}"),
            };
            let path = match case.path.as_str() {
                "/v1/account/margin" => SignedPath::new("/v1/account/margin"),
                "/v1/orders" => SignedPath::new("/v1/orders"),
                "/v1/order" => SignedPath::new("/v1/order"),
                "/v1/ws-auth" => SignedPath::new("/v1/ws-auth"),
                other => panic!("unknown path {other}"),
            };
            let headers = credentials.sign_request(
                case.timestamp.parse().unwrap(),
                method,
                path,
                case.body.as_bytes(),
            );
            assert_eq!(
                headers.sign.as_str(),
                case.sign,
                "{} {} {:?}",
                case.method,
                case.path,
                case.body
            );
            assert_eq!(headers.timestamp, case.timestamp);
            assert_eq!(headers.key.header_value(), "k");
        }
    }

    #[test]
    fn debug_output_reveals_no_secret() {
        let key = ApiKey::new("THEKEY123".to_owned()).unwrap();
        let secret = ApiSecret::new("THESECRET456".to_owned()).unwrap();
        assert!(!format!("{key:?}").contains("THEKEY123"));
        assert!(!format!("{secret:?}").contains("THESECRET456"));
        let credentials = Credentials::new(key, secret);
        let printed = format!("{credentials:?}");
        assert!(!printed.contains("THEKEY123") && !printed.contains("THESECRET456"));
        let headers = credentials.sign_request(1, Method::Get, SignedPath::new("/v1/x"), b"");
        assert!(!format!("{:?}", headers.sign).contains(headers.sign.as_str()));
        let token = AccessToken::parse("tok-en.123_~").unwrap();
        assert!(!format!("{token:?}").contains("tok-en"));
        assert_eq!(token.reveal(), "tok-en.123_~");
    }

    #[test]
    fn key_and_secret_validation() {
        assert!(matches!(
            ApiKey::new(String::new()).unwrap_err().violation,
            Violation::Size { .. }
        ));
        assert_eq!(
            ApiKey::new("ab c".to_owned()).unwrap_err().violation,
            Violation::Lexical { offset: 2 }
        );
        assert_eq!(
            ApiSecret::new("x\r\n".to_owned()).unwrap_err().violation,
            Violation::Lexical { offset: 1 }
        );
        assert!(matches!(
            ApiSecret::new("s".repeat(257)).unwrap_err().violation,
            Violation::Size { .. }
        ));
        assert!(matches!(
            AccessToken::parse("a/b").unwrap_err().violation,
            Violation::Lexical { .. }
        ));
        assert!(matches!(
            AccessToken::parse("").unwrap_err().violation,
            Violation::Size { .. }
        ));
    }

    #[test]
    fn from_env_reports_an_unset_variable_as_size_zero() {
        let err = Credentials::from_lookup(|_| None).unwrap_err();
        assert_eq!(err.primitive, Primitive::ApiKey);
        assert_eq!(
            err.violation,
            Violation::Size {
                min: 1,
                max: 128,
                actual: 0
            }
        );
        let only_key = |name: &str| (name == "GMO_API_KEY").then(|| "k".to_owned());
        let err = Credentials::from_lookup(only_key).unwrap_err();
        assert_eq!(err.primitive, Primitive::ApiSecret);
        let both = |name: &str| {
            Some(if name == "GMO_API_KEY" {
                "key".to_owned()
            } else {
                "secret".to_owned()
            })
        };
        let credentials = Credentials::from_lookup(both).unwrap();
        assert_eq!(credentials.key.header_value(), "key");
        let bad = |name: &str| {
            Some(if name == "GMO_API_KEY" {
                "key".to_owned()
            } else {
                "has space".to_owned()
            })
        };
        assert_eq!(
            Credentials::from_lookup(bad).unwrap_err().violation,
            Violation::Lexical { offset: 3 }
        );
    }
}
