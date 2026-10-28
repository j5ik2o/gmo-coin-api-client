//! 4 つの API の所在と、そこへ到達する方法。
//!
//! トランスポートの規則はコメントではなく型である。[`Transport::Plain`] は
//! [`LoopbackIp`] からしか作れず、`LoopbackIp` は `IpAddr::is_loopback()` が
//! 成り立つアドレスからしか作れない。ホスト名 `localhost` は平文スキームでは
//! 拒否する。何に解決されるかはリゾルバの判断であり、それを検査すると
//! コンストラクタの中に DNS 問い合わせが入ってしまうからである。モックサーバは
//! `127.0.0.1` に bind する。

use core::fmt;
use std::net::{IpAddr, SocketAddr};

use tokio_rustls::rustls::pki_types::ServerName;

use crate::domain::{InvalidValue, Primitive, Violation, check_size_and_lexical};

/// 4 つの API のベース URL。テストがクライアントをモック取引所へ向けられる
/// よう、それぞれ差し替え可能である。
#[derive(Clone, Debug)]
pub struct Endpoints {
    pub public_rest: RestBase,
    pub private_rest: RestBase,
    pub public_ws: WsBase,
    pub private_ws: WsBase,
}

impl Endpoints {
    /// `https://api.coin.z.com/public`、`https://api.coin.z.com/private`、
    /// `wss://api.coin.z.com/ws/public`、`wss://api.coin.z.com/ws/private`。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 4 つのベースすべてが TLS を使う。
    ///
    /// # エラー
    /// 失敗しない。
    #[expect(
        clippy::expect_used,
        reason = "本番 URL は定数で、整形式であることを単体テストで確かめている"
    )]
    pub fn production() -> Self {
        let parse = |url, scheme_tls, scheme_plain| {
            Base::parse(url, scheme_tls, scheme_plain).expect("the production URLs are well formed")
        };
        Self {
            public_rest: RestBase(parse("https://api.coin.z.com/public", "https", "http")),
            private_rest: RestBase(parse("https://api.coin.z.com/private", "https", "http")),
            public_ws: WsBase(parse("wss://api.coin.z.com/ws/public", "wss", "ws")),
            private_ws: WsBase(parse("wss://api.coin.z.com/ws/private", "wss", "ws")),
        }
    }

    /// モックサーバの配置: ループバックの 1 ポートに 4 つのパスプレフィックス
    /// (`/public`、`/private`、`/ws/public`、`/ws/private`)、平文トランスポート。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら 4 つのベースすべてが `addr` への平文トランスポートである。
    ///
    /// # エラー
    /// `addr.ip()` がループバックアドレスでないとき `Semantics("not loopback")`。
    pub fn loopback(addr: SocketAddr) -> Result<Self, InvalidValue> {
        let ip = LoopbackIp::new(addr.ip())?;
        let base = |scheme: &'static str, prefix: &str| Base {
            transport: Transport::Plain(ip),
            port: addr.port(),
            prefix: prefix.into(),
            scheme,
        };
        Ok(Self {
            public_rest: RestBase(base("http", "/public")),
            private_rest: RestBase(base("http", "/private")),
            public_ws: WsBase(base("ws", "/ws/public")),
            private_ws: WsBase(base("ws", "/ws/private")),
        })
    }
}

/// REST のベース URL: `https://host[:port][/prefix]`、または
/// `http://<loopback-ip>[:port][/prefix]`。
#[derive(Clone)]
pub struct RestBase(Base);

/// WebSocket のベース URL: `wss://host[:port][/prefix]`、または
/// `ws://<loopback-ip>[:port][/prefix]`。Private フィードは接続時に
/// `/v1/{token}` を付け加える。トークンはここには決して保存されない。
#[derive(Clone)]
pub struct WsBase(Base);

impl RestBase {
    /// REST のベース URL を解析する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら、平文の `http` ベースは必ずループバック IP リテラルを指す。
    ///
    /// # エラー
    /// 検証はサイズ (2048 バイト以下)、字句 (可視 ASCII)、構文
    /// (`scheme://authority[/prefix]`、クエリ・フラグメント・ユーザ情報・`..`・`%` なし)、
    /// 意味 (スキームが `https`、または `http` かつループバック IP リテラル) の順。
    pub fn parse(url: &str) -> Result<Self, InvalidValue> {
        Base::parse(url, "https", "http").map(Self)
    }
}

impl WsBase {
    /// WebSocket のベース URL を解析する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら、平文の `ws` ベースは必ずループバック IP リテラルを指す。
    ///
    /// # エラー
    /// [`RestBase::parse`] と同じ。スキームは `wss` と `ws`。
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

/// # 不変条件
/// `prefix` は空か、`A-Z a-z 0-9 - _` のセグメントからなる `/seg(/seg)*` で、
/// 決して `/` で終わらない。リクエストパスは `prefix + SignedPath` である。
/// `scheme` は `transport` と一致する (TLS なら `https`/`wss`、平文なら `http`/`ws`)。
#[derive(Clone)]
pub(crate) struct Base {
    pub(crate) transport: Transport,
    pub(crate) port: u16,
    pub(crate) prefix: Box<str>,
    scheme: &'static str,
}

impl Base {
    fn parse(
        url: &str,
        tls_scheme: &'static str,
        plain_scheme: &'static str,
    ) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::BaseUrl, url, 1, 2048, |b| b.is_ascii_graphic())?;
        let invalid = |violation| InvalidValue {
            primitive: Primitive::BaseUrl,
            violation,
        };
        let syntax = |why| invalid(Violation::Syntax(why));

        if url.contains(['?', '#', '@', '%']) || url.contains("..") {
            return Err(syntax(
                "query, fragment, userinfo, percent-encoding, and '..' are not allowed",
            ));
        }
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| syntax("missing scheme"))?;
        let (authority, prefix) = rest
            .find('/')
            .and_then(|i| rest.split_at_checked(i))
            .unwrap_or((rest, ""));
        if authority.is_empty() {
            return Err(syntax("missing host"));
        }
        let (host, port) = split_authority(authority).ok_or_else(|| syntax("bad authority"))?;
        if !prefix.is_empty() {
            if prefix.ends_with('/') {
                return Err(syntax("prefix ends with '/'"));
            }
            let segment_ok = |s: &str| {
                !s.is_empty()
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            };
            if !prefix.split('/').skip(1).all(segment_ok) {
                return Err(syntax("prefix segment"));
            }
        }

        let (transport, default_port, scheme) = if scheme == tls_scheme {
            let name = ServerName::try_from(host.to_owned())
                .map_err(|_| invalid(Violation::Semantics("host is not a valid server name")))?;
            (Transport::Tls(name), 443, tls_scheme)
        } else if scheme == plain_scheme {
            let ip: IpAddr = host
                .parse()
                .map_err(|_| invalid(Violation::Semantics("plain scheme needs an IP literal")))?;
            (Transport::Plain(LoopbackIp::new(ip)?), 80, plain_scheme)
        } else {
            return Err(invalid(Violation::Semantics("scheme")));
        };
        Ok(Self {
            transport,
            port: port.unwrap_or(default_port),
            prefix: prefix.into(),
            scheme,
        })
    }

    /// `Host:` ヘッダの値。スキームの既定ポートなら省く。
    pub(crate) fn host_header(&self) -> String {
        let host = self.host_text();
        let default_port = if self.is_tls() { 443 } else { 80 };
        if self.port == default_port {
            host
        } else {
            format!("{host}:{}", self.port)
        }
    }

    /// TCP 接続に使うホスト。IPv6 は角括弧なし。
    pub(crate) fn connect_host(&self) -> String {
        match &self.transport {
            Transport::Tls(name) => name.to_str().into_owned(),
            Transport::Plain(ip) => ip.ip().to_string(),
        }
    }

    pub(crate) fn is_tls(&self) -> bool {
        matches!(self.transport, Transport::Tls(_))
    }

    /// `scheme://host[:port]prefix`。
    pub(crate) fn url(&self) -> String {
        format!("{}://{}{}", self.scheme, self.host_header(), self.prefix)
    }

    /// URL で使うホストの綴り。IPv6 は角括弧付き。
    fn host_text(&self) -> String {
        match &self.transport {
            Transport::Tls(ServerName::IpAddress(ip)) => bracketed(IpAddr::from(*ip)),
            Transport::Tls(name) => name.to_str().into_owned(),
            Transport::Plain(ip) => bracketed(ip.ip()),
        }
    }
}

fn bracketed(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    }
}

/// `host[:port]` を分ける。IPv6 リテラルは `[...]` で囲まれている。
fn split_authority(authority: &str) -> Option<(&str, Option<u16>)> {
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        match after.strip_prefix(':') {
            Some(p) => (host, Some(p)),
            None if after.is_empty() => (host, None),
            None => return None,
        }
    } else {
        match authority.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        }
    };
    if host.is_empty() {
        return None;
    }
    let port = match port {
        Some(p) => Some(p.parse::<u16>().ok().filter(|&n| n != 0)?),
        None => None,
    };
    Some((host, port))
}

/// 接続の作り方。唯一の平文 variant はループバック IP を要求する。
#[derive(Clone)]
pub(crate) enum Transport {
    /// rustls による TLS 1.2 以上。サーバ名は webpki のルートで検証される。
    Tls(ServerName<'static>),
    /// 平文 TCP。ループバック限定。モック取引所のために存在する。
    Plain(LoopbackIp),
}

/// # 不変条件
/// `ip.is_loopback()` (`127.0.0.0/8` または `::1`)。コンストラクタは非公開。
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
    /// ベース URL は秘密を持たないので、そのまま出力する。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RestBase({})", self.0.url())
    }
}

impl fmt::Debug for WsBase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsBase({})", self.0.url())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_bases_are_tls_with_default_ports() {
        let e = Endpoints::production();
        assert!(e.public_rest.base().is_tls());
        assert_eq!(e.public_rest.base().host_header(), "api.coin.z.com");
        assert_eq!(
            e.private_rest.base().url(),
            "https://api.coin.z.com/private"
        );
        assert_eq!(e.private_ws.base().url(), "wss://api.coin.z.com/ws/private");
        assert_eq!(e.public_ws.base().port, 443);
        assert_eq!(
            format!("{:?}", e.public_rest),
            "RestBase(https://api.coin.z.com/public)"
        );
    }

    #[test]
    fn loopback_layout_matches_the_mock() {
        let e = Endpoints::loopback("127.0.0.1:18080".parse().unwrap()).unwrap();
        assert_eq!(e.public_rest.base().url(), "http://127.0.0.1:18080/public");
        assert_eq!(e.private_ws.base().url(), "ws://127.0.0.1:18080/ws/private");
        assert_eq!(e.public_rest.base().host_header(), "127.0.0.1:18080");
        assert!(!e.public_rest.base().is_tls());
        let v6 = Endpoints::loopback("[::1]:9".parse().unwrap()).unwrap();
        assert_eq!(v6.public_ws.base().url(), "ws://[::1]:9/ws/public");
        assert_eq!(v6.public_ws.base().connect_host(), "::1");
        let err = Endpoints::loopback("10.0.0.1:80".parse().unwrap()).unwrap_err();
        assert_eq!(err.violation, Violation::Semantics("not loopback"));
    }

    #[test]
    fn plain_schemes_need_a_loopback_ip_literal() {
        assert!(RestBase::parse("http://127.0.0.1:18080/public").is_ok());
        assert!(RestBase::parse("http://[::1]/public").is_ok());
        assert!(WsBase::parse("ws://127.0.0.1/ws/public").is_ok());
        for url in [
            "http://10.0.0.1/public",
            "http://localhost:18080/public",
            "http://api.coin.z.com/public",
            "ws://localhost/ws",
        ] {
            let err = if url.starts_with("ws") {
                WsBase::parse(url).map(|_| ())
            } else {
                RestBase::parse(url).map(|_| ())
            }
            .expect_err(url);
            assert_eq!(err.primitive, Primitive::BaseUrl, "{url}");
            assert!(
                matches!(err.violation, Violation::Semantics(_)),
                "{url}: {:?}",
                err.violation
            );
        }
    }

    #[test]
    fn syntax_is_checked_before_semantics() {
        let v = |url: &str| RestBase::parse(url).unwrap_err().violation;
        assert!(matches!(
            v("https://api.coin.z.com/public?x=1"),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("https://api.coin.z.com/public#f"),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("https://user@api.coin.z.com/public"),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("https://api.coin.z.com/../public"),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("https://api.coin.z.com/public/"),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("https://api.coin.z.com/pub%20lic"),
            Violation::Syntax(_)
        ));
        assert!(matches!(v("api.coin.z.com/public"), Violation::Syntax(_)));
        assert!(matches!(v("https:///public"), Violation::Syntax(_)));
        assert!(matches!(
            v("https://api.coin.z.com:0/public"),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("https://api.coin.z.com:70000/public"),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("https://api.coin.z.com/a b"),
            Violation::Lexical { .. }
        ));
        assert!(
            matches!(v("wss://api.coin.z.com/ws"), Violation::Semantics(_)),
            "wrong scheme family"
        );
        assert!(matches!(
            v(&format!("https://{}.com", "a".repeat(2048))),
            Violation::Size { .. }
        ));
    }

    #[test]
    fn explicit_ports_appear_in_the_host_header_only_when_not_default() {
        assert_eq!(
            RestBase::parse("https://api.coin.z.com:443/public")
                .unwrap()
                .base()
                .host_header(),
            "api.coin.z.com"
        );
        assert_eq!(
            RestBase::parse("https://api.coin.z.com:8443")
                .unwrap()
                .base()
                .host_header(),
            "api.coin.z.com:8443"
        );
        assert_eq!(
            RestBase::parse("https://api.coin.z.com")
                .unwrap()
                .base()
                .prefix
                .as_ref(),
            ""
        );
    }
}
