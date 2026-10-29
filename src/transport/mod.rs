//! ネットワーク上のバイト列。クレート内部に閉じている。
//!
//! プロトコルの実装は、広く使われ検証されたライブラリに任せる。HTTP/1.1 は
//! hyper、WebSocket は tokio-tungstenite、TLS は rustls (ring プロバイダ、
//! webpki-roots のルート証明書)。このモジュールが自前で持つのは、接続の
//! 作り方 (ループバック限定の平文、それ以外は TLS)、ルートごとの有界な
//! アイドル接続プールと同時実行の上限、`Limits` に基づくサイズ上限、そして
//! ライブラリのエラーを「種類だけ」のエラーへ写す変換である。hyper、http、
//! rustls、tungstenite の型は公開シグネチャに一切現れない。
//!
//! # 残存リスク
//! `API-KEY` と `API-SIGN` ヘッダは hyper の `HeaderMap` に、`ws-auth` の
//! PUT/DELETE ボディ (アクセストークンを含む) は hyper の書き込みバッファに、それぞれ
//! ゼロ埋めされない形で複製される。同様に、Private WebSocket の接続先 URI
//! (トークンを含む) は tungstenite の内部バッファに残る。これらはライブラリ
//! の所有物であり、このクレートからゼロ埋めする手段がない。クレートが所有
//! するバッファ (`Call::body`、応答ボディ、`Answer::data`、接続先パスの
//! 組み立て用文字列) はすべてゼロ埋めされる。ただし応答ボディは
//! `Content-Length` の分を先に確保するものの、`Transfer-Encoding: chunked`
//! の応答では伸長のたびに古い割り当てが複製として残り、それはゼロ埋め
//! されない。hyper の受信バッファに残る応答の複製も同様である。

pub(crate) mod http;
pub(crate) mod ws;

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
#[cfg(test)]
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::client::Timeouts;
use crate::endpoint::{Base, Transport};
use crate::error::{Phase, TransportErrorKind};

/// 接続を開く。プロセスに 1 つの rustls 設定を保持する。
pub(crate) struct Connector {
    tls: TlsConnector,
}

impl Connector {
    /// ring プロバイダに固定した rustls 設定 (TLS 1.2 と 1.3、webpki のルート
    /// 証明書、ALPN `http/1.1`) を組み立てる。下流の `aws-lc-rs` が
    /// プロバイダを曖昧にすることはない。
    ///
    /// # パニック
    /// ring プロバイダが TLS 1.2/1.3 を提供しない場合。ring では起きない。
    pub(crate) fn new() -> Self {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Self::with_roots(roots)
    }

    /// テスト専用: webpki のルートに加えて、`der` のサーバ証明書を信頼する。
    /// `tests/fixtures/tls/` の自己署名証明書だけに使う。
    ///
    /// # パニック
    /// `der` が証明書として読めない場合。フィクスチャはテストで検証済み。
    #[cfg(test)]
    pub(crate) fn trusting_also(der: &'static [u8]) -> Self {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        roots
            .add(CertificateDer::from(der))
            .expect("the TLS test fixture is a valid certificate");
        Self::with_roots(roots)
    }

    /// ルート証明書の集合から rustls 設定を組み立てる。
    ///
    /// # パニック
    /// ring プロバイダが TLS 1.2/1.3 を提供しない場合。ring では起きない。
    #[expect(
        clippy::expect_used,
        reason = "ring プロバイダは TLS 1.2 と 1.3 を提供するので、doc の # パニック は起きない"
    )]
    fn with_roots(roots: RootCertStore) -> Self {
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("the ring provider supports TLS 1.2 and 1.3")
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self {
            tls: TlsConnector::from(Arc::new(config)),
        }
    }

    /// TCP 接続と、`Transport::Tls` なら TLS ハンドシェイクを、まとめて
    /// `timeouts.connect` の中で行う。
    ///
    /// # 事後条件
    /// `Stream::Plain` が返るのは `Transport::Plain` のときだけで、それは
    /// ループバックアドレスにしか存在しない。平文では DNS 問い合わせも起きない。
    pub(crate) async fn connect(
        &self,
        base: &Base,
        timeouts: &Timeouts,
    ) -> Result<Stream, TransportErrorKind> {
        let dial = async {
            match &base.transport {
                Transport::Plain(ip) => {
                    let tcp = TcpStream::connect((ip.ip(), base.port))
                        .await
                        .map_err(|e| io_kind(&e))?;
                    tcp.set_nodelay(true).map_err(|e| io_kind(&e))?;
                    Ok(Stream::Plain(tcp))
                }
                Transport::Tls(name) => {
                    let tcp = TcpStream::connect((base.connect_host(), base.port))
                        .await
                        .map_err(|e| io_kind(&e))?;
                    tcp.set_nodelay(true).map_err(|e| io_kind(&e))?;
                    let tls = self
                        .tls
                        .connect(name.clone(), tcp)
                        .await
                        .map_err(|e| tls_kind(&e))?;
                    Ok(Stream::Tls(Box::new(tls)))
                }
            }
        };
        match tokio::time::timeout(timeouts.connect.get(), dial).await {
            Ok(result) => result,
            Err(_) => Err(TransportErrorKind::Timeout(Phase::Connect)),
        }
    }
}

/// 1 本の接続。同時にちょうど 1 つのリクエストまたは 1 つのフィードが所有する。
pub(crate) enum Stream {
    Tls(Box<TlsStream<TcpStream>>),
    Plain(TcpStream),
}

impl core::fmt::Debug for Stream {
    /// 接続の種類だけ。アドレスや状態は出力しない。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Stream::Tls(_) => "Stream::Tls",
            Stream::Plain(_) => "Stream::Plain",
        })
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, data),
            Stream::Plain(s) => Pin::new(s).poll_write(cx, data),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// `io::Error` を種類だけに落とす。ペイロードは任意の文字列を含み得るので保持しない。
pub(crate) fn io_kind(e: &io::Error) -> TransportErrorKind {
    match e.kind() {
        io::ErrorKind::UnexpectedEof => TransportErrorKind::ClosedEarly,
        kind => TransportErrorKind::Io(kind),
    }
}

/// TLS ハンドシェイクの失敗。rustls のエラーは `io::Error` に包まれて届く。
fn tls_kind(e: &io::Error) -> TransportErrorKind {
    let is_tls = e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<tokio_rustls::rustls::Error>())
        .is_some();
    if is_tls {
        TransportErrorKind::Tls
    } else {
        io_kind(e)
    }
}
