//! TLS の経路を `cargo test` の中で通す。
//!
//! モック取引所はループバック限定の平文なので、`Transport::Tls` の分岐、
//! SNI、TLS のエラーの分類は本番接続テストでしか通らなかった (issue #11)。
//! ここでは `tests/fixtures/tls/` のテスト専用の自己署名証明書を信頼する
//! サーバを自前で立て、TLS 越しの REST 呼び出しと Public WebSocket 接続を
//! 1 回ずつ通す。サーバ側も tokio-rustls で書くので、依存は増えない。

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::LazyConfigAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::Acceptor;
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as WsRequest, Response as WsResponse,
};

use crate::client::{Anonymous, Client, Config};
use crate::domain::{Price, Symbol};
use crate::endpoint::{Endpoints, RestBase, WsBase};
use crate::error::TransportErrorKind;
use crate::transport::Connector;
use crate::ws::{PublicChannel, PublicEvent};

/// テスト専用の自己署名証明書 (SAN: `localhost`、`127.0.0.1`)。
const CERT: &[u8] = include_bytes!("../tests/fixtures/tls/localhost-cert.der");
/// `CERT` に対応する PKCS#8 の秘密鍵。テスト専用。
const KEY: &[u8] = include_bytes!("../tests/fixtures/tls/localhost-key.der");
const REST_FIXTURE: &[u8] = include_bytes!("../tests/fixtures/public_get_ticker.json");
const WS_FIXTURE: &str = include_str!("../tests/fixtures/ws_public_ticker.json");

/// サーバが受け取ったリクエストの、テストが検査する部分。
#[derive(Clone, Debug, PartialEq, Eq)]
struct Seen {
    method: String,
    path: String,
    query: Option<String>,
    host: Option<String>,
}

fn record<B>(req: &Request<B>) -> Seen {
    Seen {
        method: req.method().as_str().to_owned(),
        path: req.uri().path().to_owned(),
        query: req.uri().query().map(str::to_owned),
        host: req
            .headers()
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
    }
}

/// TLS ハンドシェイクでサーバが観測したもの。SNI は `ClientHello` から、
/// リクエストはハンドシェイクの後で記録する。
#[derive(Debug, Default)]
struct Recorded {
    sni: Option<String>,
    request: Option<Seen>,
}

/// クライアントと同じ ring プロバイダと ALPN で、フィクスチャの証明書を
/// 提示するサーバ設定。
fn server_config() -> Arc<ServerConfig> {
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports TLS 1.2 and 1.3")
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(CERT)],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY)),
        )
        .expect("the TLS test fixture is a valid certificate and key");
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(config)
}

/// `ClientHello` を読み、SNI を記録してからハンドシェイクを続ける。
async fn accept_tls(
    tcp: TcpStream,
    recorded: &Arc<Mutex<Recorded>>,
) -> Option<TlsStream<TcpStream>> {
    let start = LazyConfigAcceptor::new(Acceptor::default(), tcp)
        .await
        .ok()?;
    recorded.lock().expect("not poisoned").sni =
        start.client_hello().server_name().map(str::to_owned);
    start.into_stream(server_config()).await.ok()
}

/// TLS で 1 接続を受け、どんなリクエストにも `REST_FIXTURE` を 200 で返す。
async fn rest_server() -> (SocketAddr, Arc<Mutex<Recorded>>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let in_task = Arc::clone(&recorded);
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept");
        let Some(tls) = accept_tls(tcp, &in_task).await else {
            return;
        };
        let service = service_fn(move |req: Request<Incoming>| {
            let recorded = Arc::clone(&in_task);
            async move {
                recorded.lock().expect("not poisoned").request = Some(record(&req));
                let body = Full::new(Bytes::from_static(REST_FIXTURE));
                let response = Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(body)
                    .expect("a valid response");
                Ok::<_, Infallible>(response)
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(tls), service)
            .await;
    });
    (addr, recorded, task)
}

/// TLS で 1 接続を受け、subscribe を 1 つ読んでから ticker を 1 つ送る。
#[expect(
    clippy::result_large_err,
    reason = "tungstenite の `Callback` は `Result<Response, ErrorResponse>` を要求し、`ErrorResponse` は大きい"
)]
async fn ws_server() -> (SocketAddr, Arc<Mutex<Recorded>>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let in_task = Arc::clone(&recorded);
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept");
        let Some(tls) = accept_tls(tcp, &in_task).await else {
            return;
        };
        let mut handshake = None;
        let callback = |req: &WsRequest, resp: WsResponse| -> Result<WsResponse, ErrorResponse> {
            handshake = Some(record(req));
            Ok(resp)
        };
        let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tls, callback).await else {
            return;
        };
        in_task.lock().expect("not poisoned").request = handshake;
        let _ = ws.next().await;
        if ws.send(Message::text(WS_FIXTURE)).await.is_err() {
            return;
        }
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(_))) => {
                    // tungstenite は close への返答を積むだけなので、読む側で
                    // flush しないとクライアントの close ハンドシェイクが終わらない。
                    let _ = ws.flush().await;
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
        // TLS の切断は close_notify まで送る。TCP をそのまま落とすと、クライアントの
        // rustls は close_notify のない EOF をエラーにする。
        let _ = tokio::io::AsyncWriteExt::shutdown(ws.get_mut()).await;
    });
    (addr, recorded, task)
}

/// `localhost` の TLS ベース 4 つ。DNS 名なので、クライアントは SNI を送る。
fn endpoints(rest_port: u16, ws_port: u16) -> Endpoints {
    let rest = |prefix: &str| RestBase::parse(&format!("https://localhost:{rest_port}{prefix}"));
    let ws = |prefix: &str| WsBase::parse(&format!("wss://localhost:{ws_port}{prefix}"));
    Endpoints {
        public_rest: rest("/public").expect("a valid base"),
        private_rest: rest("/private").expect("a valid base"),
        public_ws: ws("/ws/public").expect("a valid base"),
        private_ws: ws("/ws/private").expect("a valid base"),
    }
}

/// フィクスチャの証明書を追加で信頼するクライアント。
fn client(endpoints: Endpoints) -> Client<Anonymous> {
    Client::with_connector(endpoints, Config::default(), Connector::trusting_also(CERT))
}

#[tokio::test]
async fn a_rest_call_over_tls_returns_the_ticker() {
    let (rest, recorded, server) = rest_server().await;
    let client = client(endpoints(rest.port(), rest.port()));

    let ticker = client.ticker(Symbol::BTC).await.expect("a ticker over TLS");
    assert_eq!(ticker.symbol, Symbol::BTC);
    assert_eq!(ticker.ask, Price::parse("750760").expect("a valid price"));
    assert_eq!(ticker.last, Price::parse("756662").expect("a valid price"));

    let recorded = recorded.lock().expect("not poisoned");
    assert_eq!(recorded.sni.as_deref(), Some("localhost"));
    let seen = recorded
        .request
        .clone()
        .expect("the server saw the request");
    assert_eq!(seen.method, "GET");
    assert_eq!(seen.path, "/public/v1/ticker");
    assert_eq!(seen.query.as_deref(), Some("symbol=BTC"));
    assert_eq!(
        seen.host.as_deref(),
        Some(format!("localhost:{}", rest.port()).as_str())
    );
    server.abort();
}

#[tokio::test]
async fn a_public_feed_connects_over_tls_and_delivers_a_ticker() {
    let (ws, recorded, server) = ws_server().await;
    let client = client(endpoints(ws.port(), ws.port()));
    let mut feed = client.public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC))
        .expect("a free subscription slot");

    let event = tokio::time::timeout(Duration::from_secs(5), feed.next())
        .await
        .expect("a ticker within 5 s")
        .expect("the feed stays open");
    match event {
        PublicEvent::Ticker(ticker) => {
            assert_eq!(ticker.symbol, Symbol::BTC);
            assert_eq!(ticker.last, Price::parse("756662").expect("a valid price"));
        }
        other => panic!("expected a ticker, got {other:?}"),
    }

    let seen = {
        let recorded = recorded.lock().expect("not poisoned");
        assert_eq!(recorded.sni.as_deref(), Some("localhost"));
        recorded
            .request
            .clone()
            .expect("the server saw the handshake")
    };
    assert_eq!(seen.method, "GET");
    assert_eq!(seen.path, "/ws/public/v1");
    assert_eq!(
        seen.host.as_deref(),
        Some(format!("localhost:{}", ws.port()).as_str())
    );
    feed.close().await.expect("an error-free close");
    server.abort();
}

#[tokio::test]
async fn an_untrusted_chain_is_reported_as_a_tls_error() {
    let (rest, recorded, server) = rest_server().await;
    let endpoints = endpoints(rest.port(), rest.port());
    let config = Config::default();

    let err = Connector::new()
        .connect(endpoints.public_rest.base(), &config.timeouts)
        .await
        .expect_err("the fixture certificate is not in the webpki roots");
    assert_eq!(err, TransportErrorKind::Tls);
    assert_eq!(
        recorded.lock().expect("not poisoned").sni.as_deref(),
        Some("localhost")
    );
    server.abort();
}
