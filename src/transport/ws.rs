//! WebSocket の接続。tokio-tungstenite の `client_async_with_config` を、
//! このクレート自身のコネクタが作ったストリームの上で走らせる。
//! tungstenite の TLS 機能は決して有効にしない。

use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use zeroize::Zeroizing;

use super::{Connector, Stream, io_kind};
use crate::client::{Limits, Timeouts};
use crate::endpoint::Base;
use crate::error::{Phase, TransportErrorKind};

/// 接続済みの WebSocket。読み書きの進捗はすべてこの値の中にあるので、
/// `ws::Session` が `select!` の中で使える。
pub(crate) type WsStream = WebSocketStream<Stream>;

/// 接続が失敗した理由。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WsConnectError {
    /// ネットワーク、TLS、タイムアウト、フレーミング。
    Transport(TransportErrorKind),
    /// サーバがアップグレードを拒否した。`503` はメンテナンス、`401`/`404` は
    /// 失効した Private トークン。応答本体は保持しない。
    Rejected { http_status: u16 },
}

/// `base` に `path` を付けた先へ接続し、ハンドシェイクまで行う。
///
/// 手順: (1) コネクタで TLS または平文のストリームを作る。(2) `base` と
/// `path` から URI をローカルに組み立てる (Private フィードではトークンを含む)。
/// (3) `Limits` の `max_message_size` と `max_frame_size` を載せて
/// `client_async_with_config` に渡し、URI の文字列はすぐに捨てる。
/// (4) tungstenite のエラーはすべて種類だけに写す。
///
/// # 事前条件
/// `path` は `/` で始まり、検証済みのプリミティブだけから組み立てられている。
///
/// # 事後条件
/// `Ok` なら 101 を受け取り、以後のメッセージは `Limits` の上限で検査される。
/// 接続は `timeouts.connect`、ハンドシェイクは `timeouts.ws_handshake` に収まる。
///
/// # エラー
/// 接続・TLS・タイムアウトは `Transport`、101 以外の HTTP 状態は `Rejected`。
pub(crate) async fn connect(
    connector: &Connector,
    base: &Base,
    path: &str,
    timeouts: &Timeouts,
    limits: &Limits,
) -> Result<WsStream, WsConnectError> {
    let stream = connector
        .connect(base, timeouts)
        .await
        .map_err(WsConnectError::Transport)?;
    let uri = {
        let text = Zeroizing::new(format!("{}{path}", base.url()));
        http::Uri::try_from(text.as_str())
            .map_err(|_| WsConnectError::Transport(TransportErrorKind::Framing("websocket uri")))?
    };
    let limit = limits.max_ws_message_bytes.get() as usize;
    let config = WebSocketConfig::default()
        .max_message_size(Some(limit))
        .max_frame_size(Some(limit));
    let handshake = tokio_tungstenite::client_async_with_config(uri, stream, Some(config));
    match tokio::time::timeout(timeouts.ws_handshake.get(), handshake).await {
        Ok(Ok((ws, _response))) => Ok(ws),
        Ok(Err(WsError::Http(response))) => Err(WsConnectError::Rejected {
            http_status: response.status().as_u16(),
        }),
        Ok(Err(e)) => Err(WsConnectError::Transport(error_kind(&e, limits))),
        Err(_elapsed) => Err(WsConnectError::Transport(TransportErrorKind::Timeout(
            Phase::WsHandshake,
        ))),
    }
}

/// tungstenite のエラーを種類だけに写す。応答、URI、メッセージは保持しない。
/// `ws::Session` が読み書きの失敗にも使う。
pub(crate) fn error_kind(e: &WsError, limits: &Limits) -> TransportErrorKind {
    match e {
        WsError::ConnectionClosed | WsError::AlreadyClosed => TransportErrorKind::ClosedEarly,
        WsError::Io(io) => io_kind(io),
        WsError::Tls(_) => TransportErrorKind::Tls,
        WsError::Capacity(_) => TransportErrorKind::TooLarge {
            limit: limits.max_ws_message_bytes.get(),
        },
        WsError::Protocol(_) | WsError::AttackAttempt => TransportErrorKind::Framing("websocket"),
        WsError::WriteBufferFull(_) => TransportErrorKind::Framing("websocket write buffer"),
        WsError::Utf8(_) => TransportErrorKind::Framing("utf-8"),
        WsError::Url(_) => TransportErrorKind::Framing("websocket uri"),
        WsError::Http(response) => {
            TransportErrorKind::Framing(if response.status().is_server_error() {
                "websocket handshake rejected (5xx)"
            } else {
                "websocket handshake rejected"
            })
        }
        WsError::HttpFormat(_) => TransportErrorKind::Framing("http"),
    }
}

#[cfg(test)]
mod tests {
    // tungstenite の `Callback` は `Result<Response, ErrorResponse>` を要求する。
    #![allow(clippy::result_large_err)]

    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::client::{ByteLimit, Config};
    use crate::endpoint::Endpoints;

    /// ループバックに tungstenite のサーバを立て、1 接続を受けて `serve` に渡す。
    async fn server<F, Fut>(serve: F) -> std::net::SocketAddr
    where
        F: FnOnce(WebSocketStream<tokio::net::TcpStream>, http::Uri) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut seen = None;
            let ws = tokio_tungstenite::accept_hdr_async(tcp, |req: &http::Request<()>, resp| {
                seen = Some(req.uri().clone());
                Ok(resp)
            })
            .await
            .unwrap();
            serve(ws, seen.unwrap()).await;
        });
        addr
    }

    #[tokio::test]
    async fn handshake_sends_the_base_prefix_plus_path_and_receives_text() {
        let addr = server(|mut ws, uri| async move {
            assert_eq!(uri.path(), "/ws/public/v1");
            ws.send(Message::text("{\"hello\":1}")).await.unwrap();
        })
        .await;
        let endpoints = Endpoints::loopback(addr).unwrap();
        let config = Config::default();
        let mut ws = connect(
            &Connector::new(),
            endpoints.public_ws.base(),
            "/v1",
            &config.timeouts,
            &config.limits,
        )
        .await
        .unwrap();
        let message = ws.next().await.unwrap().unwrap();
        assert_eq!(message, Message::text("{\"hello\":1}"));
    }

    #[tokio::test]
    async fn a_message_over_the_limit_is_reported_as_too_large() {
        let addr = server(|mut ws, _| async move {
            ws.send(Message::text("x".repeat(2000))).await.unwrap();
            let _ = ws.next().await;
        })
        .await;
        let endpoints = Endpoints::loopback(addr).unwrap();
        let mut config = Config::default();
        config.limits.max_ws_message_bytes = ByteLimit::new(1024).unwrap();
        let mut ws = connect(
            &Connector::new(),
            endpoints.public_ws.base(),
            "/v1",
            &config.timeouts,
            &config.limits,
        )
        .await
        .unwrap();
        let err = ws.next().await.unwrap().unwrap_err();
        assert_eq!(
            error_kind(&err, &config.limits),
            TransportErrorKind::TooLarge { limit: 1024 }
        );
    }

    #[tokio::test]
    async fn a_rejected_upgrade_reports_the_status_only() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let result =
                tokio_tungstenite::accept_hdr_async(tcp, |_req: &http::Request<()>, _resp| {
                    let mut reject = http::Response::new(Some("maintenance".to_owned()));
                    *reject.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                    Err(reject)
                })
                .await;
            assert!(result.is_err());
        });
        let endpoints = Endpoints::loopback(addr).unwrap();
        let config = Config::default();
        let err = connect(
            &Connector::new(),
            endpoints.private_ws.base(),
            "/v1/token",
            &config.timeouts,
            &config.limits,
        )
        .await
        .unwrap_err();
        assert_eq!(err, WsConnectError::Rejected { http_status: 503 });
    }
}
