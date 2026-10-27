//! モックの WebSocket サーバ側。tokio-tungstenite の `accept_hdr_async` で
//! ハンドシェイクし、接続ごとに 1 タスクで読み書きする。
//!
//! - `/ws/public/v1` は Public。`/ws/private/v1/{token}` は Private で、
//!   `TokenStore` に生きているトークンだけを受け付け、それ以外は HTTP 401。
//!   他のパスは 404。`set_maintenance(true)` の間はすべて 503。
//! - クライアントのテキストフレームはすべて JSON として解析し、到着時刻と
//!   接続 id とともに記録する。ペーシングの検証に使う。
//! - テストは [`WsControl`] からスクリプトを送る: テキストの push、ping、
//!   close フレームなしの切断。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

use super::State;

/// 開いている接続と、それらに送られたもの。モックの `State` の中にある。
#[derive(Default)]
pub struct Hub {
    /// 受信したすべてのコマンド。到着時刻、API、接続 id、本文。
    pub commands: Vec<(Instant, Api, u64, Value)>,
    conns: Vec<Conn>,
    pub pongs: usize,
    /// 完了したハンドシェイクの数 (API ごと)。
    pub handshakes: Vec<(u64, Api)>,
    next_id: u64,
    maintenance: bool,
}

struct Conn {
    id: u64,
    api: Api,
    tx: mpsc::UnboundedSender<Outgoing>,
}

enum Outgoing {
    Text(String),
    Ping,
    Drop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Api {
    Public,
    Private,
}

/// モックの WebSocket エンドポイントに対する、テスト側の制御ハンドル。
pub struct WsControl {
    state: Arc<Mutex<State>>,
}

impl WsControl {
    pub(super) fn new(state: Arc<Mutex<State>>) -> Self {
        Self { state }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `api` の最新の接続で `channel` (と、指定があれば `symbol`) の subscribe が
    /// 届くまで (最長 `within`) 待ち、それを返す。
    pub async fn subscribed(
        &self,
        api: Api,
        channel: &str,
        symbol: Option<&str>,
        within: Duration,
    ) -> Value {
        let deadline = Instant::now() + within;
        loop {
            {
                let st = self.lock();
                let latest = st
                    .ws
                    .handshakes
                    .iter()
                    .rev()
                    .find(|(_, a)| *a == api)
                    .map(|(id, _)| *id);
                if let Some(latest) = latest {
                    let hit = st.ws.commands.iter().find(|(_, a, id, c)| {
                        *a == api
                            && *id == latest
                            && c["command"] == "subscribe"
                            && c["channel"] == channel
                            && symbol.is_none_or(|s| c["symbol"] == s)
                    });
                    if let Some((_, _, _, c)) = hit {
                        return c.clone();
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "no subscribe for {channel} {symbol:?} on {api:?} within {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// `api` の開いているすべての接続にテキストフレームを送る。
    pub fn push(&self, api: Api, json: &str) {
        for conn in self.lock().ws.conns.iter().filter(|c| c.api == api) {
            let _ = conn.tx.send(Outgoing::Text(json.to_owned()));
        }
    }

    /// `api` の開いているすべての接続を close フレームなしで閉じる。
    /// クライアントからはネットワークの切断に見える。
    pub fn drop_connections(&self, api: Api) {
        for conn in self.lock().ws.conns.iter().filter(|c| c.api == api) {
            let _ = conn.tx.send(Outgoing::Drop);
        }
    }

    /// ping を送り、`within` 以内に pong が返ったかを返す。
    pub async fn ping(&self, api: Api, within: Duration) -> bool {
        let before = {
            let st = self.lock();
            let conns: Vec<_> = st.ws.conns.iter().filter(|c| c.api == api).collect();
            if conns.is_empty() {
                return false;
            }
            for conn in conns {
                let _ = conn.tx.send(Outgoing::Ping);
            }
            st.ws.pongs
        };
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.lock().ws.pongs > before {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    /// これまでに受信したすべてのコマンド。到着順。
    pub fn commands(&self) -> Vec<(Instant, Api, Value)> {
        self.lock()
            .ws
            .commands
            .iter()
            .map(|(at, api, _, c)| (*at, *api, c.clone()))
            .collect()
    }

    /// `api` で完了したハンドシェイクの数。再接続の検証に使う。
    pub fn handshakes(&self, api: Api) -> usize {
        self.lock()
            .ws
            .handshakes
            .iter()
            .filter(|(_, a)| *a == api)
            .count()
    }

    /// `api` の開いている接続数。
    pub fn open_connections(&self, api: Api) -> usize {
        self.lock().ws.conns.iter().filter(|c| c.api == api).count()
    }

    /// `true` の間、すべてのハンドシェイクを HTTP 503 で拒否する。
    pub fn set_maintenance(&self, on: bool) {
        self.lock().ws.maintenance = on;
    }
}

/// アップグレード要求を 1 本処理する。
// tungstenite の `Callback` は `Result<Response, ErrorResponse>` を要求する。
#[allow(clippy::result_large_err)]
pub async fn serve(socket: TcpStream, state: Arc<Mutex<State>>) {
    let mut api = None;
    let callback = |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        let st = state.lock().unwrap_or_else(|e| e.into_inner());
        let reject = |code: u16| {
            let mut r = ErrorResponse::new(Some(format!("mock rejected the upgrade with {code}")));
            *r.status_mut() = http::StatusCode::from_u16(code).expect("valid status");
            Err(r)
        };
        if st.ws.maintenance {
            return reject(503);
        }
        let path = req.uri().path();
        if path == "/ws/public/v1" {
            api = Some(Api::Public);
            Ok(resp)
        } else if let Some(token) = path.strip_prefix("/ws/private/v1/") {
            if st.tokens.is_live(token) {
                api = Some(Api::Private);
                Ok(resp)
            } else {
                reject(401)
            }
        } else {
            reject(404)
        }
    };
    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(socket, callback).await else {
        return;
    };
    let api = api.expect("set by the callback on success");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let id = {
        let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
        st.ws.next_id += 1;
        let id = st.ws.next_id;
        st.ws.conns.push(Conn { id, api, tx });
        st.ws.handshakes.push((id, api));
        id
    };
    loop {
        tokio::select! {
            inbound = ws.next() => match inbound {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        state.lock().unwrap_or_else(|e| e.into_inner()).ws.commands.push((Instant::now(), api, id, value));
                    }
                }
                Some(Ok(Message::Pong(_))) => state.lock().unwrap_or_else(|e| e.into_inner()).ws.pongs += 1,
                Some(Ok(Message::Close(_))) => {
                    let _ = ws.close(None).await;
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            },
            outbound = rx.recv() => match outbound {
                Some(Outgoing::Text(text)) => {
                    if ws.send(Message::text(text)).await.is_err() {
                        break;
                    }
                }
                Some(Outgoing::Ping) => {
                    if ws.send(Message::Ping(Vec::new().into())).await.is_err() {
                        break;
                    }
                }
                Some(Outgoing::Drop) | None => break,
            },
        }
    }
    drop(ws);
    state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .ws
        .conns
        .retain(|c| c.id != id);
}
