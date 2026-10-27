//! モックの HTTP/1.1 サーバ側。hyper の `server::conn::http1` で動く。
//!
//! リクエスト処理の順序:
//! 1. ボディを読み切り、認証ヘッダを取引所と同じ順序で検査して
//!    (`sign::check`)、結果とともに記録する。
//! 2. `DropConnection` の期待に一致すれば、認証の結果にかかわらず閉じる。
//! 3. `/private` で認証に失敗していれば、その ERR-50xx を HTTP 200 の
//!    エンベロープで返す。
//! 4. 登録順に最初に一致する期待を探す。`times(n)` を使い切ったものは飛ばす。
//! 5. 期待がなく `/private/v1/ws-auth` なら、組み込みの `TokenStore` ルート。
//! 6. それ以外は未一致として記録し、404 を返す。`assert_done` が失敗する。
//!
//! `CloseAfterRequest` と `DropConnection` はサービスがエラーを返すことで
//! 実現する。hyper はその接続を応答なしで閉じる。

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::stream;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use super::sign::{self, AuthHeaders};
use super::{Recorded, Reply, SignatureCheck, State, api_error, fixture};

type Body = BoxBody<Bytes, Infallible>;

/// サービスがこれを返すと hyper は応答を書かずに接続を閉じる。
#[derive(Debug)]
struct Dropped;

impl std::fmt::Display for Dropped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("connection dropped by the mock on purpose")
    }
}

impl std::error::Error for Dropped {}

pub async fn serve(socket: TcpStream, state: Arc<Mutex<State>>) {
    let service = service_fn(move |req| handle(req, state.clone()));
    let _ = hyper::server::conn::http1::Builder::new()
        .keep_alive(true)
        .serve_connection(TokioIo::new(socket), service)
        .await;
}

/// 1 リクエストを処理する。ボディを読み切り、署名を検査して記録してから
/// 応答を決める。`DropConnection` も記録までは同じで、認証の結果に
/// かかわらず応答せずに閉じる (ネットワークが落ちたのであって、取引所が
/// 拒否したのではない)。
async fn handle(
    req: Request<Incoming>,
    state: Arc<Mutex<State>>,
) -> Result<Response<Body>, Dropped> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().map(str::to_owned);
    let header = |name: &str| {
        req.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let (key, timestamp, sign) = (
        header("API-KEY"),
        header("API-TIMESTAMP"),
        header("API-SIGN"),
    );
    let at = Instant::now();

    let body = req
        .into_body()
        .collect()
        .await
        .map_err(|_| Dropped)?
        .to_bytes()
        .to_vec();
    let headers = AuthHeaders {
        key: key.as_deref(),
        timestamp: timestamp.as_deref(),
        sign: sign.as_deref(),
    };
    let auth = sign::check(sign::now_ms(), headers, &method, &path, &body);
    let signature = sign::classify(headers, &auth);

    let reply = {
        let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
        st.requests.push(Recorded {
            method: method.clone(),
            path: path.clone(),
            query: query.clone(),
            body: body.clone(),
            signature,
            at,
        });
        let index = st.requests.len() - 1;
        let drop_now = st.routes.iter_mut().find(|r| {
            matches!(r.reply, Reply::DropConnection)
                && r.json_body.is_none()
                && r.method == method
                && r.path == path
                && r.query
                    .as_ref()
                    .is_none_or(|q| Some(q.as_str()) == query.as_deref())
                && r.times.is_none_or(|n| r.hits < n)
        });
        if let Some(route) = drop_now {
            route.hits += 1;
            return Err(Dropped);
        }
        if path.starts_with("/private")
            && let Err(code) = auth
        {
            Reply::Json(api_error(code, "mock authentication failure"))
        } else {
            let parsed_body: Option<serde_json::Value> = serde_json::from_slice(&body).ok();
            let route = st.routes.iter_mut().find(|r| {
                r.method == method
                    && r.path == path
                    && r.query
                        .as_ref()
                        .is_none_or(|q| Some(q.as_str()) == query.as_deref())
                    && r.json_body
                        .as_ref()
                        .is_none_or(|b| Some(b) == parsed_body.as_ref())
                    && (!r.signed || signature == SignatureCheck::Valid)
                    && r.times.is_none_or(|n| r.hits < n)
            });
            match route {
                Some(route) => {
                    route.hits += 1;
                    route.reply.clone()
                }
                None if path == "/private/v1/ws-auth" => {
                    ws_auth(&mut st, &method, parsed_body.as_ref())
                }
                None => {
                    st.unmatched.push(index);
                    Reply::Status {
                        code: 404,
                        body: "<html>unmatched request</html>".to_owned(),
                    }
                }
            }
        }
    };
    render(reply).await
}

/// 組み込みの `ws-auth` ルート。
fn ws_auth(st: &mut State, method: &str, body: Option<&serde_json::Value>) -> Reply {
    let token = body.and_then(|b| b.get("token")).and_then(|t| t.as_str());
    let ok = |data: Option<String>| {
        let data = data
            .map(|d| format!(r#""data":"{d}","#))
            .unwrap_or_default();
        Reply::Json(format!(
            r#"{{"status":0,{data}"responsetime":"2026-10-27T11:49:18.425Z"}}"#
        ))
    };
    match (method, token) {
        ("POST", _) => ok(Some(st.tokens.issue())),
        ("PUT", Some(token)) if st.tokens.extend(token) => ok(None),
        ("DELETE", Some(token)) if st.tokens.delete(token) => ok(None),
        _ => Reply::Json(api_error("ERR-5106", "Invalid request parameter.")),
    }
}

async fn render(reply: Reply) -> Result<Response<Body>, Dropped> {
    let json = |status: u16, body: String| {
        Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)).boxed())
            .expect("valid response")
    };
    match reply {
        Reply::Fixture(name) => Ok(json(200, fixture(name))),
        Reply::Json(body) => Ok(json(200, body)),
        Reply::JsonThenClose(body) => Ok(Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .header("connection", "close")
            .body(Full::new(Bytes::from(body)).boxed())
            .expect("valid response")),
        Reply::Status { code, body } => Ok(Response::builder()
            .status(code)
            .body(Full::new(Bytes::from(body)).boxed())
            .expect("valid response")),
        Reply::Chunked { body, chunk } => {
            let chunks: Vec<Result<Frame<Bytes>, Infallible>> = body
                .into_bytes()
                .chunks(chunk.max(1))
                .map(|c| Ok(Frame::data(Bytes::copy_from_slice(c))))
                .collect();
            Ok(Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(StreamBody::new(stream::iter(chunks)).boxed())
                .expect("valid response"))
        }
        Reply::CloseAfterRequest | Reply::DropConnection => Err(Dropped),
        Reply::Delay(wait, inner) => {
            tokio::time::sleep(wait).await;
            Box::pin(render(*inner)).await
        }
        Reply::Stall => {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Err(Dropped)
        }
    }
}
