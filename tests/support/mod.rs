//! A local GMO Coin exchange for tests.
//!
//! One `127.0.0.1` port serves all four APIs under the production path
//! layout (`/public`, `/private`, `/ws/public`, `/ws/private`), which is
//! what `Endpoints::loopback` expects.
//!
//! Independence is the point. The mock's HTTP and WebSocket server code is
//! written here, separately from the client's `wire` module, so a framing
//! bug in the client is not mirrored by the same bug in the mock. It uses
//! only crates already in the normal tree: tokio, httparse (request heads),
//! ring (HMAC verification, SHA-1 for `Sec-WebSocket-Accept`), serde_json.
//!
//! Tests program it with expectations (method + path + optional query and
//! JSON body + optional "must be signed") and canned replies, usually the
//! official fixtures in `tests/fixtures/`. It verifies every private
//! request's signature the way the exchange does and answers ERR-5010 on a
//! mismatch, so a signing bug fails the test with the real error shape.

// Each test binary uses a different subset of the mock.
#![allow(dead_code, unused_imports)]

mod http;
mod sign;
mod ws;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gmo_coin_api_client::{Anonymous, ApiKey, ApiSecret, Client, Config, Credentials, Endpoints};

pub use ws::{Api, WsControl};

/// Fixed credentials the mock verifies signatures against.
pub const TEST_KEY: &str = "mockkey0123456789";
pub const TEST_SECRET: &str = "mocksecret0123456789";

pub struct MockExchange {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    accept_loop: tokio::task::JoinHandle<()>,
}

/// Everything the mock knows. One lock, held only for bookkeeping, never
/// across socket I/O.
#[derive(Default)]
struct State {
    routes: Vec<Route>,
    requests: Vec<Recorded>,
    /// Live private WebSocket tokens, oldest-expiring first. At most 5; a
    /// sixth evicts the front, as the exchange documents.
    tokens: Vec<(String, Instant)>,
    ws: ws::Hub,
}

struct Route {
    method: &'static str,
    path: &'static str,
    query: Option<String>,
    json_body: Option<serde_json::Value>,
    signed: bool,
    reply: Reply,
    /// `None`: any number of times. `Some(n)`: exactly n, checked by `assert_done`.
    times: Option<usize>,
    hits: usize,
}

/// A canned answer.
#[derive(Clone)]
pub enum Reply {
    /// A file in `tests/fixtures/`, sent with HTTP 200.
    Fixture(&'static str),
    /// A literal body with HTTP 200 (the exchange's usual status, even for errors).
    Json(String),
    /// Another status, for example 404 with an HTML body or 503.
    Status { code: u16, body: String },
    /// Same body, `Transfer-Encoding: chunked` with chunks of this size.
    Chunked { body: String, chunk: usize },
    /// Read the request, then close without answering (outcome unknown).
    CloseAfterRequest,
    /// Wait before answering, to exercise timeouts.
    Delay(Duration, Box<Reply>),
}

/// One request as received.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body: Vec<u8>,
    pub signature: SignatureCheck,
    pub at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignatureCheck {
    /// No `API-SIGN` header (public calls).
    Absent,
    Valid,
    /// Present but wrong. The mock answered ERR-5010.
    Invalid,
}

impl MockExchange {
    /// Binds `127.0.0.1:0` and spawns the accept loop.
    pub async fn start() -> Self {
        // TODO let listener = TcpListener::bind("127.0.0.1:0").await;
        // spawn loop { (sock, _) = accept; spawn(serve(sock, state.clone())) }
        // serve: http::read_request (httparse); if Upgrade: websocket => ws::serve;
        //        else record, verify signature for /private paths, match route, reply.
        todo!()
    }

    pub fn endpoints(&self) -> Endpoints {
        Endpoints::loopback(self.addr).expect("mock binds loopback")
    }

    /// A client pointed at this mock with the default configuration.
    pub fn client(&self) -> Client<Anonymous> {
        Client::new(self.endpoints(), Config::default())
    }

    pub fn credentials() -> Credentials {
        Credentials::new(
            ApiKey::new(TEST_KEY.to_owned()).expect("valid key"),
            ApiSecret::new(TEST_SECRET.to_owned()).expect("valid secret"),
        )
    }

    /// Starts an expectation for `method path` (path as sent, including the
    /// `/public` or `/private` prefix).
    pub fn on(&self, method: &'static str, path: &'static str) -> RouteBuilder<'_> {
        RouteBuilder {
            mock: self,
            route: Route {
                method,
                path,
                query: None,
                json_body: None,
                signed: false,
                reply: Reply::Json(String::new()),
                times: None,
                hits: 0,
            },
        }
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.state.lock().expect("mock state").requests.clone()
    }

    pub fn ws(&self) -> WsControl {
        todo!()
    }

    /// Panics unless every `times(n)` route was hit exactly n times, every
    /// request matched a route, and no signature was invalid.
    pub fn assert_done(&self) {
        todo!()
    }
}

impl Drop for MockExchange {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

pub struct RouteBuilder<'a> {
    mock: &'a MockExchange,
    route: Route,
}

impl RouteBuilder<'_> {
    /// Exact query string, for example `"symbol=BTC"`.
    pub fn query(mut self, query: &str) -> Self {
        self.route.query = Some(query.to_owned());
        self
    }

    /// The request body must equal this JSON (compared as values, so key
    /// order does not matter, but the signature is still checked over the
    /// raw bytes).
    pub fn json_body(mut self, body: serde_json::Value) -> Self {
        self.route.json_body = Some(body);
        self
    }

    /// The request must carry a valid signature.
    pub fn signed(mut self) -> Self {
        self.route.signed = true;
        self
    }

    pub fn times(mut self, n: usize) -> Self {
        self.route.times = Some(n);
        self
    }

    pub fn reply(mut self, reply: Reply) {
        self.route.reply = reply;
        self.mock
            .state
            .lock()
            .expect("mock state")
            .routes
            .push(self.route);
    }

    pub fn fixture(self, name: &'static str) {
        self.reply(Reply::Fixture(name));
    }
}

/// Reads `tests/fixtures/<name>`.
pub fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// The exchange's own error shape, for `Reply::Json`.
pub fn api_error(code: &str, text: &str) -> String {
    format!(
        r#"{{"status":1,"messages":[{{"message_code":"{code}","message_string":"{text}"}}],"responsetime":"2026-10-27T11:49:18.425Z"}}"#
    )
}
