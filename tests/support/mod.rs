//! テスト用のローカル GMO コイン取引所。
//!
//! `127.0.0.1` の 1 ポートで 4 つの API すべてを本番と同じパス配置
//! (`/public`、`/private`、`/ws/public`、`/ws/private`) で提供する。
//! `Endpoints::loopback` が期待する配置である。
//!
//! サーバ側は hyper の HTTP/1.1 サーバと tokio-tungstenite の
//! `accept_hdr_async` で動く。どちらも通常の依存ツリーに既にあるクレートで、
//! dev 依存は hyper の `server` 機能と tokio の `sync` 機能を足すだけである。
//!
//! テストは期待 (メソッド + パス + 省略可能なクエリと JSON ボディ +
//! 「署名必須」) と定型の応答 (通常は `tests/fixtures/` の公式サンプル) で
//! モックをプログラムする。モックは Private リクエストの署名を取引所と同じ
//! 方法で検証し、不一致には ERR-5010 を HTTP 200 のエンベロープで返す。
//! したがって署名のバグは本物と同じエラーの形でテストを落とす。
//!
//! `ws-auth` の 3 操作は期待を登録しなくても動く組み込みルートで、
//! [`TokenStore`] を更新する。期待を登録すればそちらが優先される。

#![expect(
    dead_code,
    reason = "各テストバイナリはモックの異なる部分集合を使い、このモジュールをそれぞれ別にコンパイルする"
)]
#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "テストの補助関数は #[test] 関数ではないため clippy.toml の allow-*-in-tests が届かない。失敗はそのままテストの失敗になる"
)]

mod http;
pub(crate) mod sign;
pub(crate) mod ws;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gmo_coin_api_client::{Anonymous, ApiKey, ApiSecret, Client, Config, Credentials, Endpoints};
use tokio::net::TcpListener;

pub(crate) use ws::WsControl;

/// モックが署名を検証する固定の認証情報。
pub(crate) const TEST_KEY: &str = "mockkey0123456789";
pub(crate) const TEST_SECRET: &str = "mocksecret0123456789";

/// `API-TIMESTAMP` が現在時刻からこれ以上離れていると ERR-5008/5009 を返す。
pub(crate) const TIMESTAMP_WINDOW: Duration = Duration::from_secs(60);

pub(crate) struct MockExchange {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    accept_loop: tokio::task::JoinHandle<()>,
}

/// モックが知っているすべて。ロックは 1 つで、帳簿付けの間だけ保持し、
/// ソケット I/O をまたいで保持することはない。
#[derive(Default)]
struct State {
    routes: Vec<Route>,
    requests: Vec<Recorded>,
    /// どのルートにも一致しなかったリクエストの添字 (`requests` の中)。
    unmatched: Vec<usize>,
    tokens: TokenStore,
    ws: ws::Hub,
    /// hyper に渡した TCP 接続の数。接続プールの検証に使う。
    http_connections: usize,
    /// 今開いている HTTP 接続の数。
    http_open: usize,
    /// 同時に開いていた HTTP 接続の最大数。
    http_peak: usize,
}

struct Route {
    method: &'static str,
    path: &'static str,
    query: Option<String>,
    json_body: Option<serde_json::Value>,
    signed: bool,
    reply: Reply,
    /// `None`: 何度でも。`Some(n)`: ちょうど n 回。`assert_done` が検査する。
    times: Option<usize>,
    hits: usize,
}

/// 定型の応答。
#[derive(Clone, Debug)]
pub(crate) enum Reply {
    /// `tests/fixtures/` のファイルを HTTP 200 で送る。
    Fixture(&'static str),
    /// リテラルの本文を HTTP 200 で送る (取引所はエラーでも通常 200 を返す)。
    Json(String),
    /// 別の状態コード。たとえば HTML 本文の 404 や 503。
    Status { code: u16, body: String },
    /// リテラルの本文を HTTP 200 と `Connection: close` で送り、接続を閉じる。
    /// プールされた接続がサーバ側で閉じられた状況を作る。
    JsonThenClose(String),
    /// 同じ本文を `Transfer-Encoding: chunked` で、このサイズのチャンクに分けて送る。
    Chunked { body: String, chunk: usize },
    /// リクエストを読み切ってから、答えずに閉じる (結果不明)。認証に失敗した
    /// リクエストには、閉じる代わりに ERR-50xx が返る。
    CloseAfterRequest,
    /// リクエストを読み切り、署名の検査結果を記録してから、認証の結果に
    /// かかわらず答えずに閉じる (結果不明)。ネットワークが落ちた状況。
    DropConnection,
    /// 答える前に待つ。タイムアウトを試すため。
    Delay(Duration, Box<Reply>),
    /// 決して答えない。クライアント側のタイムアウトを試すため。
    Stall,
}

/// 受信したリクエスト 1 つ。
#[derive(Clone, Debug)]
pub(crate) struct Recorded {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body: Vec<u8>,
    pub signature: SignatureCheck,
    pub at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignatureCheck {
    /// 認証ヘッダがない (Public 呼び出し)。
    Absent,
    Valid,
    /// あるが誤っている。モックは ERR-50xx を返した。
    Invalid,
}

/// Private WebSocket のトークン。発行・延長・削除を記録し、取引所の規則
/// (最大 5 個、超えたら有効期限が最も近いものを削除) を守る。
#[derive(Default)]
pub(crate) struct TokenStore {
    live: Vec<(String, Instant)>,
    pub issued: Vec<String>,
    pub extended: Vec<String>,
    pub deleted: Vec<String>,
    /// 発行上限超過で追い出されたトークン。
    pub evicted: Vec<String>,
    counter: u64,
}

impl TokenStore {
    pub(crate) const MAX_LIVE: usize = 5;
    pub(crate) const LIFETIME: Duration = Duration::from_hours(1);

    /// 新しいトークンを発行する。6 個目は有効期限が最も近いものを追い出す。
    pub(crate) fn issue(&mut self) -> String {
        self.counter += 1;
        let token = format!("mocktoken{:04}", self.counter);
        if self.live.len() >= Self::MAX_LIVE {
            let (nearest, _) = self
                .live
                .iter()
                .enumerate()
                .min_by_key(|(_, (_, expiry))| *expiry)
                .expect("non-empty");
            let (evicted, _) = self.live.remove(nearest);
            self.evicted.push(evicted);
        }
        self.live
            .push((token.clone(), Instant::now() + Self::LIFETIME));
        self.issued.push(token.clone());
        token
    }

    /// 有効期限を 60 分にリセットする。生きていなければ `false`。
    pub(crate) fn extend(&mut self, token: &str) -> bool {
        match self.live.iter_mut().find(|(t, _)| t == token) {
            Some((_, expiry)) => {
                *expiry = Instant::now() + Self::LIFETIME;
                self.extended.push(token.to_owned());
                true
            }
            None => false,
        }
    }

    /// 削除する。生きていなければ `false`。
    pub(crate) fn delete(&mut self, token: &str) -> bool {
        let before = self.live.len();
        self.live.retain(|(t, _)| t != token);
        let removed = self.live.len() < before;
        if removed {
            self.deleted.push(token.to_owned());
        }
        removed
    }

    pub(crate) fn is_live(&self, token: &str) -> bool {
        self.live
            .iter()
            .any(|(t, expiry)| t == token && *expiry > Instant::now())
    }

    pub(crate) fn live_tokens(&self) -> Vec<String> {
        self.live.iter().map(|(t, _)| t.clone()).collect()
    }
}

impl MockExchange {
    /// `127.0.0.1:0` に bind し、accept ループを起動する。
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let state = Arc::new(Mutex::new(State::default()));
        let accept_state = Arc::clone(&state);
        let accept_loop = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(serve(socket, Arc::clone(&accept_state)));
            }
        });
        Self {
            addr,
            state,
            accept_loop,
        }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn endpoints(&self) -> Endpoints {
        Endpoints::loopback(self.addr).expect("mock binds loopback")
    }

    /// このモックを指す、既定設定のクライアント。
    pub(crate) fn client(&self) -> Client<Anonymous> {
        Client::new(self.endpoints(), Config::default())
    }

    /// このモックを指す、設定を指定したクライアント。
    pub(crate) fn client_with(&self, config: Config) -> Client<Anonymous> {
        Client::new(self.endpoints(), config)
    }

    pub(crate) fn credentials() -> Credentials {
        Credentials::new(
            ApiKey::new(TEST_KEY.to_owned()).expect("valid key"),
            ApiSecret::new(TEST_SECRET.to_owned()).expect("valid secret"),
        )
    }

    /// `method path` の期待を始める (パスは `/public` や `/private` の
    /// プレフィックスを含む、送られるままの形)。
    pub(crate) fn on(&self, method: &'static str, path: &'static str) -> RouteBuilder<'_> {
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

    pub(crate) fn requests(&self) -> Vec<Recorded> {
        self.lock().requests.clone()
    }

    /// これまでに受け付けた HTTP 接続の数 (WebSocket は含まない)。
    pub(crate) fn http_connections(&self) -> usize {
        self.lock().http_connections
    }

    /// 同時に開いていた HTTP 接続の最大数 (WebSocket は含まない)。
    pub(crate) fn peak_http_connections(&self) -> usize {
        self.lock().http_peak
    }

    pub(crate) fn ws(&self) -> WsControl {
        WsControl::new(Arc::clone(&self.state))
    }

    /// トークン台帳を読む (または書く)。
    pub(crate) fn with_tokens<R>(&self, f: impl FnOnce(&mut TokenStore) -> R) -> R {
        f(&mut self.lock().tokens)
    }

    /// REST を通さずにトークンを発行する。Private WebSocket だけを試すテスト向け。
    pub(crate) fn issue_token(&self) -> String {
        self.with_tokens(TokenStore::issue)
    }

    /// `times(n)` のルートがちょうど n 回呼ばれ、すべてのリクエストがどれかの
    /// ルート (組み込みの `ws-auth` を含む) に一致し、署名が 1 つも無効で
    /// なかったことを確かめる。そうでなければパニックする。
    pub(crate) fn assert_done(&self) {
        let state = self.lock();
        let mut problems = Vec::new();
        for route in &state.routes {
            if let Some(n) = route.times
                && route.hits != n
            {
                problems.push(format!(
                    "{} {} expected {n} hits, got {}",
                    route.method, route.path, route.hits
                ));
            }
        }
        for &i in &state.unmatched {
            let r = &state.requests[i];
            problems.push(format!(
                "unmatched request: {} {}{}",
                r.method,
                r.path,
                r.query
                    .as_deref()
                    .map(|q| format!("?{q}"))
                    .unwrap_or_default()
            ));
        }
        for r in state
            .requests
            .iter()
            .filter(|r| r.signature == SignatureCheck::Invalid)
        {
            problems.push(format!("invalid signature on {} {}", r.method, r.path));
        }
        assert!(
            problems.is_empty(),
            "mock expectations not met:\n  {}",
            problems.join("\n  ")
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for MockExchange {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

/// 1 本の TCP 接続を、WebSocket のアップグレードなら tungstenite に、
/// そうでなければ hyper に渡す。先頭を `peek` で覗くだけなので、どちらの
/// ライブラリも完全なリクエストを自分で読める。
async fn serve(socket: tokio::net::TcpStream, state: Arc<Mutex<State>>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut head = [0u8; 8192];
    let mut n = 0;
    while Instant::now() < deadline {
        n = match socket.peek(&mut head).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if head[..n].windows(4).any(|w| w == b"\r\n\r\n") || n == head.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let text = String::from_utf8_lossy(&head[..n]).to_ascii_lowercase();
    if text.contains("upgrade: websocket") {
        ws::serve(socket, state).await;
    } else {
        {
            let mut st = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.http_connections += 1;
            st.http_open += 1;
            st.http_peak = st.http_peak.max(st.http_open);
        }
        http::serve(socket, Arc::clone(&state)).await;
        let mut st = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        st.http_open -= 1;
    }
}

pub(crate) struct RouteBuilder<'a> {
    mock: &'a MockExchange,
    route: Route,
}

impl RouteBuilder<'_> {
    /// クエリ文字列そのもの。たとえば `"symbol=BTC"`。
    pub(crate) fn query(mut self, query: &str) -> Self {
        self.route.query = Some(query.to_owned());
        self
    }

    /// リクエストボディはこの JSON と等しくなければならない (値として比較する
    /// のでキーの順序は問わないが、署名は生のバイト列に対して検査される)。
    pub(crate) fn json_body(mut self, body: serde_json::Value) -> Self {
        self.route.json_body = Some(body);
        self
    }

    /// リクエストは有効な署名を持たなければならない。
    pub(crate) fn signed(mut self) -> Self {
        self.route.signed = true;
        self
    }

    pub(crate) fn times(mut self, n: usize) -> Self {
        self.route.times = Some(n);
        self
    }

    pub(crate) fn reply(mut self, reply: Reply) {
        self.route.reply = reply;
        self.mock.lock().routes.push(self.route);
    }

    pub(crate) fn fixture(self, name: &'static str) {
        self.reply(Reply::Fixture(name));
    }
}

/// `tests/fixtures/<name>` を読む。
pub(crate) fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// 取引所自身のエラーの形。`Reply::Json` 向け。
pub(crate) fn api_error(code: &str, text: &str) -> String {
    format!(
        r#"{{"status":1,"messages":[{{"message_code":"{code}","message_string":"{text}"}}],"responsetime":"2026-10-27T11:49:18.425Z"}}"#
    )
}
