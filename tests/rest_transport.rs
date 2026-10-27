//! 接続プールの再利用を、接続の閉じ方を自分で決められる素の TCP サーバで
//! 試す。モック取引所は hyper のサーバなので、「応答した後に
//! `Connection: close` を付けずに黙って閉じる」という本番のロードバランサの
//! 振る舞いを作れない。

mod support;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use gmo_coin_api_client::rest::{NewOrder, OrderExecution};
use gmo_coin_api_client::{
    Client, ClientOrderId, Config, Endpoints, LimitTimeInForce, OrderId, OrderPrice, OrderSize,
    Side, Symbol,
};
use support::sign::{self, AuthHeaders};
use support::{MockExchange, fixture};

/// サーバが読み取った 1 リクエスト。
struct Received {
    method: String,
    path: String,
    key: Option<String>,
    timestamp: Option<String>,
    sign: Option<String>,
    body: Vec<u8>,
}

impl Received {
    fn signature_is_valid(&self) -> bool {
        let headers = AuthHeaders {
            key: self.key.as_deref(),
            timestamp: self.timestamp.as_deref(),
            sign: self.sign.as_deref(),
        };
        sign::check(
            sign::now_ms(),
            headers,
            &self.method,
            &self.path,
            &self.body,
        )
        .is_ok()
    }
}

fn read_request(stream: &mut TcpStream) -> Received {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut chunk).expect("read request");
        assert!(n > 0, "the client closed before sending a full request");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.lines();
    let request_line = lines.next().expect("request line");
    let mut parts = request_line.split(' ');
    let method = parts.next().expect("method").to_owned();
    let path = parts.next().expect("path").to_owned();
    let header = |name: &str| {
        lines.clone().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
        })
    };
    let content_length: usize = header("content-length")
        .map(|v| v.parse().expect("numeric content-length"))
        .unwrap_or(0);
    let body_start = head_end + 4;
    while buf.len() < body_start + content_length {
        let n = stream.read(&mut chunk).expect("read body");
        assert!(n > 0, "the client closed mid-body");
        buf.extend_from_slice(&chunk[..n]);
    }
    Received {
        key: header("api-key"),
        timestamp: header("api-timestamp"),
        sign: header("api-sign"),
        method,
        path,
        body: buf[body_start..body_start + content_length].to_vec(),
    }
}

fn keep_alive_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// 1 本目の接続には応答した後 `Connection: close` なしで黙って閉じ、
/// 2 本目には普通に応答するサーバ。受け取ったリクエストを順に送る。
fn quiet_closing_server(body: String) -> (SocketAddr, mpsc::Receiver<Received>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let response = keep_alive_response(&body);
        let (mut first, _) = listener.accept().expect("first connection");
        tx.send(read_request(&mut first)).expect("record");
        first.write_all(response.as_bytes()).expect("respond");
        first.flush().expect("flush");
        // クライアントが応答を読み終えてプールに戻した後に閉じる。
        thread::sleep(Duration::from_millis(50));
        drop(first);
        let (mut second, _) = listener.accept().expect("second connection");
        tx.send(read_request(&mut second)).expect("record");
        second.write_all(response.as_bytes()).expect("respond");
        second.flush().expect("flush");
    });
    (addr, rx)
}

fn limit_buy() -> NewOrder {
    NewOrder {
        client_order_id: ClientOrderId::parse("abc123").unwrap(),
        symbol: Symbol::BTC,
        side: Side::Buy,
        size: OrderSize::parse("0.02").unwrap(),
        execution: OrderExecution::Limit {
            price: OrderPrice::parse("1430001").unwrap(),
            time_in_force: LimitTimeInForce::Fas,
            losscut_price: None,
        },
    }
}

/// hyper がリクエストを 1 バイトも書かずに返す、という状況を決定的に作る。
///
/// 条件は「クライアントがプールの接続を取り出して送信を依頼した後、hyper の
/// 接続タスクが次に動く前に、tokio の I/O ドライバがサーバ側の切断を
/// 取り込んでいる」こと。単一スレッドのランタイムで、`event_interval(1)` に
/// よりタスクを 1 つ走らせるごとにドライバを回し、テスト本体を spawn した
/// タスクとして走らせると、次の順になる。
/// 1. テストのタスクがブロックして待つ間にサーバが閉じる。ドライバは回らない
///    ので、接続はまだ生きて見える。
/// 2. テストのタスクがプールの接続で送信を依頼して待ちに入る。
/// 3. ドライバが回り、切断が取り込まれる。
/// 4. 接続タスクが動き、書き込みの前に行うアイドル読み取りで切断を見つけ、
///    依頼を書かずに返す。
#[test]
fn a_post_on_a_pooled_connection_the_server_closed_quietly_is_resent_once_with_a_fresh_signature() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .event_interval(1)
        .build()
        .expect("runtime");
    let body = async {
        let (addr, received) = quiet_closing_server(fixture("private_post_order.json"));
        let client = Client::new(Endpoints::loopback(addr).unwrap(), Config::default())
            .authenticate(MockExchange::credentials());
        let order = limit_buy();

        let first = client.place_order(&order).await.expect("first order");
        thread::sleep(Duration::from_millis(200));
        let second = client
            .place_order(&order)
            .await
            .expect("resent on a fresh connection, not reported as a transport error");

        assert_eq!((first, second), (OrderId::new(637000).unwrap(), first));
        let seen: Vec<Received> = received.try_iter().collect();
        assert_eq!(seen.len(), 2, "one request per connection");
        assert!(
            seen.iter()
                .all(|r| r.method == "POST" && r.path == "/private/v1/order"),
            "the resend is the same call"
        );
        assert!(
            seen.iter().all(Received::signature_is_valid),
            "the resend is signed afresh for its own timestamp"
        );
    };
    runtime
        .block_on(runtime.spawn(body))
        .expect("the test task did not panic");
}
