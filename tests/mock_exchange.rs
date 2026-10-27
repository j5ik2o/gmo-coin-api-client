//! モック取引所そのものの振る舞い。WebSocket 側は tokio-tungstenite を
//! クライアントとして直接使って確かめる。フェーズ 3 (WebSocket フィード) は
//! このモックを編集せずに使う前提なので、ここでその契約を固定する。

mod support;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use support::sign::{self, AuthHeaders};
use support::{Api, MockExchange, SignatureCheck, TEST_KEY, TEST_SECRET, TokenStore};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, client_async};

const WAIT: Duration = Duration::from_secs(5);

async fn connect(mock: &MockExchange, path: &str) -> Result<WebSocketStream<TcpStream>, WsError> {
    let tcp = TcpStream::connect(mock.addr()).await.expect("loopback");
    let url = format!("ws://{}{path}", mock.addr());
    client_async(url, tcp).await.map(|(ws, _)| ws)
}

#[tokio::test]
async fn public_feed_records_commands_pushes_text_and_answers_pings() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut client = connect(&mock, "/ws/public/v1")
        .await
        .expect("public upgrade");
    assert_eq!(ws.handshakes(Api::Public), 1);
    assert_eq!(ws.open_connections(Api::Public), 1);

    client
        .send(Message::text(
            r#"{"command":"subscribe","channel":"ticker","symbol":"BTC"}"#,
        ))
        .await
        .unwrap();
    let seen = ws
        .subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    assert_eq!(seen["command"], "subscribe");
    let commands = ws.commands();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].1, Api::Public);

    ws.push(Api::Public, &support::fixture("ws_public_ticker.json"));
    let message = client.next().await.unwrap().unwrap();
    assert!(
        matches!(&message, Message::Text(t) if t.contains("\"channel\":\"ticker\"")),
        "{message:?}"
    );

    let ping_task = tokio::spawn({
        let state = mock.ws();
        async move { state.ping(Api::Public, WAIT).await }
    });
    let ping = client.next().await.unwrap().unwrap();
    assert!(matches!(ping, Message::Ping(_)), "{ping:?}");
    client.flush().await.unwrap();
    assert!(ping_task.await.unwrap(), "the pong came back");

    ws.drop_connections(Api::Public);
    let after_drop = client.next().await;
    assert!(
        matches!(after_drop, None | Some(Err(_))),
        "the mock closed without a close frame: {after_drop:?}"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ws.open_connections(Api::Public), 0);
}

#[tokio::test]
async fn subscribed_waits_for_the_newest_connection_only() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut first = connect(&mock, "/ws/public/v1").await.unwrap();
    first
        .send(Message::text(
            r#"{"command":"subscribe","channel":"trades","symbol":"ETH"}"#,
        ))
        .await
        .unwrap();
    ws.subscribed(Api::Public, "trades", Some("ETH"), WAIT)
        .await;

    let mut second = connect(&mock, "/ws/public/v1").await.unwrap();
    assert_eq!(ws.handshakes(Api::Public), 2);
    let pending = tokio::time::timeout(
        Duration::from_millis(200),
        ws.subscribed(Api::Public, "trades", Some("ETH"), WAIT),
    )
    .await;
    assert!(
        pending.is_err(),
        "the first connection's subscribe does not satisfy a wait on the second"
    );
    second
        .send(Message::text(
            r#"{"command":"subscribe","channel":"trades","symbol":"ETH"}"#,
        ))
        .await
        .unwrap();
    ws.subscribed(Api::Public, "trades", Some("ETH"), WAIT)
        .await;
    assert_eq!(ws.commands().len(), 2);
    assert!(
        ws.commands()[0].0 <= ws.commands()[1].0,
        "arrival times are recorded in order"
    );
}

#[tokio::test]
async fn private_feed_accepts_live_tokens_only() {
    let mock = MockExchange::start().await;
    let token = mock.issue_token();

    let err = connect(&mock, "/ws/private/v1/deadtoken")
        .await
        .expect_err("dead token");
    assert!(
        matches!(&err, WsError::Http(r) if r.status() == 401),
        "{err:?}"
    );

    let mut live = connect(&mock, &format!("/ws/private/v1/{token}"))
        .await
        .expect("live token");
    live.send(Message::text(
        r#"{"command":"subscribe","channel":"executionEvents"}"#,
    ))
    .await
    .unwrap();
    mock.ws()
        .subscribed(Api::Private, "executionEvents", None, WAIT)
        .await;
    mock.ws().push(
        Api::Private,
        &support::fixture("ws_private_executionEvents.json"),
    );
    assert!(matches!(
        live.next().await.unwrap().unwrap(),
        Message::Text(_)
    ));

    mock.with_tokens(|t| assert!(t.delete(&token)));
    let err = connect(&mock, &format!("/ws/private/v1/{token}"))
        .await
        .expect_err("deleted token");
    assert!(
        matches!(&err, WsError::Http(r) if r.status() == 401),
        "{err:?}"
    );

    let err = connect(&mock, "/ws/other").await.expect_err("unknown path");
    assert!(
        matches!(&err, WsError::Http(r) if r.status() == 404),
        "{err:?}"
    );
}

#[tokio::test]
async fn maintenance_rejects_every_upgrade_with_503() {
    let mock = MockExchange::start().await;
    mock.ws().set_maintenance(true);
    let err = connect(&mock, "/ws/public/v1")
        .await
        .expect_err("maintenance");
    assert!(
        matches!(&err, WsError::Http(r) if r.status() == 503),
        "{err:?}"
    );
    mock.ws().set_maintenance(false);
    assert!(connect(&mock, "/ws/public/v1").await.is_ok());
}

#[test]
fn token_store_keeps_at_most_five_and_evicts_the_nearest_expiry() {
    let mut store = TokenStore::default();
    let first = store.issue();
    let others: Vec<String> = (0..4).map(|_| store.issue()).collect();
    assert_eq!(store.live_tokens().len(), 5);
    assert!(
        store.extend(&first),
        "extending moves the first token's expiry to the far end"
    );
    let sixth = store.issue();
    assert_eq!(store.live_tokens().len(), 5);
    assert_eq!(
        store.evicted,
        [others[0].clone()],
        "the nearest expiry goes, not the oldest issued"
    );
    assert!(store.is_live(&first) && store.is_live(&sixth));
    assert!(!store.is_live(&others[0]));
    assert!(!store.extend(&others[0]));
    assert!(!store.delete(&others[0]));
    assert!(store.delete(&sixth));
    assert_eq!(store.deleted, [sixth]);
}

#[tokio::test]
async fn assert_done_reports_unmatched_requests_and_missed_expectations() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .times(2)
        .fixture("public_get_status.json");
    let client = mock.client();
    client.status().await.unwrap();
    let err = client
        .symbols()
        .await
        .expect_err("no expectation for symbols");
    assert!(
        matches!(err, gmo_coin_api_client::Error::Protocol(_)),
        "{err:?}"
    );

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| mock.assert_done()));
    let message = outcome
        .expect_err("assert_done must panic")
        .downcast::<String>()
        .unwrap();
    assert!(message.contains("expected 2 hits, got 1"), "{message}");
    assert!(
        message.contains("unmatched request: GET /public/v1/symbols"),
        "{message}"
    );
}

fn hmac_hex(ts: &str, method: &str, path: &str, body: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, TEST_SECRET.as_bytes());
    let tag = ring::hmac::sign(&key, format!("{ts}{method}{path}{body}").as_bytes());
    tag.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn the_mock_verifies_like_the_exchange() {
    let now = 1_700_000_000_000u64;
    let ts = now.to_string();
    let good = hmac_hex(&ts, "POST", "/v1/order", "{}");
    let headers = AuthHeaders {
        key: Some(TEST_KEY),
        timestamp: Some(&ts),
        sign: Some(&good),
    };
    let check = |h: AuthHeaders<'_>, method: &str, body: &[u8]| {
        sign::check(now, h, method, "/private/v1/order?x=1", body)
    };
    assert_eq!(check(headers, "POST", b"{}"), Ok(()));
    assert_eq!(
        check(AuthHeaders::default(), "POST", b"{}"),
        Err("ERR-5011")
    );
    assert_eq!(
        check(
            AuthHeaders {
                key: Some("other"),
                ..headers
            },
            "POST",
            b"{}"
        ),
        Err("ERR-5012")
    );
    assert_eq!(
        check(
            AuthHeaders {
                timestamp: None,
                ..headers
            },
            "POST",
            b"{}"
        ),
        Err("ERR-5007")
    );
    assert_eq!(
        check(
            AuthHeaders {
                timestamp: Some("abc"),
                ..headers
            },
            "POST",
            b"{}"
        ),
        Err("ERR-5007")
    );
    let late = (now + 61_000).to_string();
    assert_eq!(
        check(
            AuthHeaders {
                timestamp: Some(&late),
                ..headers
            },
            "POST",
            b"{}"
        ),
        Err("ERR-5008")
    );
    let early = (now - 61_000).to_string();
    assert_eq!(
        check(
            AuthHeaders {
                timestamp: Some(&early),
                ..headers
            },
            "POST",
            b"{}"
        ),
        Err("ERR-5009")
    );
    assert_eq!(
        check(
            AuthHeaders {
                sign: None,
                ..headers
            },
            "POST",
            b"{}"
        ),
        Err("ERR-5010")
    );
    assert_eq!(
        check(headers, "POST", b"{ }"),
        Err("ERR-5010"),
        "the raw bytes are signed, not the JSON value"
    );
    assert_eq!(check(headers, "PUT", b"{}"), Err("ERR-5010"));
    assert_eq!(sign::classify(headers, &Ok(())), SignatureCheck::Valid);
    assert_eq!(
        sign::classify(headers, &Err("ERR-5010")),
        SignatureCheck::Invalid
    );
    assert_eq!(
        sign::classify(AuthHeaders::default(), &Err("ERR-5011")),
        SignatureCheck::Absent
    );
}
