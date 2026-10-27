//! WebSocket feeds against the mock: decoding, reconnect with replayed
//! subscriptions, pacing, and the private token lifecycle.

mod support;

use std::time::Duration;

use gmo_coin_api_client::Symbol;
use gmo_coin_api_client::ws::{PrivateChannel, PrivateEvent, PublicChannel, PublicEvent};
use support::{Api, MockExchange, fixture};

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn public_feed_replays_subscriptions_after_a_drop() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = mock.client().public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let first = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    ws.push(Api::Public, &fixture("ws_public_ticker.json"));
    let (mut feed, event) = first.await.unwrap();
    assert!(matches!(event, Ok(PublicEvent::Ticker(t)) if t.symbol == Symbol::BTC));

    ws.drop_connections(Api::Public);
    assert!(matches!(
        feed.next().await,
        Ok(PublicEvent::Disconnected(_))
    ));
    assert!(matches!(feed.next().await, Ok(PublicEvent::Reconnected)));

    let replay = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    ws.push(Api::Public, &fixture("ws_public_ticker.json"));
    let (feed, event) = replay.await.unwrap();
    assert!(matches!(event, Ok(PublicEvent::Ticker(_))));

    let subscribes = ws
        .commands()
        .iter()
        .filter(|(_, _, c)| c["command"] == "subscribe")
        .count();
    assert_eq!(
        subscribes, 2,
        "one per connection: the duplicate subscribe was a no-op"
    );
    feed.close().await.unwrap();
}

#[tokio::test]
async fn subscribe_commands_are_paced_one_per_second() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = mock.client().public_feed();
    for symbol in [Symbol::BTC, Symbol::ETH, Symbol::XRP] {
        feed.subscribe(PublicChannel::Ticker(symbol)).unwrap();
    }

    let driver = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(4), feed.next()).await;
    });
    ws.subscribed(Api::Public, "ticker", Some("XRP"), WAIT)
        .await;
    driver.abort();

    let times: Vec<_> = ws.commands().iter().map(|(at, _, _)| *at).collect();
    assert_eq!(times.len(), 3);
    assert!(
        times
            .windows(2)
            .all(|w| w[1] - w[0] >= Duration::from_secs(1)),
        "per-IP limit respected"
    );
}

#[tokio::test]
async fn private_feed_reuses_its_token_across_a_reconnect_and_deletes_it_on_close() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let client = mock.client().authenticate(MockExchange::credentials());
    let mut feed = client.private_feed();
    feed.subscribe(PrivateChannel::ExecutionEvents).unwrap();

    let first = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Private, "executionEvents", None, WAIT)
        .await;
    ws.push(Api::Private, &fixture("ws_private_executionEvents.json"));
    let (mut feed, event) = first.await.unwrap();
    assert!(matches!(event, Ok(PrivateEvent::Execution(_))));

    ws.drop_connections(Api::Private);
    assert!(matches!(
        feed.next().await,
        Ok(PrivateEvent::Disconnected(_))
    ));
    assert!(matches!(feed.next().await, Ok(PrivateEvent::Reconnected)));
    feed.close().await.unwrap();

    let auth: Vec<_> = mock
        .requests()
        .into_iter()
        .filter(|r| r.path == "/private/v1/ws-auth")
        .map(|r| r.method)
        .collect();
    assert_eq!(
        auth,
        ["POST", "DELETE"],
        "one token for both connections, deleted on close"
    );
}
