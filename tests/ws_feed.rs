//! モックに対する WebSocket フィード: デコード、購読を再生する再接続、
//! ペーシング、Private トークンのライフサイクル。
//!
//! ペーシング (1 秒に 1 コマンド) を検証するのは
//! `subscribe_commands_are_paced_one_per_second` だけで、他のテストは
//! `unpaced_ws_commands_for_loopback` で待ち時間を省く。

mod support;

use std::time::{Duration, Instant};

use gmo_coin_api_client::error::{Phase, TransportErrorKind};
use gmo_coin_api_client::ws::{
    DisconnectReason, PrivateChannel, PrivateEvent, PublicChannel, PublicEvent,
};
use gmo_coin_api_client::{Anonymous, Backoff, Client, Config, Error, Symbol, Timeout};
use support::{Api, MockExchange, fixture};

const WAIT: Duration = Duration::from_secs(5);

/// 再接続のバックオフを数十ミリ秒にし、ペーシングを止めたクライアント。
fn fast_client(mock: &MockExchange, tune: impl FnOnce(&mut Config)) -> Client<Anonymous> {
    let mut config = Config::default();
    let ms = |n| Timeout::new(Duration::from_millis(n)).unwrap();
    config.reconnect = Backoff::new(ms(20), ms(80)).unwrap();
    tune(&mut config);
    let client = mock.client_with(config);
    client.unpaced_ws_commands_for_loopback().unwrap();
    client
}

#[tokio::test]
async fn public_feed_replays_subscriptions_after_a_drop() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let client = mock.client();
    client.unpaced_ws_commands_for_loopback().unwrap();
    let mut feed = client.public_feed();
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
    client.unpaced_ws_commands_for_loopback().unwrap();
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

/// `next` を数ミリ秒ごとに打ち切り続けても、購読は 1 回ずつ送られ、
/// 押し込まれたイベントは 1 つも失われない。
#[tokio::test]
async fn cancelling_next_repeatedly_loses_no_subscription_and_no_event() {
    const PUSHED: usize = 40;
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |_| {}).public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();
    feed.subscribe(PublicChannel::OrderBooks(Symbol::BTC))
        .unwrap();
    feed.subscribe(PublicChannel::Trades {
        symbol: Symbol::BTC,
        taker_only: true,
    })
    .unwrap();

    let pusher = {
        let ws = mock.ws();
        tokio::spawn(async move {
            ws.subscribed(Api::Public, "trades", Some("BTC"), WAIT)
                .await;
            for _ in 0..PUSHED {
                ws.push(Api::Public, &fixture("ws_public_ticker.json"));
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };

    let mut received = 0;
    let deadline = Instant::now() + WAIT;
    while received < PUSHED && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(2), feed.next()).await {
            Ok(Ok(PublicEvent::Ticker(_))) => received += 1,
            Ok(other) => panic!("unexpected {other:?}"),
            Err(_cancelled) => {}
        }
    }
    pusher.await.unwrap();
    assert_eq!(
        received, PUSHED,
        "every pushed event was yielded exactly once"
    );

    let mut channels: Vec<String> = ws
        .commands()
        .iter()
        .map(|(_, _, c)| {
            assert_eq!(c["command"], "subscribe");
            c["channel"].as_str().unwrap().to_owned()
        })
        .collect();
    channels.sort();
    assert_eq!(channels, ["orderbooks", "ticker", "trades"]);
    assert_eq!(
        ws.handshakes(Api::Public),
        1,
        "cancellation never reconnected"
    );
    feed.close().await.unwrap();
}

#[tokio::test]
async fn a_silent_connection_is_dropped_at_the_idle_deadline_and_redialled() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |c| {
        c.timeouts.ws_idle = Timeout::new(Duration::from_millis(200)).unwrap();
    })
    .public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let started = Instant::now();
    match feed.next().await.unwrap() {
        PublicEvent::Disconnected(d) => {
            assert!(matches!(
                &d.reason,
                DisconnectReason::Transport(t) if *t.kind() == TransportErrorKind::Timeout(Phase::WsIdle)
            ));
        }
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(matches!(feed.next().await, Ok(PublicEvent::Reconnected)));
    assert_eq!(ws.handshakes(Api::Public), 2);
    feed.close().await.unwrap();
}

#[tokio::test]
async fn a_server_ping_is_answered_while_next_is_polled() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |_| {}).public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let driver = tokio::spawn(async move {
        let _ = tokio::time::timeout(WAIT, feed.next()).await;
    });
    ws.subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    assert!(ws.ping(Api::Public, WAIT).await, "pong within the wait");
    driver.abort();
}

#[tokio::test]
async fn close_completes_the_handshake_and_leaves_no_open_connection() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |_| {}).public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let first = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    ws.push(Api::Public, &fixture("ws_public_ticker.json"));
    let (feed, event) = first.await.unwrap();
    assert!(matches!(event, Ok(PublicEvent::Ticker(_))));
    assert_eq!(ws.open_connections(Api::Public), 1);

    feed.close().await.expect("an error-free close");

    let deadline = Instant::now() + WAIT;
    while ws.open_connections(Api::Public) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(ws.open_connections(Api::Public), 0);
}

#[tokio::test]
async fn unsubscribe_sends_the_command_and_later_data_for_that_key_is_dropped() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |_| {}).public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();
    feed.subscribe(PublicChannel::Ticker(Symbol::ETH)).unwrap();

    let driver = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_millis(300), feed.next()).await;
        feed
    });
    ws.subscribed(Api::Public, "ticker", Some("ETH"), WAIT)
        .await;
    let mut feed = driver.await.unwrap();

    assert!(feed.unsubscribe(&PublicChannel::Ticker(Symbol::ETH)));
    assert!(!feed.unsubscribe(&PublicChannel::Ticker(Symbol::ETH)));

    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    let deadline = Instant::now() + WAIT;
    loop {
        let unsubscribed = ws.commands().iter().any(|(_, _, c)| {
            c["command"] == "unsubscribe" && c["channel"] == "ticker" && c["symbol"] == "ETH"
        });
        if unsubscribed {
            break;
        }
        assert!(Instant::now() < deadline, "no unsubscribe arrived");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let eth = fixture("ws_public_ticker.json").replace("\"BTC\"", "\"ETH\"");
    ws.push(Api::Public, &eth);
    ws.push(Api::Public, &fixture("ws_public_ticker.json"));
    let (feed, event) = driver.await.unwrap();
    match event.unwrap() {
        PublicEvent::Ticker(t) => assert_eq!(t.symbol, Symbol::BTC, "the ETH ticker was dropped"),
        other => panic!("{other:?}"),
    }
    feed.close().await.unwrap();
}

#[tokio::test]
async fn an_undecodable_message_on_a_subscribed_channel_ends_the_feed() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |_| {}).public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    ws.push(
        Api::Public,
        r#"{"channel":"ticker","symbol":"BTC","last":"not a price"}"#,
    );
    let (mut feed, event) = driver.await.unwrap();
    match event {
        Err(Error::Protocol(p)) => assert!(
            matches!(
                p.kind(),
                gmo_coin_api_client::error::ProtocolErrorKind::WsMessage { .. }
            ),
            "{p:?}"
        ),
        other => panic!("{other:?}"),
    }
    assert!(matches!(feed.next().await, Err(Error::FeedClosed)));
}

#[tokio::test]
async fn maintenance_at_the_handshake_is_reported_and_retried() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    ws.set_maintenance(true);
    let mut feed = fast_client(&mock, |_| {}).public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    match feed.next().await.unwrap() {
        PublicEvent::Disconnected(d) => assert!(matches!(d.reason, DisconnectReason::Maintenance)),
        other => panic!("{other:?}"),
    }
    ws.set_maintenance(false);
    assert!(
        matches!(feed.next().await, Ok(PublicEvent::Reconnected)),
        "a Disconnected is always followed by Reconnected, even before the first connection"
    );
    feed.close().await.unwrap();
}

#[tokio::test]
async fn a_private_token_rejected_at_reconnect_is_reacquired() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client.private_feed();
    feed.subscribe(PrivateChannel::OrderEvents).unwrap();

    let driver = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_millis(300), feed.next()).await;
        feed
    });
    ws.subscribed(Api::Private, "orderEvents", None, WAIT).await;
    let mut feed = driver.await.unwrap();

    let first = mock.with_tokens(|t| {
        let live = t.live_tokens();
        for token in &live {
            t.delete(token);
        }
        live
    });
    assert_eq!(first.len(), 1);
    ws.drop_connections(Api::Private);

    assert!(matches!(
        feed.next().await,
        Ok(PrivateEvent::Disconnected(_))
    ));
    match feed.next().await.unwrap() {
        PrivateEvent::Disconnected(d) => assert!(
            matches!(
                d.reason,
                DisconnectReason::HandshakeRejected { http_status: 401 }
            ),
            "{:?}",
            d.reason
        ),
        other => panic!("{other:?}"),
    }
    assert!(matches!(feed.next().await, Ok(PrivateEvent::Reconnected)));
    feed.close().await.unwrap();

    let issued = mock.with_tokens(|t| t.issued.clone());
    assert_eq!(issued.len(), 2, "re-acquired once, after the 401");
    assert_eq!(
        mock.with_tokens(|t| t.deleted.clone()),
        vec![first[0].clone(), issued[1].clone()],
        "close deleted the second token"
    );
}

/// 書き込みの期限は、flush が終わった後の沈黙には適用されない。コマンドを
/// 1 つ流したあと `ws_write` より長く何も届かなくても、接続は保たれる。
#[tokio::test]
async fn a_flushed_command_followed_by_silence_does_not_trip_the_write_deadline() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |c| {
        c.timeouts.ws_write = Timeout::new(Duration::from_millis(100)).unwrap();
    })
    .public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let quiet = tokio::time::timeout(Duration::from_millis(500), feed.next()).await;
    assert!(
        quiet.is_err(),
        "nothing should be yielded while the server is silent, got {quiet:?}"
    );
    ws.subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    assert_eq!(ws.handshakes(Api::Public), 1, "the connection was kept");
    feed.close().await.unwrap();
}
