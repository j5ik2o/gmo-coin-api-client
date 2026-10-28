//! モックに対する WebSocket フィード: デコード、購読を再生する再接続、
//! ペーシング、Private トークンのライフサイクル。
//!
//! ペーシング (1 秒に 1 コマンド) を検証するのは
//! `subscribe_commands_are_paced_one_per_second` と
//! `commands_stay_one_second_apart_*` だけで、他のテストは
//! `unpaced_ws_commands_for_loopback` で待ち時間を省く。

#![expect(
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    reason = "テストの補助関数は #[test] 関数ではないため clippy.toml の allow-*-in-tests が届かない。失敗はそのままテストの失敗になる"
)]
#![expect(
    clippy::assert_is_empty,
    reason = "空かどうかを見る意図を is_empty で直接書く。assert_eq! に直すと空配列の型注釈が要り、読みにくくなる"
)]

mod support;

use std::time::{Duration, Instant};

use futures_util::StreamExt;
use gmo_coin_api_client::error::{ErrorCode, Phase, ProtocolErrorKind, TransportErrorKind};
use gmo_coin_api_client::ws::{
    DisconnectReason, PrivateChannel, PrivateEvent, PublicChannel, PublicEvent,
};
use gmo_coin_api_client::{Anonymous, Backoff, Client, Config, Endpoints, Error, Symbol, Timeout};
use support::ws::Api;
use support::{MockExchange, Reply, api_error, fixture};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

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

/// 3 銘柄を subscribe し、1 つ目の subscribe の後にデータイベントで `next`
/// から戻り、`away` の間 `next` を呼ばずに置いてから再開する (使い方の契約
/// である 2 分の範囲内)。モックが記録したコマンドの到着時刻の間隔を返す。
async fn command_gaps_with_the_caller_away(away: Duration) -> Vec<Duration> {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = mock.client().public_feed();
    for symbol in [Symbol::BTC, Symbol::ETH, Symbol::XRP] {
        feed.subscribe(PublicChannel::Ticker(symbol)).unwrap();
    }

    let first = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Public, "ticker", Some("BTC"), WAIT)
        .await;
    ws.push(Api::Public, &fixture("ws_public_ticker.json"));
    let (mut feed, event) = first.await.unwrap();
    assert!(matches!(event, Ok(PublicEvent::Ticker(_))), "{event:?}");

    tokio::time::sleep(away).await;

    let driver = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(4), feed.next()).await;
    });
    ws.subscribed(Api::Public, "ticker", Some("XRP"), WAIT)
        .await;
    driver.abort();

    let times: Vec<_> = ws.commands().iter().map(|(at, _, _)| *at).collect();
    assert_eq!(times.len(), 3);
    times.windows(2).map(|w| w[1] - w[0]).collect()
}

/// 実際の送信同士の間隔を確かめる。呼び出し側が遅れて戻ると、遅れた分だけ
/// 2 つ目のコマンドが遅れ、3 つ目はそこからさらに 1 秒以上後に出る。
/// 下限の 1 秒は、ペーサの間隔 (1.05 秒) から計時のずれの分を引いたもの。
#[tokio::test]
async fn commands_stay_one_second_apart_when_the_caller_returns_1300ms_late() {
    let gaps = command_gaps_with_the_caller_away(Duration::from_millis(1300)).await;
    assert!(
        gaps.iter().all(|g| *g >= Duration::from_secs(1)),
        "per-IP limit violated: {gaps:?}"
    );
}

#[tokio::test]
async fn commands_stay_one_second_apart_when_the_caller_returns_2500ms_late() {
    let gaps = command_gaps_with_the_caller_away(Duration::from_millis(2500)).await;
    assert!(
        gaps.iter().all(|g| *g >= Duration::from_secs(1)),
        "per-IP limit violated: {gaps:?}"
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
    mock.with_tokens(|t| {
        assert_eq!(t.issued.len(), 1);
        assert_eq!(t.deleted, t.issued, "close deleted the one token");
        assert!(t.live_tokens().is_empty());
    });
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
            matches!(p.kind(), ProtocolErrorKind::WsMessage { .. }),
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

/// `next` を数ミリ秒ごとに打ち切りながら、次のイベントまで進める。
async fn next_under_cancellation<C: gmo_coin_api_client::ws::FeedChannel>(
    feed: &mut gmo_coin_api_client::ws::Feed<C>,
) -> gmo_coin_api_client::Result<C::Event> {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Ok(event) = tokio::time::timeout(Duration::from_millis(2), feed.next()).await {
            return event;
        }
        assert!(Instant::now() < deadline, "no event within {WAIT:?}");
    }
}

fn ws_auth_methods(mock: &MockExchange) -> Vec<String> {
    mock.requests()
        .into_iter()
        .filter(|r| r.path == "/private/v1/ws-auth")
        .map(|r| r.method)
        .collect()
}

/// この試行で発行したトークンをハンドシェイクが拒むのは設定の問題である。
/// フィードは削除してから終端エラーで終わり、トークンを発行し続けない。
#[tokio::test]
async fn a_fresh_token_rejected_at_the_handshake_is_deleted_and_ends_the_feed() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    ws.reject_private_handshakes(Some(403));
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client.private_feed();
    feed.subscribe(PrivateChannel::OrderEvents).unwrap();

    match next_under_cancellation(&mut feed).await {
        Err(Error::Protocol(p)) => assert_eq!(
            *p.kind(),
            ProtocolErrorKind::WsHandshake { http_status: 403 },
            "{p:?}"
        ),
        other => panic!("{other:?}"),
    }
    assert!(matches!(feed.next().await, Err(Error::FeedClosed)));

    assert_eq!(ws_auth_methods(&mock), ["POST", "DELETE"]);
    mock.with_tokens(|t| {
        assert_eq!(t.issued.len(), 1);
        assert_eq!(t.deleted, t.issued, "the rejected token was deleted");
        assert!(t.live_tokens().is_empty());
    });
    assert_eq!(ws.handshakes(Api::Private), 0);
    feed.close().await.unwrap();
    assert_eq!(
        ws_auth_methods(&mock),
        ["POST", "DELETE"],
        "close has nothing left to delete"
    );
}

/// 再利用したトークンが拒まれたら 1 度だけ取り直す。取り直したトークンも
/// 拒まれたら、新しいトークンの規則で終わる。発行は 2 回で止まる。
#[tokio::test]
async fn a_reused_token_rejected_at_the_handshake_is_replaced_once_then_the_feed_ends() {
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

    ws.reject_private_handshakes(Some(401));
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
    match feed.next().await {
        Err(Error::Protocol(p)) => assert_eq!(
            *p.kind(),
            ProtocolErrorKind::WsHandshake { http_status: 401 },
            "{p:?}"
        ),
        other => panic!("{other:?}"),
    }
    assert!(matches!(feed.next().await, Err(Error::FeedClosed)));

    let issued = mock.with_tokens(|t| t.issued.clone());
    assert_eq!(issued.len(), 2, "re-acquired exactly once");
    assert_eq!(
        mock.with_tokens(|t| t.deleted.clone()),
        vec![issued[1].clone()],
        "the fresh token was deleted; the dead one is the exchange's to expire"
    );
    assert_eq!(ws_auth_methods(&mock), ["POST", "POST", "DELETE"]);
}

/// `POST ws-auth` の一時的な失敗は `Disconnected` になり、次の試行で
/// 取り直す。ERR-5003 はバックオフの梯子を 2 段先 (4 倍) から始める。
#[tokio::test]
async fn transient_ws_auth_failures_are_retried_and_rate_limit_waits_longer() {
    struct Case {
        reply: Reply,
        check: fn(&DisconnectReason) -> bool,
        retry_in: Duration,
    }
    let cases = [
        Case {
            reply: Reply::Json(api_error("ERR-554", "Server unavailable.")),
            check: |r| matches!(r, DisconnectReason::Api(e) if e.has(ErrorCode::SERVER_UNAVAILABLE)),
            retry_in: Duration::from_millis(20),
        },
        Case {
            reply: Reply::Json(api_error("ERR-5008", "Timestamp too late.")),
            check: |r| matches!(r, DisconnectReason::Api(e) if e.has(ErrorCode::TIMESTAMP_TOO_LATE)),
            retry_in: Duration::from_millis(20),
        },
        Case {
            reply: Reply::Json(api_error("ERR-5003", "Requests are too many.")),
            check: |r| matches!(r, DisconnectReason::Api(e) if e.has(ErrorCode::RATE_LIMIT)),
            retry_in: Duration::from_millis(80),
        },
        Case {
            reply: Reply::Status {
                code: 502,
                body: "<html>bad gateway</html>".to_owned(),
            },
            check: |r| matches!(r, DisconnectReason::Protocol(p) if *p.kind() == ProtocolErrorKind::HttpStatus(502)),
            retry_in: Duration::from_millis(20),
        },
    ];
    for case in cases {
        let mock = MockExchange::start().await;
        let ws = mock.ws();
        mock.on("POST", "/private/v1/ws-auth")
            .times(1)
            .reply(case.reply.clone());
        let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
        let mut feed = client.private_feed();
        feed.subscribe(PrivateChannel::OrderEvents).unwrap();

        match feed.next().await {
            Ok(PrivateEvent::Disconnected(d)) => {
                assert!((case.check)(&d.reason), "{:?}: {:?}", case.reply, d.reason);
                assert_eq!(d.retry_in, case.retry_in, "{:?}", case.reply);
            }
            other => panic!("{:?}: {other:?}", case.reply),
        }
        assert!(
            matches!(feed.next().await, Ok(PrivateEvent::Reconnected)),
            "{:?}: the built-in route issued a token on the second try",
            case.reply
        );
        assert_eq!(ws.handshakes(Api::Private), 1);
        feed.close().await.unwrap();
        mock.assert_done();
    }
}

/// キーの拒否 (ERR-5012) は一時的ではない: フィードは終わり、取り直さない。
#[tokio::test]
async fn a_rejected_api_key_at_ws_auth_ends_the_feed() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/ws-auth")
        .times(1)
        .reply(Reply::Json(api_error("ERR-5012", "API key rejected.")));
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client.private_feed();
    feed.subscribe(PrivateChannel::OrderEvents).unwrap();

    match feed.next().await {
        Err(Error::Api(e)) => assert!(e.has(ErrorCode::API_KEY_REJECTED), "{e:?}"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(feed.next().await, Err(Error::FeedClosed)));
    assert_eq!(ws_auth_methods(&mock), ["POST"]);
    mock.assert_done();
}

/// 45 分を待てないので、スケジュールを縮めて延長が `PUT ws-auth` を
/// 送ることを確かめる。キャンセルを繰り返しても PUT は 1 回だけ。
#[tokio::test]
async fn the_token_is_extended_on_schedule_even_under_cancellation() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client
        .private_feed_with_lease_schedule_for_loopback(
            Duration::from_millis(300),
            Duration::from_millis(50),
            Duration::from_secs(60),
        )
        .unwrap();
    feed.subscribe(PrivateChannel::OrderEvents).unwrap();

    let deadline = Instant::now() + WAIT;
    let mut extended = Vec::new();
    while extended.is_empty() && Instant::now() < deadline {
        let _ = tokio::time::timeout(Duration::from_millis(3), feed.next()).await;
        extended = mock.with_tokens(|t| t.extended.clone());
    }
    let issued = mock.with_tokens(|t| t.issued.clone());
    assert_eq!(issued.len(), 1, "one token for the whole run");
    assert_eq!(extended, issued, "the live token was extended");
    assert_eq!(ws.handshakes(Api::Private), 1, "no reconnect was needed");

    let settle = Instant::now() + Duration::from_millis(100);
    while Instant::now() < settle {
        let _ = tokio::time::timeout(Duration::from_millis(3), feed.next()).await;
    }
    let puts = mock.requests().iter().filter(|r| r.method == "PUT").count();
    assert_eq!(
        puts, 1,
        "a cancelled extend is not retried while it is in flight"
    );
    feed.close().await.unwrap();
    mock.assert_done();
}

/// ソケットが常に読めるときも延長は期限に送られる。モックが先にイベントを
/// 大量に積み、呼び出し側はゆっくり読む: `next` のたびにフレームが待って
/// いるので、タイマーがソケットの後ろに並ぶ実装では PUT が来ない。
#[tokio::test]
async fn the_token_is_extended_while_the_socket_stays_busy() {
    const BACKLOG: usize = 2_000;
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client
        .private_feed_with_lease_schedule_for_loopback(
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_secs(60),
        )
        .unwrap();
    feed.subscribe(PrivateChannel::OrderEvents).unwrap();

    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Private, "orderEvents", None, WAIT).await;
    for _ in 0..BACKLOG {
        ws.push(Api::Private, &fixture("ws_private_orderEvents.json"));
    }
    let (mut feed, event) = driver.await.unwrap();
    assert!(matches!(event, Ok(PrivateEvent::Order(_))), "{event:?}");

    let started = Instant::now();
    let mut orders = 1;
    while started.elapsed() < Duration::from_millis(400) {
        match feed.next().await.unwrap() {
            PrivateEvent::Order(_) => orders += 1,
            other => panic!("{other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(orders < BACKLOG, "the backlog outlasted the run: {orders}");
    let extended = mock.with_tokens(|t| t.extended.clone());
    let issued = mock.with_tokens(|t| t.issued.clone());
    assert_eq!(issued.len(), 1);
    assert!(
        !extended.is_empty() && extended.iter().all(|t| *t == issued[0]),
        "the one token was extended during the burst: {extended:?}"
    );
    assert_eq!(ws.handshakes(Api::Private), 1);
    feed.close().await.unwrap();
}

/// `close` が接続試行の途中で呼ばれても、その試行で発行したトークンは
/// 削除される。
#[tokio::test]
async fn close_during_a_dial_deletes_the_token_issued_by_that_dial() {
    let mock = MockExchange::start().await;
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client.private_feed();
    feed.subscribe(PrivateChannel::OrderEvents).unwrap();

    let _ = tokio::time::timeout(Duration::from_millis(5), feed.next()).await;
    feed.close().await.unwrap();

    mock.with_tokens(|t| {
        assert_eq!(t.issued.len(), 1, "the dial had issued a token");
        assert_eq!(t.deleted, t.issued);
        assert!(t.live_tokens().is_empty());
    });
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

/// 本番で観測した拒否の形 (`tests/fixtures/live/ws_public_error.json`) は
/// `Rejected` になり、本文はサーバが送ったままである。拒否は返答待ちの
/// コマンド (ETH の subscribe) に帰属し、裏付け済みの BTC は流れ続ける。
#[tokio::test]
async fn a_server_rejection_names_the_command_in_flight_and_the_feed_stays_open() {
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
    ws.push(Api::Public, &fixture("ws_public_ticker.json"));
    let (mut feed, event) = driver.await.unwrap();
    assert!(matches!(event, Ok(PublicEvent::Ticker(_))), "{event:?}");

    feed.subscribe(PublicChannel::Ticker(Symbol::ETH)).unwrap();
    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Public, "ticker", Some("ETH"), WAIT)
        .await;
    ws.push(Api::Public, &fixture("live/ws_public_error.json"));
    let (mut feed, event) = driver.await.unwrap();
    match event.unwrap() {
        PublicEvent::Rejected { channel, text } => {
            assert_eq!(channel, Some(PublicChannel::Ticker(Symbol::ETH)));
            assert_eq!(&*text, "ERR-5106 Invalid request parameter. channel");
        }
        other => panic!("{other:?}"),
    }

    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.push(Api::Public, &fixture("ws_public_ticker.json"));
    let (feed, event) = driver.await.unwrap();
    assert!(matches!(event, Ok(PublicEvent::Ticker(_))), "{event:?}");
    feed.close().await.unwrap();
}

/// 拒否された subscribe は望ましい集合から外れる: 再接続しても送り直さず、
/// 呼び出し側が `subscribe` をもう一度呼んだときだけ送り直す。
#[tokio::test]
async fn a_rejected_subscribe_is_dropped_until_the_caller_subscribes_again() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    ws.reject_subscribe(
        Api::Private,
        "orderEvents",
        "ERR-5106 Invalid request parameter. channel",
    );
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client.private_feed();
    feed.subscribe(PrivateChannel::ExecutionEvents).unwrap();

    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Private, "executionEvents", None, WAIT)
        .await;
    ws.push(Api::Private, &fixture("ws_private_executionEvents.json"));
    let (mut feed, event) = driver.await.unwrap();
    assert!(matches!(event, Ok(PrivateEvent::Execution(_))), "{event:?}");

    feed.subscribe(PrivateChannel::OrderEvents).unwrap();
    match next_under_cancellation(&mut feed).await.unwrap() {
        PrivateEvent::Rejected { channel, text } => {
            assert_eq!(channel, Some(PrivateChannel::OrderEvents));
            assert_eq!(&*text, "ERR-5106 Invalid request parameter. channel");
        }
        other => panic!("{other:?}"),
    }

    ws.drop_connections(Api::Private);
    assert!(matches!(
        feed.next().await,
        Ok(PrivateEvent::Disconnected(_))
    ));
    assert!(matches!(feed.next().await, Ok(PrivateEvent::Reconnected)));
    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Private, "executionEvents", None, WAIT)
        .await;
    ws.push(Api::Private, &fixture("ws_private_executionEvents.json"));
    let (mut feed, event) = driver.await.unwrap();
    assert!(matches!(event, Ok(PrivateEvent::Execution(_))), "{event:?}");
    let order_subscribes = |ws: &support::WsControl| {
        ws.commands()
            .iter()
            .filter(|(_, _, c)| c["command"] == "subscribe" && c["channel"] == "orderEvents")
            .count()
    };
    assert_eq!(
        order_subscribes(&ws),
        1,
        "the reconnect did not replay the rejected subscribe"
    );

    feed.subscribe(PrivateChannel::OrderEvents).unwrap();
    match next_under_cancellation(&mut feed).await.unwrap() {
        PrivateEvent::Rejected { channel, .. } => {
            assert_eq!(channel, Some(PrivateChannel::OrderEvents));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        order_subscribes(&ws),
        2,
        "a fresh subscribe call sends the command again"
    );
    feed.close().await.unwrap();
}

/// 拒否を読むまでの呼び出し側の振る舞い。
enum Caller {
    /// `next` で待ち続ける。拒否は subscribe がモックに届いてから
    /// `rejection_after` 後に届く。
    Waiting { rejection_after: Duration },
    /// subscribe がモックに届いたところで `next` を打ち切る (キャンセル)。
    /// 拒否はすぐ届き、呼び出し側は `returns_after` 後に `next` に戻る。
    Away { returns_after: Duration },
}

/// 静かな Private チャネル (`orderEvents`) を subscribe し、`caller` のとおりに
/// 駆動して、サーバが送った形の拒否を読む。返るのは `Rejected` の `channel`
/// と、その後に push した orderEvents が届いたか (購読が集合に残っていれば
/// 届く) と、接続を落として再接続した後までにモックが受けた orderEvents の
/// subscribe の総数 (購読が集合に残っていれば再接続で送り直す)。
async fn rejection_of_a_quiet_subscribe(caller: Caller) -> (Option<PrivateChannel>, bool, usize) {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let client = fast_client(&mock, |_| {}).authenticate(MockExchange::credentials());
    let mut feed = client.private_feed();
    feed.subscribe(PrivateChannel::OrderEvents).unwrap();
    let rejection = fixture("live/ws_public_error.json");

    let event = match caller {
        Caller::Waiting { rejection_after } => {
            let exchange = async {
                ws.subscribed(Api::Private, "orderEvents", None, WAIT).await;
                tokio::time::sleep(rejection_after).await;
                ws.push(Api::Private, &rejection);
            };
            tokio::join!(feed.next(), exchange).0
        }
        Caller::Away { returns_after } => {
            tokio::select! {
                event = feed.next() => panic!("no event before the caller leaves: {event:?}"),
                _ = ws.subscribed(Api::Private, "orderEvents", None, WAIT) => {}
            }
            ws.push(Api::Private, &rejection);
            tokio::time::sleep(returns_after).await;
            feed.next().await
        }
    };
    let channel = match event.unwrap() {
        PrivateEvent::Rejected { channel, text } => {
            assert_eq!(&*text, "ERR-5106 Invalid request parameter. channel");
            channel
        }
        other => panic!("{other:?}"),
    };

    ws.push(Api::Private, &fixture("ws_private_orderEvents.json"));
    let delivered = match tokio::time::timeout(Duration::from_millis(500), feed.next()).await {
        Ok(Ok(PrivateEvent::Order(_))) => true,
        Ok(other) => panic!("{other:?}"),
        Err(_elapsed) => false,
    };

    ws.drop_connections(Api::Private);
    assert!(matches!(
        feed.next().await,
        Ok(PrivateEvent::Disconnected(_))
    ));
    assert!(matches!(feed.next().await, Ok(PrivateEvent::Reconnected)));
    let _ = tokio::time::timeout(Duration::from_millis(500), feed.next()).await;
    let subscribes = ws
        .commands()
        .iter()
        .filter(|(_, _, c)| c["command"] == "subscribe" && c["channel"] == "orderEvents")
        .count();
    feed.close().await.unwrap();
    (channel, delivered, subscribes)
}

/// 取引所は不正なコマンドにすぐ答えるので、`next` で待っている間に書き込みの
/// 直後に届いた拒否はその subscribe のものとみなし、購読は集合から外れる。
#[tokio::test]
async fn a_rejection_right_after_a_quiet_subscribe_is_attributed_to_it() {
    let (channel, delivered, _) = rejection_of_a_quiet_subscribe(Caller::Waiting {
        rejection_after: Duration::ZERO,
    })
    .await;
    assert_eq!(channel, Some(PrivateChannel::OrderEvents));
    assert!(!delivered, "the rejected subscribe left the desired set");
}

/// `next` で待っている間に、書き込みからコマンドの間隔 (1.05 秒) を過ぎても
/// ソケットが空なら、窓は閉じる。その後に届いた拒否はどのコマンドのものとも
/// しない。`channel` は `None` で、静かなチャネルの購読は生き続ける。
#[tokio::test]
async fn a_rejection_after_the_window_names_no_channel_and_drops_nothing() {
    let (channel, delivered, _) = rejection_of_a_quiet_subscribe(Caller::Waiting {
        rejection_after: Duration::from_millis(1300),
    })
    .await;
    assert_eq!(channel, None);
    assert!(delivered, "the subscription stayed in the desired set");
}

/// `next` を呼ばずにいた間に届いた拒否は、窓の長さを過ぎてから読んでも、
/// 窓の中に届いたかもしれないので、その subscribe に帰属する。拒否された
/// subscribe は集合から外れ、再接続でも送り直さない (issue #6)。上の 1.3 秒の
/// テストと待つ時間は同じで、違うのは呼び出し側がソケットを見ていたかどうか
/// だけである。
#[tokio::test]
async fn a_prompt_rejection_read_after_the_caller_was_away_is_attributed_to_it() {
    let (channel, delivered, subscribes) = rejection_of_a_quiet_subscribe(Caller::Away {
        returns_after: Duration::from_millis(1300),
    })
    .await;
    assert_eq!(channel, Some(PrivateChannel::OrderEvents));
    assert!(!delivered, "the rejected subscribe left the desired set");
    assert_eq!(
        subscribes, 1,
        "the reconnect did not replay the rejected subscribe"
    );
}

/// 同じキーを置き換える subscribe は、古い購読のデータでは裏付けられない。
/// 置き換えが拒否されたら、間に古いデータが流れていても拒否は置き換えに
/// 帰属し、置き換えは集合から外れる。
///
/// 順序の作り方: 置き換えを登録し、古い購読の trade を先に push してモックに
/// 譲り、クライアントのソケットに載せてから `next` を呼ぶ。`next` は
/// ペーサの時刻が来ていれば読む前に書く (`poll_socket` の順序) ので、
/// 置き換えはその trade より前に書かれ、trade は置き換えの後、モックの
/// 拒否の前に読まれる。譲りが短すぎた場合に起きるのは偽の合格であって、
/// 偽の失敗ではない。
#[tokio::test]
async fn a_rejected_replacement_is_not_confirmed_by_the_old_subscription_data() {
    let mock = MockExchange::start().await;
    let ws = mock.ws();
    let mut feed = fast_client(&mock, |_| {}).public_feed();
    let plain = PublicChannel::Trades {
        symbol: Symbol::BTC,
        taker_only: false,
    };
    let taker_only = PublicChannel::Trades {
        symbol: Symbol::BTC,
        taker_only: true,
    };
    feed.subscribe(plain.clone()).unwrap();

    let driver = tokio::spawn(async move {
        let event = feed.next().await;
        (feed, event)
    });
    ws.subscribed(Api::Public, "trades", Some("BTC"), WAIT)
        .await;
    ws.push(Api::Public, &fixture("ws_public_trades.json"));
    let (mut feed, event) = driver.await.unwrap();
    assert!(matches!(event, Ok(PublicEvent::Trade(_))), "{event:?}");

    ws.reject_subscribe(
        Api::Public,
        "trades",
        "ERR-5106 Invalid request parameter. option",
    );
    feed.subscribe(taker_only.clone()).unwrap();
    ws.push(Api::Public, &fixture("ws_public_trades.json"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        matches!(feed.next().await, Ok(PublicEvent::Trade(_))),
        "the old subscription's trade is read after the replacement was written"
    );
    match feed.next().await.unwrap() {
        PublicEvent::Rejected { channel, .. } => assert_eq!(channel, Some(taker_only.clone())),
        other => panic!("{other:?}"),
    }

    let trades_subscribes = || {
        ws.commands()
            .iter()
            .filter(|(_, _, c)| c["command"] == "subscribe" && c["channel"] == "trades")
            .count()
    };
    assert_eq!(trades_subscribes(), 2);
    feed.subscribe(taker_only.clone()).unwrap();
    match feed.next().await.unwrap() {
        PublicEvent::Rejected { channel, .. } => assert_eq!(channel, Some(taker_only)),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        trades_subscribes(),
        3,
        "the replacement left the desired set, so subscribing again sends it"
    );
    feed.close().await.unwrap();
}

/// サーバが close フレームを送ったら、tungstenite が積んだ返答の close を
/// 流してから接続を捨てる (RFC 6455 のエコー)。モックは受け取った close を
/// 数える。その後は通常の再接続である。
#[tokio::test]
async fn a_server_close_frame_is_echoed_before_the_connection_is_dropped() {
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
    ws.close_connections(Api::Public);
    let (mut feed, event) = driver.await.unwrap();
    match event.unwrap() {
        PublicEvent::Disconnected(d) => assert!(
            matches!(
                d.reason,
                DisconnectReason::ServerClosed { code: Some(1000) }
            ),
            "{:?}",
            d.reason
        ),
        other => panic!("{other:?}"),
    }
    let deadline = Instant::now() + WAIT;
    while ws.closes_received() == 0 {
        assert!(
            Instant::now() < deadline,
            "the mock never received the close echo"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(matches!(feed.next().await, Ok(PublicEvent::Reconnected)));
    feed.close().await.unwrap();
}

/// close の返答を flush している途中で `next` をキャンセルしても、切断の理由は
/// 失われない。サーバは受信を止めたまま ping を送り続け、クライアントが返す
/// pong で送信を詰まらせてから close (1000) を送る。flush で止まった `next` を
/// 捨てたあとでサーバが受信を再開すると、次の `next` は `ServerClosed` を返す。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_next_while_the_close_reply_is_flushed_keeps_the_close_reason() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (close_sent_tx, close_sent_rx) = oneshot::channel();
    let (drain_tx, drain_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let ping: Vec<u8> = [0x89, 125]
            .into_iter()
            .chain(std::iter::repeat_n(b'p', 125))
            .collect();
        let pings = ping.repeat(1000);
        let tcp = ws.get_mut();
        for _ in 0..200 {
            tcp.write_all(&pings).await.unwrap();
        }
        tcp.write_all(&[0x88, 2, 0x03, 0xE8]).await.unwrap();
        close_sent_tx.send(()).unwrap();
        drain_rx.await.unwrap();
        while let Some(Ok(message)) = ws.next().await {
            if message.is_close() {
                break;
            }
        }
    });

    let mut config = Config::default();
    config.timeouts.ws_write = Timeout::new(Duration::from_secs(120)).unwrap();
    config.timeouts.ws_idle = Timeout::new(Duration::from_secs(120)).unwrap();
    let mut feed = Client::new(Endpoints::loopback(addr).unwrap(), config).public_feed();

    let first = {
        let next = std::pin::pin!(feed.next());
        tokio::select! {
            event = next => Some(event),
            () = async {
                close_sent_rx.await.unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await;
            } => None,
        }
    };
    assert!(
        first.is_none(),
        "next must still be flushing the close reply when it is cancelled: {first:?}"
    );

    drain_tx.send(()).unwrap();
    let second = tokio::time::timeout(Duration::from_secs(20), feed.next())
        .await
        .unwrap();
    match second.unwrap() {
        PublicEvent::Disconnected(d) => assert!(
            matches!(
                d.reason,
                DisconnectReason::ServerClosed { code: Some(1000) }
            ),
            "{:?}",
            d.reason
        ),
        other => panic!("{other:?}"),
    }
    tokio::time::timeout(WAIT, server).await.unwrap().unwrap();
}
