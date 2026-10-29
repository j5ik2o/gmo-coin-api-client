//! モック取引所に対する REST の振る舞い: 公式サンプルのデコード、署名、
//! 部分成功、契約の検査。

#![expect(
    clippy::expect_used,
    reason = "テストの補助関数は #[test] 関数ではないため clippy.toml の allow-*-in-tests が届かない。失敗はそのままテストの失敗になる"
)]
#![expect(
    clippy::unreadable_literal,
    reason = "公式ドキュメントの応答例から写した id と値をそのまま残す"
)]

mod support;

use std::num::NonZeroU8;
use std::time::Duration;

use gmo_coin_api_client::error::{
    ContractViolation, Phase, Postcondition, ProtocolErrorKind, TransportErrorKind,
};
use gmo_coin_api_client::rest::{NewOrder, OrderExecution};
use gmo_coin_api_client::{
    Batch, ClientOrderId, Config, Error, ExchangeStatus, KlineSpan, LongInterval, OrderId,
    OrderPrice, OrderSize, PageRequest, Price, Primitive, RestBase, Side, Size, Symbol, Timeout,
    Violation,
};
use serde_json::json;
use support::{MockExchange, Reply, SignatureCheck, api_error, fixture};

fn order_ids(ids: &[u64]) -> Vec<OrderId> {
    ids.iter()
        .map(|&n| OrderId::new(n).expect("non-zero"))
        .collect()
}

#[tokio::test]
async fn ticker_decodes_the_official_fixture() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/ticker")
        .query("symbol=BTC")
        .times(1)
        .fixture("public_get_ticker.json");

    let ticker = mock.client().ticker(Symbol::BTC).await.expect("ticker");

    assert_eq!(ticker.last, Price::parse("756662").unwrap());
    assert_eq!(
        ticker.volume.to_string(),
        "194785.8484",
        "exact text survives the round trip"
    );
    mock.assert_done();
}

#[tokio::test]
async fn every_public_endpoint_round_trips_through_the_mock() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .times(1)
        .fixture("public_get_status.json");
    mock.on("GET", "/public/v1/ticker")
        .times(1)
        .fixture("public_get_ticker.json");
    mock.on("GET", "/public/v1/orderbooks")
        .query("symbol=BTC")
        .times(1)
        .fixture("public_get_orderbooks.json");
    mock.on("GET", "/public/v1/trades")
        .query("symbol=BTC&page=1&count=100")
        .times(1)
        .fixture("public_get_trades.json");
    mock.on("GET", "/public/v1/klines")
        .query("symbol=BTC&interval=1day&date=2025")
        .times(1)
        .fixture("public_get_klines.json");
    mock.on("GET", "/public/v1/symbols")
        .times(1)
        .fixture("public_get_symbols.json");
    let client = mock.client();

    assert_eq!(client.status().await.unwrap(), ExchangeStatus::Open);
    assert_eq!(client.tickers().await.unwrap().len(), 1);
    let book = client.order_book(Symbol::BTC).await.unwrap();
    assert_eq!(book.bids[0].size, Size::parse("0.1").unwrap());
    let trades = client
        .trades(Symbol::BTC, PageRequest::FIRST)
        .await
        .unwrap();
    assert_eq!(trades.items().len(), 1);
    assert_eq!(trades.pagination().unwrap().current_page, 1);
    let span = KlineSpan::Year {
        interval: LongInterval::Day1,
        year: 2025,
    };
    let klines = client.klines(Symbol::BTC, span).await.unwrap();
    assert_eq!(klines.len(), 2);
    let rules = client.symbols().await.unwrap();
    assert_eq!(rules[0].maker_fee.to_string(), "-0.0001");
    assert!(
        mock.requests()
            .iter()
            .all(|r| r.signature == SignatureCheck::Absent),
        "public calls carry no auth headers"
    );
    mock.assert_done();
}

#[tokio::test]
async fn pooled_connections_are_reused_across_calls() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .times(3)
        .fixture("public_get_status.json");
    let client = mock.client();
    for _ in 0..3 {
        client.status().await.unwrap();
    }
    assert_eq!(
        mock.http_connections(),
        1,
        "three sequential calls share one pooled connection"
    );
    mock.assert_done();
}

/// 応答を遅らせて 16 件を確実に重ねる。同時に処理するリクエストを 4 件に
/// 絞っているので、開く接続も 4 本を超えず、残りの 12 件はその接続を再利用する。
#[tokio::test]
async fn concurrent_calls_open_at_most_the_inflight_limit_in_connections() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .times(16)
        .reply(Reply::Delay(
            Duration::from_millis(50),
            Box::new(Reply::Fixture("public_get_status.json")),
        ));
    let mut config = Config::default();
    config.limits.max_inflight_requests = NonZeroU8::new(4).unwrap();
    let client = mock.client_with(config);

    let answers = futures_util::future::join_all((0..16).map(|_| client.status())).await;

    assert!(answers.iter().all(Result::is_ok), "every call is answered");
    assert_eq!(
        mock.peak_http_connections(),
        4,
        "only the permitted requests have connections open at once"
    );
    assert_eq!(
        mock.http_connections(),
        4,
        "the calls that waited reuse the four pooled connections"
    );
    mock.assert_done();
}

#[tokio::test]
async fn place_order_signs_the_exact_bytes_it_sends() {
    let mock = MockExchange::start().await;
    let id = ClientOrderId::parse("abc123").unwrap();
    mock.on("POST", "/private/v1/order")
        .signed()
        .json_body(json!({
            "symbol": "BTC_JPY", "side": "BUY", "executionType": "LIMIT",
            "timeInForce": "FAS", "price": "1430001", "losscutPrice": "1300000",
            "size": "0.02", "clientOrderId": "abc123"
        }))
        .times(1)
        .fixture("private_post_order.json");
    let client = mock.client().authenticate(MockExchange::credentials());

    let order_id = client
        .place_order(&NewOrder {
            client_order_id: id,
            symbol: Symbol::BTC_JPY,
            side: Side::Buy,
            size: OrderSize::parse("0.02").unwrap(),
            execution: OrderExecution::Limit {
                price: OrderPrice::parse("1430001").unwrap(),
                time_in_force: gmo_coin_api_client::LimitTimeInForce::Fas,
                losscut_price: Some(OrderPrice::parse("1300000").unwrap()),
            },
        })
        .await
        .expect("order accepted");

    assert_eq!(
        order_id,
        OrderId::new(637000).unwrap(),
        "string \"637000\" decodes as an id"
    );
    assert_eq!(mock.requests()[0].signature, SignatureCheck::Valid);
    mock.assert_done();
}

#[tokio::test]
async fn cancel_orders_returns_partial_success_as_ok() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrders")
        .signed()
        .times(1)
        .fixture("private_post_cancelOrders.json");
    let client = mock.client().authenticate(MockExchange::credentials());
    let sent = Batch::new(order_ids(&[1, 2, 3, 4])).unwrap();

    let outcome = client
        .cancel_orders(&sent)
        .await
        .expect("partial success is not an error");

    assert_eq!(outcome.cancelled(), order_ids(&[3, 4]).as_slice());
    let rejected: Vec<u64> = outcome
        .rejected()
        .iter()
        .map(|r| r.order_id.get())
        .collect();
    assert_eq!(rejected, [1, 2]);
    assert!(outcome.rejected().iter().all(|r| r.message.code.0 == 5122));
}

#[tokio::test]
async fn cancel_orders_postcondition_rejects_an_id_never_sent() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrders")
        .signed()
        .fixture("private_post_cancelOrders.json");
    let client = mock.client().authenticate(MockExchange::credentials());
    let sent = Batch::new(order_ids(&[1, 2, 3])).unwrap();

    let err = client
        .cancel_orders(&sent)
        .await
        .expect_err("id 4 was never sent");

    match err {
        Error::Protocol(p) => assert_eq!(
            p.kind(),
            &ProtocolErrorKind::Postcondition(Postcondition::CancelResultIdNotRequested(
                OrderId::new(4).unwrap()
            ))
        ),
        other => panic!("expected a postcondition failure, got {other:?}"),
    }
}

#[tokio::test]
async fn eleven_ids_are_rejected_before_any_request_exists() {
    let mock = MockExchange::start().await;

    let err = Batch::<OrderId, 10>::new(order_ids(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
        .expect_err("over the limit");

    assert_eq!(err.primitive, Primitive::Batch);
    assert_eq!(
        err.violation,
        Violation::Size {
            min: 1,
            max: 10,
            actual: 11
        }
    );
    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn cancel_before_on_a_buy_is_rejected_at_the_boundary() {
    let mock = MockExchange::start().await;
    let client = mock.client().authenticate(MockExchange::credentials());

    let err = client
        .place_order(&NewOrder {
            client_order_id: ClientOrderId::generate(),
            symbol: Symbol::BTC,
            side: Side::Buy,
            size: OrderSize::parse("0.01").unwrap(),
            execution: OrderExecution::MarketCancelBefore,
        })
        .await
        .expect_err("cancelBefore is sell-only");

    assert!(matches!(
        err,
        Error::Contract(ContractViolation::CancelBeforeRequiresSpotSell)
    ));
    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn rate_limit_is_a_typed_error() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .reply(Reply::Json(api_error("ERR-5003", "Requests are too many.")));

    let err = mock.client().status().await.expect_err("rate limited");

    assert!(err.is_rate_limited());
}

#[tokio::test]
async fn a_five_digit_code_above_u16_is_still_an_api_error() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .reply(Reply::Json(api_error("ERR-70000", "Future error.")));

    let err = mock.client().status().await.expect_err("ERR-70000");

    match err {
        Error::Api(api) => assert_eq!(api.messages()[0].code.0, 70000),
        other => panic!("expected an API error, got {other:?}"),
    }
}

#[tokio::test]
async fn an_error_without_responsetime_still_decodes() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/klines")
        .fixture("error_without_responsetime.json");

    let span = KlineSpan::Year {
        interval: LongInterval::Day1,
        year: 2026,
    };
    let err = mock
        .client()
        .klines(Symbol::BTC, span)
        .await
        .expect_err("ERR-5207");

    match err {
        Error::Api(api) => {
            assert_eq!(api.messages()[0].code.0, 5207);
            assert_eq!(api.response_time(), None);
        }
        other => panic!("expected an API error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_changed_responsetime_format_keeps_success_data_and_error_codes() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status").reply(Reply::Json(
        r#"{"status":0,"data":{"status":"OPEN"},"responsetime":"2026/10/27 11:49:18"}"#.to_owned(),
    ));
    mock.on("GET", "/public/v1/klines").reply(Reply::Json(
        r#"{"status":1,"messages":[{"message_code":"ERR-5207","message_string":"Not found"}],"responsetime":1750000000}"#
            .to_owned(),
    ));

    assert_eq!(
        mock.client()
            .status()
            .await
            .expect("a changed responsetime must not hide the success data"),
        ExchangeStatus::Open
    );

    let span = KlineSpan::Year {
        interval: LongInterval::Day1,
        year: 2026,
    };
    match mock
        .client()
        .klines(Symbol::BTC, span)
        .await
        .expect_err("ERR-5207")
    {
        Error::Api(api) => {
            assert_eq!(api.messages()[0].code.0, 5207);
            assert_eq!(api.response_time(), None);
        }
        other => panic!("a changed responsetime must not hide the error code: {other:?}"),
    }
}

#[tokio::test]
async fn a_ticker_for_another_symbol_fails_the_postcondition() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/ticker")
        .query("symbol=ETH")
        .fixture("public_get_ticker.json");

    let err = mock
        .client()
        .ticker(Symbol::ETH)
        .await
        .expect_err("the fixture answers BTC");

    match err {
        Error::Protocol(p) => assert_eq!(
            p.kind(),
            &ProtocolErrorKind::Postcondition(Postcondition::WrongSymbol)
        ),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_get_survives_an_idle_connection_the_server_closed() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .times(1)
        .reply(Reply::JsonThenClose(fixture("public_get_status.json")));
    mock.on("GET", "/public/v1/status")
        .times(1)
        .fixture("public_get_status.json");
    let client = mock.client();

    assert_eq!(client.status().await.unwrap(), ExchangeStatus::Open);
    assert_eq!(
        client.status().await.unwrap(),
        ExchangeStatus::Open,
        "a fresh connection replaces the closed one"
    );

    assert_eq!(mock.requests().len(), 2);
    mock.assert_done();
}

#[tokio::test]
async fn a_chunked_body_decodes_like_a_fixed_length_one() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/symbols").reply(Reply::Chunked {
        body: fixture("public_get_symbols.json"),
        chunk: 7,
    });

    let rules = mock.client().symbols().await.expect("chunked");

    assert_eq!(rules.len(), 2);
}

#[tokio::test]
async fn a_non_envelope_404_is_a_protocol_error_with_the_status() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status").reply(Reply::Status {
        code: 404,
        body: "<html>not found</html>".to_owned(),
    });

    let err = mock.client().status().await.expect_err("404");

    match &err {
        Error::Protocol(p) => assert_eq!(p.kind(), &ProtocolErrorKind::HttpStatus(404)),
        other => panic!("{other:?}"),
    }
    assert!(
        !err.outcome_unknown(),
        "a GET changes nothing, so there is no outcome to doubt"
    );
    assert!(!err.accepted_but_unreadable());
}

#[tokio::test]
async fn a_stalled_server_times_out_the_request_phase() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status").reply(Reply::Stall);
    let mut config = Config::default();
    config.timeouts.request = Timeout::new(Duration::from_millis(200)).unwrap();

    let err = mock
        .client_with(config)
        .status()
        .await
        .expect_err("stalled");

    match &err {
        Error::Transport(t) => assert_eq!(*t.kind(), TransportErrorKind::Timeout(Phase::Request)),
        other => panic!("{other:?}"),
    }
    assert!(
        err.outcome_unknown(),
        "a GET that was written and never answered is MaybeSent"
    );
}

#[tokio::test]
async fn a_delayed_answer_inside_the_timeout_still_arrives() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status").reply(Reply::Delay(
        Duration::from_millis(100),
        Box::new(Reply::Fixture("public_get_status.json")),
    ));

    assert_eq!(mock.client().status().await.unwrap(), ExchangeStatus::Open);
}

#[tokio::test]
async fn a_body_over_the_limit_is_refused_before_it_is_buffered() {
    let mock = MockExchange::start().await;
    let big = format!(r#"{{"status":0,"data":[],"pad":"{}"}}"#, "x".repeat(4096));
    mock.on("GET", "/public/v1/symbols").reply(Reply::Chunked {
        body: big,
        chunk: 512,
    });
    let mut config = Config::default();
    config.limits.max_response_bytes = gmo_coin_api_client::ByteLimit::new(1024).unwrap();

    let err = mock
        .client_with(config)
        .symbols()
        .await
        .expect_err("too large");

    match &err {
        Error::Transport(t) => assert_eq!(*t.kind(), TransportErrorKind::TooLarge { limit: 1024 }),
        other => panic!("{other:?}"),
    }
}

#[test]
fn plain_http_is_only_for_loopback() {
    assert!(RestBase::parse("http://127.0.0.1:18080/public").is_ok());
    assert!(RestBase::parse("https://api.coin.z.com/public").is_ok());
    for url in [
        "http://10.0.0.1/public",
        "http://localhost:18080/public",
        "http://api.coin.z.com/public",
    ] {
        let err = RestBase::parse(url).expect_err(url);
        assert_eq!(err.primitive, Primitive::BaseUrl, "{url}");
    }
}

/// マルチスレッドのランタイムでは、応答を読み終えた直後に hyper の接続
/// タスクがまだ動いておらず、接続が「準備できていない」ことがある。それでも
/// 閉じていない接続はプールに戻り、次の呼び出しが準備を待って再利用する。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_calls_on_a_multi_thread_runtime_reuse_one_connection() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status")
        .times(50)
        .fixture("public_get_status.json");
    let client = mock.client();
    for _ in 0..50 {
        client.status().await.unwrap();
    }
    assert_eq!(
        mock.http_connections(),
        1,
        "fifty sequential calls share one pooled connection"
    );
    mock.assert_done();
}
