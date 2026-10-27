//! REST behaviour against the mock exchange: decoding the official
//! fixtures, signing, partial success, and contract checks.

mod support;

use gmo_coin_api_client::error::{ContractViolation, Postcondition, ProtocolErrorKind};
use gmo_coin_api_client::rest::{NewOrder, OrderExecution};
use gmo_coin_api_client::{
    Batch, ClientOrderId, Error, OrderId, Price, Primitive, RestBase, Side, Size, Symbol, Violation,
};
use serde_json::json;
use support::{MockExchange, Reply, SignatureCheck, api_error};

fn order_ids(ids: &[u64]) -> Vec<OrderId> {
    ids.iter().map(|&n| OrderId::new(n).expect("non-zero")).collect()
}

#[tokio::test]
async fn ticker_decodes_the_official_fixture() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/ticker").query("symbol=BTC").times(1).fixture("public_get_ticker.json");

    let ticker = mock.client().ticker(Symbol::BTC).await.expect("ticker");

    assert_eq!(ticker.last, Price::parse("756662").unwrap());
    assert_eq!(ticker.volume.to_string(), "194785.8484", "exact text survives the round trip");
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
            size: Size::parse("0.02").unwrap(),
            execution: OrderExecution::Limit {
                price: Price::parse("1430001").unwrap(),
                time_in_force: gmo_coin_api_client::LimitTimeInForce::Fas,
                losscut_price: Some(Price::parse("1300000").unwrap()),
            },
        })
        .await
        .expect("order accepted");

    assert_eq!(order_id, OrderId::new(637000).unwrap(), "string \"637000\" decodes as an id");
    assert_eq!(mock.requests()[0].signature, SignatureCheck::Valid);
    mock.assert_done();
}

#[tokio::test]
async fn cancel_orders_returns_partial_success_as_ok() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrders").signed().times(1).fixture("private_post_cancelOrders.json");
    let client = mock.client().authenticate(MockExchange::credentials());
    let sent = Batch::new(order_ids(&[1, 2, 3, 4])).unwrap();

    let outcome = client.cancel_orders(&sent).await.expect("partial success is not an error");

    assert_eq!(outcome.cancelled(), order_ids(&[3, 4]).as_slice());
    let rejected: Vec<u64> = outcome.rejected().iter().map(|r| r.order_id.get()).collect();
    assert_eq!(rejected, [1, 2]);
    assert!(outcome.rejected().iter().all(|r| r.message.code.0 == 5122));
}

#[tokio::test]
async fn cancel_orders_postcondition_rejects_an_id_never_sent() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrders").signed().fixture("private_post_cancelOrders.json");
    let client = mock.client().authenticate(MockExchange::credentials());
    let sent = Batch::new(order_ids(&[1, 2, 3])).unwrap();

    let err = client.cancel_orders(&sent).await.expect_err("id 4 was never sent");

    match err {
        Error::Protocol(p) => assert_eq!(
            p.kind(),
            &ProtocolErrorKind::Postcondition(Postcondition::CancelResultIdNotRequested(OrderId::new(4).unwrap()))
        ),
        other => panic!("expected a postcondition failure, got {other:?}"),
    }
}

#[tokio::test]
async fn eleven_ids_are_rejected_before_any_request_exists() {
    let mock = MockExchange::start().await;

    let err = Batch::<OrderId, 10>::new(order_ids(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])).expect_err("over the limit");

    assert_eq!(err.primitive, Primitive::Batch);
    assert_eq!(err.violation, Violation::Size { min: 1, max: 10, actual: 11 });
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
            size: Size::parse("0.01").unwrap(),
            execution: OrderExecution::MarketCancelBefore,
        })
        .await
        .expect_err("cancelBefore is sell-only");

    assert!(matches!(err, Error::Contract(ContractViolation::CancelBeforeRequiresSpotSell)));
    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn rate_limit_is_a_typed_error() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/status").reply(Reply::Json(api_error("ERR-5003", "Requests are too many.")));

    let err = mock.client().status().await.expect_err("rate limited");

    assert!(err.is_rate_limited());
}

#[tokio::test]
async fn an_error_without_responsetime_still_decodes() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/public/v1/klines").fixture("error_without_responsetime.json");

    let span = gmo_coin_api_client::KlineSpan::Year {
        interval: gmo_coin_api_client::LongInterval::Day1,
        year: 2026,
    };
    let err = mock.client().klines(Symbol::BTC, span).await.expect_err("ERR-5207");

    match err {
        Error::Api(api) => {
            assert_eq!(api.messages()[0].code.0, 5207);
            assert_eq!(api.response_time(), None);
        }
        other => panic!("expected an API error, got {other:?}"),
    }
}

#[test]
fn plain_http_is_only_for_loopback() {
    assert!(RestBase::parse("http://127.0.0.1:18080/public").is_ok());
    assert!(RestBase::parse("https://api.coin.z.com/public").is_ok());
    for url in ["http://10.0.0.1/public", "http://localhost:18080/public", "http://api.coin.z.com/public"] {
        let err = RestBase::parse(url).expect_err(url);
        assert_eq!(err.primitive, Primitive::BaseUrl, "{url}");
    }
}
