//! Private REST の注文・約定・建玉系の操作を、モック取引所に対して端から端まで
//! 試す。取引所が観測するリクエストと、デコードされた結果、契約違反で何も
//! 送られないこと、事後条件の検査を確かめる。

mod support;

use std::time::Duration;

use gmo_coin_api_client::error::{
    ContractViolation, ErrorCode, Phase, Postcondition, ProtocolErrorKind, TransportErrorKind,
};
use gmo_coin_api_client::rest::{
    BulkCancel, BulkClose, BulkCloseExecution, CloseExecution, ClosePosition, ExecutionLookup,
    NewOrder, OrderExecution, OrderLookup,
};
use gmo_coin_api_client::{
    Authenticated, Batch, CancelType, Client, ClientOrderId, Config, Error, ExecutionId,
    LimitTimeInForce, OrderId, OrderPrice, OrderSize, OrderStatus, PageRequest, PositionId,
    SettleFilter, SettleType, Side, Symbol, Timeout,
};
use serde_json::json;
use support::{MockExchange, Reply, SignatureCheck, api_error};

fn authed(mock: &MockExchange) -> Client<Authenticated> {
    mock.client().authenticate(MockExchange::credentials())
}

fn oid(n: u64) -> OrderId {
    OrderId::new(n).expect("non-zero")
}

fn cid(text: &str) -> ClientOrderId {
    ClientOrderId::parse(text).expect("client order id")
}

fn size(text: &str) -> OrderSize {
    OrderSize::parse(text).expect("order size")
}

fn price(text: &str) -> OrderPrice {
    OrderPrice::parse(text).expect("order price")
}

fn limit_buy(symbol: Symbol) -> NewOrder {
    NewOrder {
        client_order_id: cid("abc123"),
        symbol,
        side: Side::Buy,
        size: size("0.02"),
        execution: OrderExecution::Limit {
            price: price("1430001"),
            time_in_force: LimitTimeInForce::Fas,
            losscut_price: None,
        },
    }
}

fn assert_contract(err: Error, expected: ContractViolation) {
    match err {
        Error::Contract(v) => assert_eq!(v, expected),
        other => panic!("expected a contract violation, got {other:?}"),
    }
}

#[tokio::test]
async fn orders_by_order_ids_decode_the_official_fixture() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/orders")
        .signed()
        .query("orderId=223456789,123456789")
        .times(1)
        .fixture("private_get_orders.json");
    let lookup = OrderLookup::ByOrderIds(Batch::new(vec![oid(223456789), oid(123456789)]).unwrap());

    let orders = authed(&mock).orders(&lookup).await.expect("orders");

    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].client_order_id, Some(cid("abc123")));
    assert_eq!(orders[0].status, OrderStatus::Executed);
    assert_eq!(orders[0].price.unwrap().to_string(), "1430001");
    assert_eq!(orders[1].client_order_id, None);
    assert_eq!(orders[1].cancel_type, Some(CancelType::User));
    assert_eq!(mock.requests()[0].signature, SignatureCheck::Valid);
    mock.assert_done();
}

#[tokio::test]
async fn orders_by_client_order_ids_use_their_own_query_key() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/orders")
        .signed()
        .query("clientOrderId=abc123")
        .times(1)
        .reply(Reply::Json(
            r#"{"status":0,"data":{"list":[{"orderId":223456789,"clientOrderId":"abc123","rootOrderId":223456789,
            "symbol":"BTC_JPY","side":"BUY","orderType":"NORMAL","executionType":"LIMIT","settleType":"OPEN",
            "size":"0.02","executedSize":"0.02","price":"1430001","losscutPrice":"0","status":"EXECUTED",
            "timeInForce":"FAS","timestamp":"2020-10-14T20:18:59.343Z"}]}}"#
                .to_owned(),
        ));
    let lookup = OrderLookup::ByClientOrderIds(Batch::one(cid("abc123")));

    let orders = authed(&mock).orders(&lookup).await.expect("orders");

    assert_eq!(orders.len(), 1);
    mock.assert_done();
}

#[tokio::test]
async fn orders_returns_an_empty_list_for_ids_the_exchange_does_not_know() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/orders")
        .reply(Reply::Json(r#"{"status":0,"data":{}}"#.to_owned()));

    let orders = authed(&mock)
        .orders(&OrderLookup::ByOrderIds(Batch::one(oid(1))))
        .await
        .expect("empty");

    assert!(orders.is_empty());
}

#[tokio::test]
async fn an_object_without_a_list_key_is_a_shape_error_not_an_empty_answer() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/orders").reply(Reply::Json(
        r#"{"status":0,"data":{"items":[]}}"#.to_owned(),
    ));
    mock.on("GET", "/private/v1/activeOrders")
        .reply(Reply::Json(r#"{"status":0,"data":{"foo":1}}"#.to_owned()));
    let client = authed(&mock);

    let orders = client
        .orders(&OrderLookup::ByOrderIds(Batch::one(oid(1))))
        .await
        .expect_err("items is not list");
    let active = client
        .active_orders(Symbol::BTC, PageRequest::FIRST)
        .await
        .expect_err("foo is not a page");

    for err in [orders, active] {
        assert!(
            matches!(&err, Error::Protocol(p) if matches!(p.kind(), ProtocolErrorKind::Shape { .. })),
            "{err:?}"
        );
    }
}

#[tokio::test]
async fn an_order_that_was_not_asked_for_fails_the_postcondition() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/orders")
        .fixture("private_get_orders.json");
    let only_first = OrderLookup::ByOrderIds(Batch::one(oid(223456789)));

    let err = authed(&mock)
        .orders(&only_first)
        .await
        .expect_err("the fixture also holds order 123456789");

    match err {
        Error::Protocol(p) => assert_eq!(
            p.kind(),
            &ProtocolErrorKind::Postcondition(Postcondition::OrderNotRequested(oid(123456789)))
        ),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn an_order_without_a_client_order_id_cannot_answer_a_client_order_id_lookup() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/orders")
        .fixture("private_get_orders.json");
    let lookup = OrderLookup::ByClientOrderIds(Batch::one(cid("abc123")));

    let err = authed(&mock)
        .orders(&lookup)
        .await
        .expect_err("stray order");

    assert!(matches!(
        err,
        Error::Protocol(p) if p.kind()
            == &ProtocolErrorKind::Postcondition(Postcondition::OrderNotRequested(oid(123456789)))
    ));
}

#[tokio::test]
async fn active_orders_send_the_page_and_decode_the_pagination() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/activeOrders")
        .signed()
        .query("symbol=BTC&page=2&count=30")
        .times(1)
        .fixture("private_get_activeOrders.json");

    let page = authed(&mock)
        .active_orders(Symbol::BTC, PageRequest::new(2, 30).unwrap())
        .await
        .expect("active orders");

    assert_eq!(page.items().len(), 1);
    assert_eq!(page.items()[0].status, OrderStatus::Ordered);
    assert_eq!(page.pagination().unwrap().count, 30);
    mock.assert_done();
}

#[tokio::test]
async fn an_empty_active_orders_answer_is_ok_with_no_items() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/activeOrders")
        .reply(Reply::Json(r#"{"status":0,"data":{}}"#.to_owned()));

    let page = authed(&mock)
        .active_orders(Symbol::BTC, PageRequest::FIRST)
        .await
        .expect("empty page");

    assert!(page.items().is_empty());
    assert!(page.pagination().is_none());
}

fn execution_json(
    execution_id: u64,
    order_id: u64,
    client_order_id: Option<&str>,
) -> serde_json::Value {
    let mut execution = json!({
        "executionId": execution_id,
        "orderId": order_id,
        "positionId": 1234567,
        "symbol": "BTC",
        "side": "BUY",
        "settleType": "OPEN",
        "size": "0.02",
        "price": "1900000",
        "lossGain": "0",
        "fee": "223",
        "timestamp": "2020-11-24T21:27:04.764Z"
    });
    if let Some(client_order_id) = client_order_id {
        execution["clientOrderId"] = json!(client_order_id);
    }
    execution
}

fn executions_reply(list: Vec<serde_json::Value>) -> Reply {
    Reply::Json(json!({"status": 0, "data": {"list": list}}).to_string())
}

#[tokio::test]
async fn executions_use_one_query_key_per_lookup_kind() {
    let mock = MockExchange::start().await;
    for query in [
        "orderId=223456789",
        "executionId=92123912,72123911",
        "clientOrderId=abc123",
    ] {
        mock.on("GET", "/private/v1/executions")
            .signed()
            .query(query)
            .times(1)
            .reply(executions_reply(vec![
                execution_json(92123912, 223456789, Some("abc123")),
                execution_json(72123911, 223456789, Some("abc123")),
            ]));
    }
    let client = authed(&mock);
    let ids = Batch::new(vec![
        ExecutionId::new(92123912).unwrap(),
        ExecutionId::new(72123911).unwrap(),
    ])
    .unwrap();

    let by_order = client
        .executions(&ExecutionLookup::ByOrderId(oid(223456789)))
        .await
        .expect("by order id");
    client
        .executions(&ExecutionLookup::ByExecutionIds(ids))
        .await
        .expect("by execution ids");
    client
        .executions(&ExecutionLookup::ByClientOrderId(cid("abc123")))
        .await
        .expect("by client order id");

    assert_eq!(by_order.len(), 2);
    assert_eq!(
        by_order[0].position_id,
        Some(PositionId::new(1234567).unwrap())
    );
    assert_eq!(by_order[0].settle_type, SettleType::Open);
    assert_eq!(by_order[0].fee.to_string(), "223");
    mock.assert_done();
}

#[tokio::test]
async fn an_execution_that_does_not_match_the_lookup_fails_the_postcondition() {
    let stray_id = ExecutionId::new(72123911).unwrap();
    let lookups = [
        ExecutionLookup::ByOrderId(oid(223456789)),
        ExecutionLookup::ByClientOrderId(cid("abc123")),
        ExecutionLookup::ByExecutionIds(Batch::one(ExecutionId::new(92123912).unwrap())),
    ];
    for lookup in lookups {
        let mock = MockExchange::start().await;
        mock.on("GET", "/private/v1/executions")
            .fixture("private_get_executions.json");

        let err = authed(&mock).executions(&lookup).await.expect_err(
            "the fixture holds an execution of another order, without a client order id",
        );

        match err {
            Error::Protocol(p) => assert_eq!(
                p.kind(),
                &ProtocolErrorKind::Postcondition(Postcondition::ExecutionNotRequested(stray_id)),
                "{lookup:?}"
            ),
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test]
async fn latest_executions_send_the_page_and_decode() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/latestExecutions")
        .signed()
        .query("symbol=BTC&page=1&count=100")
        .times(1)
        .fixture("private_get_latestExecutions.json");

    let page = authed(&mock)
        .latest_executions(Symbol::BTC, PageRequest::FIRST)
        .await
        .expect("latest executions");

    assert_eq!(
        page.items()[0].execution_id,
        ExecutionId::new(72123911).unwrap()
    );
    assert_eq!(page.items()[0].price.to_string(), "877404");
    mock.assert_done();
}

#[tokio::test]
async fn open_positions_decode_the_misspelled_ordered_size() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/openPositions")
        .signed()
        .query("symbol=BTC_JPY&page=1&count=100")
        .times(1)
        .fixture("private_get_openPositions.json");

    let page = authed(&mock)
        .open_positions(Symbol::BTC_JPY, PageRequest::FIRST)
        .await
        .expect("open positions");

    let position = &page.items()[0];
    assert_eq!(position.position_id, PositionId::new(1234567).unwrap());
    assert_eq!(position.ordered_size.to_string(), "0");
    assert_eq!(position.leverage.to_string(), "4");
    mock.assert_done();
}

#[tokio::test]
async fn position_summary_works_with_and_without_a_symbol() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/positionSummary")
        .signed()
        .query("symbol=BTC_JPY")
        .times(1)
        .fixture("private_get_positionSummary.json");
    mock.on("GET", "/private/v1/positionSummary")
        .signed()
        .times(1)
        .fixture("private_get_positionSummary.json");
    let client = authed(&mock);

    let one = client
        .position_summary(Some(Symbol::BTC_JPY))
        .await
        .expect("one symbol");
    let all = client.position_summary(None).await.expect("all symbols");

    assert_eq!(one, all);
    assert_eq!(all[0].sum_position_quantity.to_string(), "11.6999");
    assert_eq!(all[0].position_loss_gain.to_string(), "250675");
    assert_eq!(mock.requests()[1].query, None);
    mock.assert_done();
}

#[tokio::test]
async fn leverage_only_reads_reject_a_spot_symbol_before_sending() {
    let mock = MockExchange::start().await;
    let client = authed(&mock);

    let positions = client
        .open_positions(Symbol::BTC, PageRequest::FIRST)
        .await
        .expect_err("spot symbol");
    let summary = client
        .position_summary(Some(Symbol::BTC))
        .await
        .expect_err("spot symbol");

    assert_contract(positions, ContractViolation::RequiresLeverageSymbol);
    assert_contract(summary, ContractViolation::RequiresLeverageSymbol);
    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn place_order_signs_and_sends_each_execution_grammar() {
    let mock = MockExchange::start().await;
    let bodies = [
        json!({"symbol":"BTC","side":"BUY","executionType":"MARKET","size":"0.02","clientOrderId":"abc123"}),
        json!({"symbol":"BTC","side":"SELL","executionType":"MARKET","cancelBefore":true,"size":"0.02","clientOrderId":"abc123"}),
        json!({"symbol":"BTC_JPY","side":"BUY","executionType":"STOP","price":"1500000","losscutPrice":"1300000","size":"0.02","clientOrderId":"abc123"}),
        json!({"symbol":"BTC","side":"BUY","executionType":"LIMIT","timeInForce":"SOK","price":"1430001","size":"0.02","clientOrderId":"abc123"}),
    ];
    for body in bodies {
        mock.on("POST", "/private/v1/order")
            .signed()
            .json_body(body)
            .times(1)
            .fixture("private_post_order.json");
    }
    let client = authed(&mock);
    let order = |symbol, side, execution| NewOrder {
        client_order_id: cid("abc123"),
        symbol,
        side,
        size: size("0.02"),
        execution,
    };

    let placed = [
        order(Symbol::BTC, Side::Buy, OrderExecution::Market),
        order(Symbol::BTC, Side::Sell, OrderExecution::MarketCancelBefore),
        order(
            Symbol::BTC_JPY,
            Side::Buy,
            OrderExecution::Stop {
                price: price("1500000"),
                losscut_price: Some(price("1300000")),
            },
        ),
        order(
            Symbol::BTC,
            Side::Buy,
            OrderExecution::Limit {
                price: price("1430001"),
                time_in_force: LimitTimeInForce::PostOnly,
                losscut_price: None,
            },
        ),
    ];
    for o in &placed {
        assert_eq!(client.place_order(o).await.expect("accepted"), oid(637000));
    }

    assert!(
        mock.requests()
            .iter()
            .all(|r| r.signature == SignatureCheck::Valid)
    );
    mock.assert_done();
}

#[tokio::test]
async fn order_preconditions_the_types_cannot_carry_are_checked_before_sending() {
    let mock = MockExchange::start().await;
    let client = authed(&mock);
    let losscut_on_spot = NewOrder {
        execution: OrderExecution::Limit {
            price: price("1"),
            time_in_force: LimitTimeInForce::Fas,
            losscut_price: Some(price("1")),
        },
        ..limit_buy(Symbol::BTC)
    };
    let cancel_before_on_leverage = NewOrder {
        side: Side::Sell,
        execution: OrderExecution::MarketCancelBefore,
        ..limit_buy(Symbol::BTC_JPY)
    };

    let a = client
        .place_order(&losscut_on_spot)
        .await
        .expect_err("losscut");
    let b = client
        .place_order(&cancel_before_on_leverage)
        .await
        .expect_err("cancelBefore");

    assert_contract(a, ContractViolation::LosscutRequiresLeverageSymbol);
    assert_contract(b, ContractViolation::CancelBeforeRequiresSpotSell);
    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn a_reused_client_order_id_is_a_typed_api_error() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/order")
        .reply(Reply::Json(api_error(
            "ERR-915",
            "The clientOrderId was already used.",
        )));

    let err = authed(&mock)
        .place_order(&limit_buy(Symbol::BTC))
        .await
        .expect_err("ERR-915");

    match &err {
        Error::Api(api) => assert!(api.has(ErrorCode::CLIENT_ORDER_ID_USED)),
        other => panic!("{other:?}"),
    }
    assert!(!err.outcome_unknown(), "the exchange answered");
}

#[tokio::test]
async fn a_dropped_connection_leaves_the_order_outcome_unknown_and_the_resend_keeps_its_id() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/order")
        .times(1)
        .reply(Reply::DropConnection);
    mock.on("POST", "/private/v1/order")
        .signed()
        .times(1)
        .fixture("private_post_order.json");
    let client = authed(&mock);
    let order = limit_buy(Symbol::BTC);

    let first = client.place_order(&order).await.expect_err("dropped");
    assert!(
        first.outcome_unknown(),
        "the request may have been processed"
    );
    let resent = client.place_order(&order).await.expect("resend accepted");

    assert_eq!(resent, oid(637000));
    let requests = mock.requests();
    assert_eq!(requests.len(), 2, "the client did not retry on its own");
    let resent: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(
        resent,
        json!({
            "symbol": "BTC", "side": "BUY", "executionType": "LIMIT", "timeInForce": "FAS",
            "price": "1430001", "size": "0.02", "clientOrderId": "abc123"
        }),
        "the resend carries the same clientOrderId"
    );
    mock.assert_done();
}

#[tokio::test]
async fn a_stalled_exchange_also_leaves_the_order_outcome_unknown() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/order").reply(Reply::Stall);
    let mut config = Config::default();
    config.timeouts.request = Timeout::new(Duration::from_millis(200)).unwrap();

    let err = mock
        .client_with(config)
        .authenticate(MockExchange::credentials())
        .place_order(&limit_buy(Symbol::BTC))
        .await
        .expect_err("stalled");

    match &err {
        Error::Transport(t) => assert_eq!(*t.kind(), TransportErrorKind::Timeout(Phase::Request)),
        other => panic!("{other:?}"),
    }
    assert!(err.outcome_unknown());
}

#[tokio::test]
async fn a_non_numeric_order_id_in_the_answer_is_a_protocol_error() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/order")
        .reply(Reply::Json(r#"{"status":0,"data":"not-an-id"}"#.to_owned()));

    let err = authed(&mock)
        .place_order(&limit_buy(Symbol::BTC))
        .await
        .expect_err("bad id");

    assert!(matches!(
        &err,
        Error::Protocol(p) if matches!(p.kind(), ProtocolErrorKind::Shape { .. })
    ));
    assert!(
        err.accepted_but_unreadable(),
        "status 0 means the order exists; only its id was lost: {err:?}"
    );
    assert!(err.outcome_unknown(), "{err:?}");
}

#[tokio::test]
async fn a_gateway_error_after_a_post_leaves_the_order_outcome_unknown() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/order").reply(Reply::Status {
        code: 502,
        body: "<html>bad gateway</html>".to_owned(),
    });

    let err = authed(&mock)
        .place_order(&limit_buy(Symbol::BTC))
        .await
        .expect_err("502");

    assert!(matches!(
        &err,
        Error::Protocol(p) if *p.kind() == ProtocolErrorKind::HttpStatus(502)
    ));
    assert!(
        err.outcome_unknown(),
        "a gateway answered, the exchange may still have placed the order: {err:?}"
    );
    assert!(
        !err.accepted_but_unreadable(),
        "nothing says it was accepted"
    );
}

#[tokio::test]
async fn a_success_envelope_behind_a_non_2xx_status_is_not_an_accepted_order() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/order").reply(Reply::Status {
        code: 502,
        body: support::fixture("private_post_order.json"),
    });

    let err = authed(&mock)
        .place_order(&limit_buy(Symbol::BTC))
        .await
        .expect_err("502 wraps a stale or cached body");

    assert!(matches!(
        &err,
        Error::Protocol(p) if *p.kind() == ProtocolErrorKind::HttpStatus(502)
    ));
    assert!(err.outcome_unknown(), "{err:?}");
    assert!(!err.accepted_but_unreadable(), "{err:?}");
}

#[tokio::test]
async fn a_success_envelope_without_data_after_a_post_is_accepted_but_unreadable() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/closeOrder")
        .reply(Reply::Json(r#"{"status":0}"#.to_owned()));

    let err = authed(&mock)
        .close_position(&ClosePosition {
            client_order_id: cid("close1"),
            symbol: Symbol::BTC_JPY,
            side: Side::Sell,
            position_id: PositionId::new(1234567).unwrap(),
            size: size("0.1"),
            execution: CloseExecution::Market,
        })
        .await
        .expect_err("no data");

    assert!(matches!(
        &err,
        Error::Protocol(p) if *p.kind() == ProtocolErrorKind::MissingData
    ));
    assert!(err.accepted_but_unreadable(), "{err:?}");
    assert!(err.outcome_unknown(), "{err:?}");
}

#[tokio::test]
async fn change_order_sends_price_and_optional_losscut() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/changeOrder")
        .signed()
        .json_body(json!({"orderId": 123456789, "price": "9100000"}))
        .times(1)
        .fixture("private_post_changeOrder.json");
    mock.on("POST", "/private/v1/changeOrder")
        .signed()
        .json_body(json!({"orderId": 123456789, "price": "9100000", "losscutPrice": "8000000"}))
        .times(1)
        .fixture("private_post_changeOrder.json");
    let client = authed(&mock);

    client
        .change_order(oid(123456789), price("9100000"), None)
        .await
        .expect("changed");
    client
        .change_order(oid(123456789), price("9100000"), Some(price("8000000")))
        .await
        .expect("changed with losscut");

    mock.assert_done();
}

#[tokio::test]
async fn cancel_order_sends_the_order_id() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrder")
        .signed()
        .json_body(json!({"orderId": 123456789}))
        .times(1)
        .fixture("private_post_cancelOrder.json");

    authed(&mock)
        .cancel_order(oid(123456789))
        .await
        .expect("cancel accepted");

    mock.assert_done();
}

#[tokio::test]
async fn cancel_orders_sends_the_ids_and_reports_partial_success() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrders")
        .signed()
        .json_body(json!({"orderIds": [1, 2, 3, 4]}))
        .times(1)
        .fixture("private_post_cancelOrders.json");
    let sent = Batch::new(vec![oid(1), oid(2), oid(3), oid(4)]).unwrap();

    let outcome = authed(&mock).cancel_orders(&sent).await.expect("partial");

    assert_eq!(outcome.cancelled(), [oid(3), oid(4)]);
    assert_eq!(outcome.rejected().len(), 2);
    assert_eq!(
        outcome.rejected()[0].message.code,
        ErrorCode::ORDER_NOT_MODIFIABLE
    );
    assert!(outcome.unaccounted().is_empty());
    mock.assert_done();
}

#[tokio::test]
async fn cancel_orders_reports_an_id_the_exchange_never_answered_for() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrders")
        .reply(Reply::Json(
            r#"{"status":0,"data":{"success":[3],"failed":[]}}"#.to_owned(),
        ));
    let sent = Batch::new(vec![oid(3), oid(5)]).unwrap();

    let outcome = authed(&mock).cancel_orders(&sent).await.expect("ok");

    assert_eq!(outcome.cancelled(), [oid(3)]);
    assert_eq!(outcome.unaccounted(), [oid(5)], "not assumed cancelled");
}

#[tokio::test]
async fn cancel_orders_rejects_an_id_reported_twice() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelOrders")
        .reply(Reply::Json(
            r#"{"status":0,"data":{"success":[3,3],"failed":[]}}"#.to_owned(),
        ));
    let sent = Batch::new(vec![oid(3), oid(5)]).unwrap();

    let err = authed(&mock).cancel_orders(&sent).await.expect_err("twice");

    assert!(matches!(
        &err,
        Error::Protocol(p) if p.kind()
            == &ProtocolErrorKind::Postcondition(Postcondition::CancelResultIdRepeated(oid(3)))
    ));
    assert!(
        err.outcome_unknown(),
        "the exchange accepted the cancel; which ids it took is unknown: {err:?}"
    );
    assert!(err.accepted_but_unreadable(), "{err:?}");
}

#[tokio::test]
async fn cancel_bulk_orders_sends_the_filter_and_returns_the_ids() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelBulkOrder")
        .signed()
        .json_body(json!({"symbols": ["BTC", "BTC_JPY"], "side": "BUY", "settleType": "CLOSE", "desc": true}))
        .times(1)
        .fixture("private_post_cancelBulkOrder.json");
    mock.on("POST", "/private/v1/cancelBulkOrder")
        .signed()
        .json_body(json!({"symbols": ["BTC"]}))
        .times(1)
        .fixture("private_post_cancelBulkOrder.json");
    let client = authed(&mock);

    let narrowed = client
        .cancel_bulk_orders(&BulkCancel {
            symbols: vec![Symbol::BTC, Symbol::BTC_JPY],
            side: Some(Side::Buy),
            settle_type: Some(SettleFilter::Close),
            newest_first: true,
        })
        .await
        .expect("narrowed");
    let plain = client
        .cancel_bulk_orders(&BulkCancel {
            symbols: vec![Symbol::BTC],
            side: None,
            settle_type: None,
            newest_first: false,
        })
        .await
        .expect("plain");

    assert_eq!(narrowed, [oid(637000), oid(637002)]);
    assert_eq!(plain, narrowed);
    mock.assert_done();
}

fn bulk_cancel_filter() -> BulkCancel {
    BulkCancel {
        symbols: vec![Symbol::BTC],
        side: None,
        settle_type: None,
        newest_first: false,
    }
}

async fn bulk_cancel_answering(ids: Vec<u64>) -> Result<Vec<OrderId>, Error> {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/cancelBulkOrder")
        .reply(Reply::Json(json!({"status": 0, "data": ids}).to_string()));
    authed(&mock)
        .cancel_bulk_orders(&bulk_cancel_filter())
        .await
}

#[tokio::test]
async fn cancel_bulk_orders_accepts_exactly_ten_ids() {
    let ids = bulk_cancel_answering((1..=10).collect())
        .await
        .expect("ten ids");

    assert_eq!(ids, (1..=10).map(oid).collect::<Vec<_>>());
}

#[tokio::test]
async fn cancel_bulk_orders_rejects_more_than_ten_ids() {
    let err = bulk_cancel_answering((1..=11).collect())
        .await
        .expect_err("eleven ids");

    assert!(matches!(
        err,
        Error::Protocol(p) if p.kind()
            == &ProtocolErrorKind::Postcondition(Postcondition::BulkCancelTooManyIds { actual: 11 })
    ));
}

#[tokio::test]
async fn cancel_bulk_orders_rejects_a_repeated_id() {
    let err = bulk_cancel_answering(vec![5, 6, 5])
        .await
        .expect_err("id 5 twice");

    assert!(matches!(
        err,
        Error::Protocol(p) if p.kind()
            == &ProtocolErrorKind::Postcondition(Postcondition::BulkCancelIdRepeated(oid(5)))
    ));
}

#[tokio::test]
async fn cancel_bulk_orders_without_symbols_never_leaves_the_client() {
    let mock = MockExchange::start().await;

    let err = authed(&mock)
        .cancel_bulk_orders(&BulkCancel {
            symbols: vec![],
            side: None,
            settle_type: None,
            newest_first: false,
        })
        .await
        .expect_err("no symbols");

    assert!(matches!(
        err,
        Error::Contract(ContractViolation::InvalidValue(_))
    ));
    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn close_position_settles_exactly_one_position() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/closeOrder")
        .signed()
        .json_body(json!({
            "symbol": "BTC_JPY", "side": "SELL", "executionType": "LIMIT", "timeInForce": "FAK",
            "price": "1900000",
            "settlePosition": [{"positionId": 1234567, "size": "0.1"}],
            "clientOrderId": "close1"
        }))
        .times(1)
        .fixture("private_post_closeOrder.json");

    let id = authed(&mock)
        .close_position(&ClosePosition {
            client_order_id: cid("close1"),
            symbol: Symbol::BTC_JPY,
            side: Side::Sell,
            position_id: PositionId::new(1234567).unwrap(),
            size: size("0.1"),
            execution: CloseExecution::Limit {
                price: price("1900000"),
                time_in_force: LimitTimeInForce::Fak,
            },
        })
        .await
        .expect("close accepted");

    assert_eq!(id, oid(637000));
    mock.assert_done();
}

#[tokio::test]
async fn close_bulk_sends_the_size_without_settle_positions() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/closeBulkOrder")
        .signed()
        .json_body(json!({
            "symbol": "BTC_JPY", "side": "SELL", "executionType": "STOP", "price": "1000000",
            "size": "0.5", "clientOrderId": "bulk1"
        }))
        .times(1)
        .fixture("private_post_closeBulkOrder.json");

    let id = authed(&mock)
        .close_bulk(&BulkClose {
            client_order_id: cid("bulk1"),
            symbol: Symbol::BTC_JPY,
            side: Side::Sell,
            size: size("0.5"),
            execution: BulkCloseExecution::Stop {
                price: price("1000000"),
            },
        })
        .await
        .expect("bulk close accepted");

    assert_eq!(id, oid(637000));
    mock.assert_done();
}

#[tokio::test]
async fn closing_a_spot_symbol_is_rejected_before_sending() {
    let mock = MockExchange::start().await;
    let client = authed(&mock);

    let single = client
        .close_position(&ClosePosition {
            client_order_id: cid("c"),
            symbol: Symbol::BTC,
            side: Side::Sell,
            position_id: PositionId::new(1).unwrap(),
            size: size("1"),
            execution: CloseExecution::Market,
        })
        .await
        .expect_err("spot");
    let bulk = client
        .close_bulk(&BulkClose {
            client_order_id: cid("b"),
            symbol: Symbol::ETH,
            side: Side::Sell,
            size: size("1"),
            execution: BulkCloseExecution::Market,
        })
        .await
        .expect_err("spot");

    assert_contract(single, ContractViolation::RequiresLeverageSymbol);
    assert_contract(bulk, ContractViolation::RequiresLeverageSymbol);
    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn change_losscut_price_sends_the_position_and_price() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/changeLosscutPrice")
        .signed()
        .json_body(json!({"positionId": 1234567, "losscutPrice": "7000000"}))
        .times(1)
        .fixture("private_post_changeLosscutPrice.json");

    authed(&mock)
        .change_losscut_price(PositionId::new(1234567).unwrap(), price("7000000"))
        .await
        .expect("changed");

    mock.assert_done();
}
