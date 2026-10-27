//! Private REST の口座系 9 操作を、モック取引所に対して端から端まで試す。
//! 取引所が観測するリクエスト (メソッド、パス、クエリ、ボディ、署名) と、
//! デコードされた結果を確かめる。

mod support;

use gmo_coin_api_client::error::{ContractViolation, Postcondition, ProtocolErrorKind};
use gmo_coin_api_client::rest::VolumeLimit;
use gmo_coin_api_client::{
    Amount, Authenticated, BusinessDate, BusinessDateRange, Client, CryptoShortfallStatus, Error,
    FeeType, HistoryWindow, MarginCallStatus, Symbol, Timestamp, TransferType,
};
use serde_json::json;
use support::{MockExchange, Reply, SignatureCheck, api_error};

fn authed(mock: &MockExchange) -> Client<Authenticated> {
    mock.client().authenticate(MockExchange::credentials())
}

fn at(text: &str) -> Timestamp {
    text.parse().expect("timestamp")
}

fn day(text: &str) -> BusinessDate {
    BusinessDate::parse(text).expect("business date")
}

#[tokio::test]
async fn margin_decodes_the_official_fixture_and_signs() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/account/margin")
        .signed()
        .times(1)
        .fixture("private_get_account_margin.json");

    let margin = authed(&mock).margin().await.expect("margin");

    assert_eq!(margin.available_amount.to_string(), "57262506");
    assert_eq!(margin.margin_ratio.to_string(), "6683.6");
    assert_eq!(margin.margin_call_status, MarginCallStatus::Normal);
    assert_eq!(
        margin.crypto_shortfall_status,
        CryptoShortfallStatus::Normal
    );
    assert_eq!(mock.requests()[0].signature, SignatureCheck::Valid);
    mock.assert_done();
}

#[tokio::test]
async fn assets_decode_one_row_per_symbol() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/account/assets")
        .signed()
        .times(1)
        .fixture("private_get_account_assets.json");

    let assets = authed(&mock).assets().await.expect("assets");

    let symbols: Vec<&str> = assets.iter().map(|a| a.symbol.as_str()).collect();
    assert_eq!(symbols, ["JPY", "BTC"]);
    assert_eq!(assets[1].amount.to_string(), "4.0002");
    assert_eq!(assets[1].conversion_rate.to_string(), "859614");
    mock.assert_done();
}

#[tokio::test]
async fn trading_volume_tells_the_two_limit_shapes_apart() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/account/tradingVolume")
        .signed()
        .times(1)
        .fixture("private_get_account_tradingVolume.json");

    let volume = authed(&mock).trading_volume().await.expect("volume");

    assert_eq!(volume.tier_level, 1);
    assert_eq!(volume.limit.len(), 2);
    assert!(matches!(
        &volume.limit[0],
        VolumeLimit::Leverage { symbol, today_limit_open_size, .. }
            if symbol.as_str() == "BTC/JPY" && today_limit_open_size.to_string() == "10000"
    ));
    assert!(matches!(
        &volume.limit[1],
        VolumeLimit::Spot { maker_fee, today_limit_sell_size, .. }
            if maker_fee.to_string() == "-0.0007" && today_limit_sell_size.to_string() == "102"
    ));
    mock.assert_done();
}

#[tokio::test]
async fn a_limit_entry_with_both_shapes_is_a_protocol_error() {
    let mock = MockExchange::start().await;
    let ambiguous = r#"{"status":0,"data":{"jpyVolume":"1","tierLevel":1,"limit":[
        {"symbol":"BTC","todayLimitOpenSize":"1","todayLimitBuySize":"1","todayLimitSellSize":"1",
         "takerFee":"0","makerFee":"0"}]}}"#;
    mock.on("GET", "/private/v1/account/tradingVolume")
        .reply(Reply::Json(ambiguous.to_owned()));

    let err = authed(&mock).trading_volume().await.expect_err("ambiguous");

    assert!(matches!(
        err,
        Error::Protocol(p) if matches!(p.kind(), ProtocolErrorKind::Shape { .. })
    ));
}

#[tokio::test]
async fn fiat_histories_send_the_window_and_decode() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/account/fiatDeposit/history")
        .signed()
        .query("fromTimestamp=2024-02-28T11:00:00.000Z&toTimestamp=2024-02-28T11:30:00.000Z")
        .times(1)
        .fixture("private_get_account_fiatDeposit_history.json");
    mock.on("GET", "/private/v1/account/fiatWithdrawal/history")
        .signed()
        .query("fromTimestamp=2024-02-28T11:00:00.000Z")
        .times(1)
        .fixture("private_get_account_fiatWithdrawal_history.json");
    let client = authed(&mock);
    let both = HistoryWindow::between(
        at("2024-02-28T11:00:00.000Z"),
        at("2024-02-28T11:30:00.000Z"),
    )
    .unwrap();
    let open_ended = HistoryWindow::starting_at(at("2024-02-28T11:00:00.000Z"));

    let deposits = client.fiat_deposit_history(both).await.expect("deposits");
    let withdrawals = client
        .fiat_withdrawal_history(open_ended)
        .await
        .expect("withdrawals");

    assert_eq!(deposits[0].amount.to_string(), "50000");
    assert_eq!(deposits[0].symbol, Symbol::JPY);
    assert_eq!(withdrawals.len(), 1);
    mock.assert_done();
}

#[tokio::test]
async fn crypto_histories_send_the_symbol_and_window_and_decode() {
    let mock = MockExchange::start().await;
    let query = "symbol=BTC&fromTimestamp=2021-10-05T06:00:00.000Z";
    mock.on("GET", "/private/v1/account/deposit/history")
        .signed()
        .query(query)
        .times(1)
        .fixture("private_get_account_deposit_history.json");
    mock.on("GET", "/private/v1/account/withdrawal/history")
        .signed()
        .query(query)
        .times(1)
        .fixture("private_get_account_withdrawal_history.json");
    let client = authed(&mock);
    let window = HistoryWindow::starting_at(at("2021-10-05T06:00:00.000Z"));

    let deposits = client
        .deposit_history(Symbol::BTC, window)
        .await
        .expect("deposits");
    let withdrawals = client
        .withdrawal_history(Symbol::BTC, window)
        .await
        .expect("withdrawals");

    assert_eq!(deposits[0].tx_hash.as_ref(), "yyy");
    assert_eq!(deposits[0].fee, None, "the sample has no fee");
    assert_eq!(withdrawals[0].amount.to_string(), "0.9503");
    mock.assert_done();
}

#[tokio::test]
async fn exchange_fee_history_pages_by_cursor() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/account/exchangeFee/history")
        .signed()
        .query("fromDate=20260615&toDate=20260617&count=50")
        .times(1)
        .fixture("private_get_account_exchangeFee_history.json");
    mock.on("GET", "/private/v1/account/exchangeFee/history")
        .signed()
        .query("fromDate=20260615&toDate=20260617&count=50&cursor=ab%2Bc%2Fd%3D")
        .times(1)
        .reply(Reply::Json(
            r#"{"status":0,"data":{"pagination":{},"list":[]}}"#.to_owned(),
        ));
    let client = authed(&mock);
    let range = BusinessDateRange::between(day("20260615"), day("20260617")).unwrap();

    let first = client
        .exchange_fee_history(range, 50, None)
        .await
        .expect("first page");
    let last = client
        .exchange_fee_history(range, 50, Some("ab+c/d="))
        .await
        .expect("last page");

    assert_eq!(first.list.len(), 2);
    assert_eq!(first.list[0].fee_type, FeeType::LeverageFee);
    assert_eq!(first.list[0].delivery_amount.to_string(), "-120");
    assert_eq!(first.last_finalized_date, Some(day("20260615")));
    assert!(first.next_cursor().unwrap().starts_with("AbzejT4z"));
    assert_eq!(last.next_cursor(), None);
    mock.assert_done();
}

#[tokio::test]
async fn exchange_fee_history_accepts_a_page_without_pagination() {
    let mock = MockExchange::start().await;
    mock.on("GET", "/private/v1/account/exchangeFee/history")
        .query("fromDate=20260615&count=100")
        .times(1)
        .reply(Reply::Json(r#"{"status":0,"data":{"list":[]}}"#.to_owned()));

    let page = authed(&mock)
        .exchange_fee_history(BusinessDateRange::single(day("20260615")), 100, None)
        .await
        .expect("single day");

    assert_eq!(page.next_cursor(), None);
    assert_eq!(page.last_finalized_date, None);
    mock.assert_done();
}

#[tokio::test]
async fn a_page_size_outside_one_to_one_hundred_never_leaves_the_client() {
    let mock = MockExchange::start().await;
    let client = authed(&mock);
    let range = BusinessDateRange::single(day("20260615"));

    for count in [0, 101] {
        let err = client
            .exchange_fee_history(range, count, None)
            .await
            .expect_err("count out of range");
        assert!(matches!(
            err,
            Error::Contract(ContractViolation::InvalidValue(_))
        ));
    }

    assert!(mock.requests().is_empty(), "nothing reached the exchange");
}

#[tokio::test]
async fn transfer_sends_the_amount_and_direction_and_returns_the_moved_amount() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/account/transfer")
        .signed()
        .json_body(json!({"amount": "100000", "transferType": "DEPOSIT"}))
        .times(1)
        .fixture("private_post_account_transfer.json");

    let moved = authed(&mock)
        .transfer(Amount::parse("100000").unwrap(), TransferType::Deposit)
        .await
        .expect("transfer");

    assert_eq!(moved, Amount::parse("100000").unwrap());
    assert_eq!(mock.requests()[0].signature, SignatureCheck::Valid);
    mock.assert_done();
}

#[tokio::test]
async fn transfer_requires_exactly_one_entry_in_the_answer() {
    for (data, actual) in [
        ("[]", 0),
        (
            r#"[{"transferredAmount":"1"},{"transferredAmount":"2"}]"#,
            2,
        ),
    ] {
        let mock = MockExchange::start().await;
        mock.on("POST", "/private/v1/account/transfer")
            .reply(Reply::Json(format!(r#"{{"status":0,"data":{data}}}"#)));

        let err = authed(&mock)
            .transfer(Amount::parse("1").unwrap(), TransferType::Withdrawal)
            .await
            .expect_err("not exactly one entry");

        match err {
            Error::Protocol(p) => assert_eq!(
                p.kind(),
                &ProtocolErrorKind::Postcondition(Postcondition::ExpectedOneEntry { actual })
            ),
            other => panic!("expected a postcondition failure, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn an_exchange_refusal_is_a_typed_api_error() {
    let mock = MockExchange::start().await;
    mock.on("POST", "/private/v1/account/transfer")
        .reply(Reply::Json(api_error(
            "ERR-754",
            "Transfer exceeds the transferable amount.",
        )));

    let err = authed(&mock)
        .transfer(Amount::parse("999999999").unwrap(), TransferType::Deposit)
        .await
        .expect_err("over the transferable amount");

    match err {
        Error::Api(api) => {
            assert!(api.has(gmo_coin_api_client::error::ErrorCode::TRANSFER_OVER_TRANSFERABLE))
        }
        other => panic!("expected an API error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_bad_signature_is_what_the_exchange_would_reject() {
    let mock = MockExchange::start().await;
    let wrong = gmo_coin_api_client::Credentials::new(
        gmo_coin_api_client::ApiKey::new("mockkey0123456789".to_owned()).unwrap(),
        gmo_coin_api_client::ApiSecret::new("not-the-mock-secret!!".to_owned()).unwrap(),
    );
    mock.on("GET", "/private/v1/account/margin")
        .fixture("private_get_account_margin.json");

    let err = mock
        .client()
        .authenticate(wrong)
        .margin()
        .await
        .expect_err("signature mismatch");

    match err {
        Error::Api(api) => assert!(api.has(gmo_coin_api_client::error::ErrorCode::BAD_SIGNATURE)),
        other => panic!("expected ERR-5010, got {other:?}"),
    }
    assert_eq!(mock.requests()[0].signature, SignatureCheck::Invalid);
}
