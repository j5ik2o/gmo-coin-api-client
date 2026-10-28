//! Public REST: 認証情報なし。任意の `Client<A>` で呼べる。

use serde::Deserialize;

use super::{Call, Page, PageRequest, endpoint};
use crate::client::{Access, Client};
use crate::domain::{Decimal, ExchangeStatus, KlineSpan, Price, Side, Size, Symbol, Timestamp};
use crate::error::{Postcondition, ProtocolErrorKind, Result};

/// 銘柄 1 つの最新レート。`GET /public/v1/ticker` と Public WebSocket の `ticker` で同じ形。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Ticker {
    /// 取扱銘柄。
    pub symbol: Symbol,
    /// 現在の売注文の最良気配値。
    pub ask: Price,
    /// 現在の買注文の最良気配値。
    pub bid: Price,
    /// 当日の最高値(最終取引価格)。
    pub high: Price,
    /// 最終取引価格。
    pub last: Price,
    /// 当日の最安値(最終取引価格)。
    pub low: Price,
    /// 24 時間の取引量。
    pub volume: Size,
    /// 約定時のタイムスタンプ。
    pub timestamp: Timestamp,
}

/// 板情報のスナップショット。`asks` は最良気配から昇順、`bids` は最良気配から
/// 降順。送られたままの順序である。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct OrderBook {
    /// 取扱銘柄。
    pub symbol: Symbol,
    /// 売り注文の情報（最良気配から昇順）。
    pub asks: Vec<OrderBookLevel>,
    /// 買い注文の情報（最良気配から降順）。
    pub bids: Vec<OrderBookLevel>,
}

/// 板の 1 気配。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
pub struct OrderBookLevel {
    /// 気配の価格。
    pub price: Price,
    /// 気配の数量。
    pub size: Size,
}

/// `GET /public/v1/trades` の約定 1 件。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Trade {
    /// 約定価格。
    pub price: Price,
    /// 売買区分。
    pub side: Side,
    /// 約定数量。
    pub size: Size,
    /// 約定日時。
    pub timestamp: Timestamp,
}

/// `GET /public/v1/klines` の 1 本。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Kline {
    /// 開始時刻の unix タイムスタンプ(ミリ秒)。
    #[serde(deserialize_with = "Timestamp::deserialize_millis")]
    pub open_time: Timestamp,
    /// 始値。
    pub open: Price,
    /// 高値。
    pub high: Price,
    /// 安値。
    pub low: Price,
    /// 終値。
    pub close: Price,
    /// 取引量。
    pub volume: Size,
}

/// `GET /symbols` の 1 行。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolRule {
    /// 取扱銘柄。
    pub symbol: Symbol,
    /// 最小注文数量/回。
    pub min_order_size: Size,
    /// 最大注文数量/回。
    pub max_order_size: Size,
    /// 最小注文単位/回。
    pub size_step: Size,
    /// 注文価格の呼値。
    pub tick_size: Price,
    /// 符号付き: maker 手数料は負 (リベート) になる。
    pub taker_fee: Decimal,
    /// maker 手数料。
    pub maker_fee: Decimal,
}

impl<A: Access> Client<A> {
    /// `GET /public/v1/status`。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 取引所の現在の状態。未知の状態は `ExchangeStatus::Unknown`。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run() -> gmo_coin_api_client::Result<()> {
    /// let client = gmo_coin_api_client::Client::production();
    /// let status = client.status().await?;
    /// println!("{status}");
    /// # Ok(()) }
    /// ```
    pub async fn status(&self) -> Result<ExchangeStatus> {
        #[derive(Deserialize)]
        struct Data {
            status: ExchangeStatus,
        }
        let call = Call::new(endpoint!(GET "/v1/status"));
        Ok(self.public(call).await?.data::<Data>()?.status)
    }

    /// `GET /public/v1/ticker?symbol=`。銘柄 1 つ。
    ///
    /// # 事前条件
    /// `Symbol` 自身の不変条件以外になし。
    ///
    /// # 事後条件
    /// 答えはちょうど 1 件のティッカーを持ち、その `symbol == symbol`。
    /// そうでなければ `Protocol(Postcondition(WrongSymbol))`。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。取引所が扱わない銘柄には ERR-5207。
    pub async fn ticker(&self, symbol: Symbol) -> Result<Ticker> {
        let call = Call::new(endpoint!(GET "/v1/ticker")).query("symbol", symbol);
        let answer = self.public(call).await?;
        let wrong = answer.protocol(ProtocolErrorKind::Postcondition(Postcondition::WrongSymbol));
        let mut list: Vec<Ticker> = answer.data()?;
        match (list.pop(), list.is_empty()) {
            (Some(t), true) if t.symbol == symbol => Ok(t),
            _ => Err(wrong),
        }
    }

    /// 銘柄なしの `GET /public/v1/ticker`: 全銘柄。ドキュメントは銘柄ごとに
    /// 呼ぶよりこちらを推奨している。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 上場銘柄ごとに 1 件。このクレートより新しい銘柄も含む。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    pub async fn tickers(&self) -> Result<Vec<Ticker>> {
        self.public(Call::new(endpoint!(GET "/v1/ticker")))
            .await?
            .data()
    }

    /// `GET /public/v1/orderbooks?symbol=`。
    ///
    /// # 事前条件
    /// `Symbol` 自身の不変条件以外になし。
    ///
    /// # 事後条件
    /// `book.symbol == symbol` (そうでなければ `Protocol(Postcondition(WrongSymbol))`)。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    pub async fn order_book(&self, symbol: Symbol) -> Result<OrderBook> {
        let call = Call::new(endpoint!(GET "/v1/orderbooks")).query("symbol", symbol);
        let answer = self.public(call).await?;
        let wrong = answer.protocol(ProtocolErrorKind::Postcondition(Postcondition::WrongSymbol));
        let book: OrderBook = answer.data()?;
        if book.symbol == symbol {
            Ok(book)
        } else {
            Err(wrong)
        }
    }

    /// `GET /public/v1/trades?symbol=&page=&count=`。新しい順。
    ///
    /// # 事前条件
    /// [`PageRequest`] が保証する (page >= 1、count 1..=100)。
    ///
    /// # 事後条件
    /// 高々 `page.count()` 件。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    pub async fn trades(&self, symbol: Symbol, page: PageRequest) -> Result<Page<Trade>> {
        let call = Call::new(endpoint!(GET "/v1/trades"))
            .query("symbol", symbol)
            .query("page", page.page())
            .query("count", page.count());
        self.public(call).await?.data()
    }

    /// `GET /public/v1/klines?symbol=&interval=&date=`。古い順。
    /// [`KlineSpan`] が interval と日付形式の組み合わせを保証する。
    ///
    /// # 事前条件
    /// [`KlineSpan`] が保証する。2021-04-15 より前の日 (日中足) や、銘柄の
    /// 取扱開始年より前の年は取引所が拒否する (ERR-5207)。
    ///
    /// # 事後条件
    /// 送られたままの古い順。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    pub async fn klines(&self, symbol: Symbol, span: KlineSpan) -> Result<Vec<Kline>> {
        let (interval, date) = span.query();
        let call = Call::new(endpoint!(GET "/v1/klines"))
            .query("symbol", symbol)
            .query("interval", interval)
            .query("date", date);
        self.public(call).await?.data()
    }

    /// `GET /public/v1/symbols`: 銘柄ごとの数量と呼値の規則。このクレートは
    /// 注文をこれらの規則と照合しない。規則は取引所のもので、変わり得る。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 上場銘柄ごとに 1 件の規則。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    pub async fn symbols(&self) -> Result<Vec<SymbolRule>> {
        self.public(Call::new(endpoint!(GET "/v1/symbols")))
            .await?
            .data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ApiGroup, Error};
    use crate::rest::envelope;

    fn answer(fixture: &str) -> crate::rest::Answer {
        envelope::parse(ApiGroup::PublicRest, "GET /v1/x", 200, fixture.as_bytes()).unwrap()
    }

    #[test]
    fn every_public_fixture_decodes() {
        #[derive(Deserialize)]
        struct Data {
            status: ExchangeStatus,
        }
        let tickers: Vec<Ticker> =
            answer(include_str!("../../tests/fixtures/public_get_ticker.json"))
                .data()
                .unwrap();
        assert_eq!(tickers[0].symbol, Symbol::BTC);
        assert_eq!(tickers[0].volume.to_string(), "194785.8484");

        let book: OrderBook = answer(include_str!(
            "../../tests/fixtures/public_get_orderbooks.json"
        ))
        .data()
        .unwrap();
        assert_eq!(book.asks[0].price, Price::parse("455659").unwrap());

        let trades: Page<Trade> =
            answer(include_str!("../../tests/fixtures/public_get_trades.json"))
                .data()
                .unwrap();
        assert_eq!(trades.items()[0].side, Side::Buy);
        assert_eq!(trades.pagination().unwrap().count, 30);

        let klines: Vec<Kline> =
            answer(include_str!("../../tests/fixtures/public_get_klines.json"))
                .data()
                .unwrap();
        assert_eq!(klines[0].open_time.unix_millis(), 1_618_588_800_000);
        assert_eq!(klines[1].close.to_string(), "6418245");

        let rules: Vec<SymbolRule> =
            answer(include_str!("../../tests/fixtures/public_get_symbols.json"))
                .data()
                .unwrap();
        assert!(rules[0].maker_fee.is_negative());
        assert_eq!(rules[1].symbol, Symbol::BTC_JPY);

        let status: Data = answer(include_str!("../../tests/fixtures/public_get_status.json"))
            .data()
            .unwrap();
        assert_eq!(status.status, ExchangeStatus::Open);
    }

    #[test]
    fn live_captures_decode_including_symbols_newer_than_the_constants() {
        let tickers: Vec<Ticker> = answer(include_str!(
            "../../tests/fixtures/live/public_get_ticker_all.json"
        ))
        .data()
        .unwrap();
        assert!(tickers.iter().any(|t| t.symbol.as_str() == "WILD"));
        let rules: Vec<SymbolRule> = answer(include_str!(
            "../../tests/fixtures/live/public_get_symbols.json"
        ))
        .data()
        .unwrap();
        assert!(rules.len() >= 29);
        let klines: Vec<Kline> = answer(include_str!(
            "../../tests/fixtures/live/public_get_klines_1day.json"
        ))
        .data()
        .unwrap();
        assert!(
            klines.windows(2).all(|w| w[0].open_time < w[1].open_time),
            "oldest first"
        );
    }

    #[test]
    fn a_json_number_where_a_decimal_string_belongs_is_a_shape_error() {
        let err = answer(r#"{"status":0,"data":{"asks":[{"price":455659,"size":"0.1"}],"bids":[],"symbol":"BTC"}}"#)
            .data::<OrderBook>()
            .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(p) if matches!(p.kind(), ProtocolErrorKind::Shape { .. }))
        );
    }
}
