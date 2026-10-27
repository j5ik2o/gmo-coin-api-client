//! Public REST: no credentials, callable on any `Client<A>`.

use serde::Deserialize;

use super::{Call, Page, PageRequest, endpoint};
use crate::client::{Access, Client};
use crate::domain::{Decimal, ExchangeStatus, KlineSpan, Price, Side, Size, Symbol, Timestamp};
use crate::error::{Postcondition, ProtocolErrorKind, Result};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Ticker {
    pub symbol: Symbol,
    pub ask: Price,
    pub bid: Price,
    pub high: Price,
    pub last: Price,
    pub low: Price,
    /// 24-hour volume.
    pub volume: Size,
    pub timestamp: Timestamp,
}

/// Order book snapshot. `asks` ascending from the best ask, `bids`
/// descending from the best bid, as sent.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct OrderBook {
    pub symbol: Symbol,
    pub asks: Vec<OrderBookLevel>,
    pub bids: Vec<OrderBookLevel>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
pub struct OrderBookLevel {
    pub price: Price,
    pub size: Size,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Trade {
    pub price: Price,
    pub side: Side,
    pub size: Size,
    pub timestamp: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Kline {
    #[serde(deserialize_with = "Timestamp::deserialize_millis")]
    pub open_time: Timestamp,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Size,
}

/// One row of `GET /symbols`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolRule {
    pub symbol: Symbol,
    pub min_order_size: Size,
    pub max_order_size: Size,
    pub size_step: Size,
    pub tick_size: Price,
    /// Signed: maker fees are negative (a rebate).
    pub taker_fee: Decimal,
    pub maker_fee: Decimal,
}

impl<A: Access> Client<A> {
    /// `GET /public/v1/status`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// The exchange's current status; an unrecognized one is `ExchangeStatus::Unknown`.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn status(&self) -> Result<ExchangeStatus> {
        #[derive(Deserialize)]
        struct Data {
            status: ExchangeStatus,
        }
        let call = Call::new(endpoint!(GET "/v1/status"));
        Ok(self.public(call).await?.data::<Data>()?.status)
    }

    /// `GET /public/v1/ticker?symbol=`. One symbol.
    ///
    /// # Preconditions
    /// None beyond `Symbol`'s own invariants.
    ///
    /// # Postconditions
    /// The answer held exactly one ticker and its `symbol == symbol`;
    /// otherwise `Protocol(Postcondition(WrongSymbol))`.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors); ERR-5207 for a symbol the exchange does not list.
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

    /// `GET /public/v1/ticker` without a symbol: every symbol, which the docs
    /// recommend over one call per symbol.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// One entry per listed symbol, including symbols newer than this crate.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn tickers(&self) -> Result<Vec<Ticker>> {
        self.public(Call::new(endpoint!(GET "/v1/ticker")))
            .await?
            .data()
    }

    /// `GET /public/v1/orderbooks?symbol=`.
    ///
    /// # Preconditions
    /// None beyond `Symbol`'s own invariants.
    ///
    /// # Postconditions
    /// `book.symbol == symbol` (`Protocol(Postcondition(WrongSymbol))` otherwise).
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn order_book(&self, symbol: Symbol) -> Result<OrderBook> {
        let _ = symbol;
        todo!()
    }

    /// `GET /public/v1/trades?symbol=&page=&count=`, newest first.
    ///
    /// # Preconditions
    /// Carried by [`PageRequest`] (page >= 1, count 1..=100).
    ///
    /// # Postconditions
    /// At most `page.count()` items.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn trades(&self, symbol: Symbol, page: PageRequest) -> Result<Page<Trade>> {
        let call = Call::new(endpoint!(GET "/v1/trades"))
            .query("symbol", symbol)
            .query("page", page.page())
            .query("count", page.count());
        self.public(call).await?.data()
    }

    /// `GET /public/v1/klines?symbol=&interval=&date=`, oldest first.
    /// [`KlineSpan`] carries the interval/date-format pairing.
    ///
    /// # Preconditions
    /// Carried by [`KlineSpan`]. Dates before 2021-04-15 (intraday) or before the symbol's listing year are the exchange's to reject (ERR-5207).
    ///
    /// # Postconditions
    /// Oldest first, as sent.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn klines(&self, symbol: Symbol, span: KlineSpan) -> Result<Vec<Kline>> {
        let (interval, date) = span.query();
        let call = Call::new(endpoint!(GET "/v1/klines"))
            .query("symbol", symbol)
            .query("interval", interval)
            .query("date", date);
        self.public(call).await?.data()
    }

    /// `GET /public/v1/symbols`: size and tick rules per symbol. The crate
    /// does not check orders against these rules; the exchange owns them and
    /// they change (see DESIGN.md, Shape).
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// One rule per listed symbol.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn symbols(&self) -> Result<Vec<SymbolRule>> {
        self.public(Call::new(endpoint!(GET "/v1/symbols")))
            .await?
            .data()
    }
}
