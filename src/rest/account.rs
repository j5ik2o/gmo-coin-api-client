//! Private REST: account state, histories, and the JPY transfer.

use serde::Deserialize;

use super::{Call, endpoint};
use crate::client::{Authenticated, Client};
use crate::domain::{
    Amount, BusinessDate, BusinessDateRange, CryptoShortfallStatus, Decimal, FeeType,
    HistoryWindow, MarginCallStatus, Price, Size, Symbol, Timestamp, TransferStatus, TransferType,
};
use crate::error::Result;

/// `GET /account/margin`. Signed values: an account under margin call can
/// show negative amounts.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Margin {
    pub actual_profit_loss: Decimal,
    pub available_amount: Decimal,
    pub available_amount_for_spot: Decimal,
    pub crypto_shortfall_status: CryptoShortfallStatus,
    pub margin: Decimal,
    pub margin_call_status: MarginCallStatus,
    pub margin_ratio: Decimal,
    pub profit_loss: Decimal,
    pub transferable_amount: Decimal,
}

/// `GET /account/assets`, one row per asset symbol (`JPY`, `BTC`, `ZPG`...).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub symbol: Symbol,
    pub amount: Amount,
    pub available: Amount,
    pub conversion_rate: Price,
}

/// `GET /account/tradingVolume`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradingVolume {
    pub jpy_volume: Amount,
    /// 1, 2, or 3. Kept as sent; the tier table is the exchange's.
    pub tier_level: u8,
    pub limit: Vec<VolumeLimit>,
}

/// A `limit` entry. The docs give two shapes, told apart by which size
/// fields are present. Decoding is by presence, not `#[serde(untagged)]`, so
/// an entry with both or neither shape is a `Protocol` error instead of
/// whichever variant happened to match first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VolumeLimit {
    /// Has `todayLimitBuySize` and `todayLimitSellSize`.
    Spot {
        symbol: Symbol,
        today_limit_buy_size: Size,
        today_limit_sell_size: Size,
        taker_fee: Decimal,
        maker_fee: Decimal,
    },
    /// Has `todayLimitOpenSize`. The symbol is spelled `BTC/JPY` here.
    Leverage {
        symbol: Symbol,
        today_limit_open_size: Size,
        taker_fee: Decimal,
        maker_fee: Decimal,
    },
}

impl<'de> Deserialize<'de> for VolumeLimit {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        // TODO deserialize into a struct with every field Option, then:
        // (open: Some, buy: None, sell: None) => Leverage
        // (open: None, buy: Some, sell: Some) => Spot
        // otherwise => error "ambiguous tradingVolume limit entry".
        let _ = d;
        todo!()
    }
}

/// A JPY deposit or withdrawal record.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct FiatTransfer {
    pub amount: Amount,
    pub fee: Amount,
    pub status: TransferStatus,
    pub symbol: Symbol,
    pub timestamp: Timestamp,
}

/// A crypto deposit or withdrawal record.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoTransfer {
    pub address: Box<str>,
    pub amount: Amount,
    /// Documented in the response table, absent from the example.
    pub fee: Option<Amount>,
    pub status: TransferStatus,
    pub symbol: Symbol,
    pub timestamp: Timestamp,
    pub tx_hash: Box<str>,
}

/// One page of `GET /account/exchangeFee/history` (cursor paging).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeFeePage {
    pub list: Vec<ExchangeFee>,
    /// Absent until the account's first daily run has finished.
    pub last_finalized_date: Option<BusinessDate>,
    #[serde(rename = "pagination")]
    next: CursorField,
}

impl ExchangeFeePage {
    /// The continuation token, present only when another page exists. Pass
    /// it back with the same date range.
    pub fn next_cursor(&self) -> Option<&str> {
        self.next.cursor.as_deref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct CursorField {
    cursor: Option<Box<str>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeFee {
    pub symbol: Symbol,
    pub fee_type: FeeType,
    pub delivery_date: BusinessDate,
    pub delivery_datetime: Timestamp,
    /// JPY; refunds positive, charges negative.
    pub delivery_amount: Decimal,
}

impl Client<Authenticated> {
    /// `GET /private/v1/account/margin`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// Signed values as sent; statuses may be `Unknown`.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn margin(&self) -> Result<Margin> {
        self.private(Call::new(endpoint!(GET "/v1/account/margin")))
            .await?
            .data()
    }

    /// `GET /private/v1/account/assets`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// One row per asset symbol, including symbols newer than this crate.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn assets(&self) -> Result<Vec<Asset>> {
        self.private(Call::new(endpoint!(GET "/v1/account/assets")))
            .await?
            .data()
    }

    /// `GET /private/v1/account/tradingVolume`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// Each `limit` entry is exactly one of the two documented shapes.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn trading_volume(&self) -> Result<TradingVolume> {
        self.private(Call::new(endpoint!(GET "/v1/account/tradingVolume")))
            .await?
            .data()
    }

    /// `GET /private/v1/account/fiatDeposit/history`.
    ///
    /// # Preconditions
    /// Carried by [`HistoryWindow`]: at most 30 minutes, `to` after `from`.
    ///
    /// # Postconditions
    /// Records inside the window, as sent.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn fiat_deposit_history(&self, window: HistoryWindow) -> Result<Vec<FiatTransfer>> {
        let _ = window;
        todo!()
    }

    /// `GET /private/v1/account/fiatWithdrawal/history`.
    ///
    /// # Preconditions
    /// Carried by [`HistoryWindow`].
    ///
    /// # Postconditions
    /// Records inside the window, as sent.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn fiat_withdrawal_history(
        &self,
        window: HistoryWindow,
    ) -> Result<Vec<FiatTransfer>> {
        let _ = window;
        todo!()
    }

    /// `GET /private/v1/account/deposit/history?symbol=&fromTimestamp=&toTimestamp=`.
    ///
    /// # Preconditions
    /// Carried by [`HistoryWindow`].
    ///
    /// # Postconditions
    /// Records inside the window, as sent.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn deposit_history(
        &self,
        symbol: Symbol,
        window: HistoryWindow,
    ) -> Result<Vec<CryptoTransfer>> {
        let mut call = Call::new(endpoint!(GET "/v1/account/deposit/history"))
            .query("symbol", symbol)
            .query("fromTimestamp", window.from());
        if let Some(to) = window.to() {
            call = call.query("toTimestamp", to);
        }
        self.private(call).await?.data()
    }

    /// `GET /private/v1/account/withdrawal/history`.
    ///
    /// # Preconditions
    /// Carried by [`HistoryWindow`].
    ///
    /// # Postconditions
    /// Records inside the window, as sent.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn withdrawal_history(
        &self,
        symbol: Symbol,
        window: HistoryWindow,
    ) -> Result<Vec<CryptoTransfer>> {
        let _ = (symbol, window);
        todo!()
    }

    /// `GET /private/v1/account/exchangeFee/history`.
    ///
    /// # Preconditions
    /// Carried by [`BusinessDateRange`] (at most 7 days, `from <= to`) and by
    /// `count` being `1..=100` (`Contract` otherwise). `cursor` must come
    /// from a page fetched with the same `range`.
    ///
    /// # Postconditions
    /// `next_cursor()` is `Some` exactly when the exchange reports another page.
    ///
    /// # Errors
    /// `Api` with ERR-5016 when the exchange times out; the docs say retry
    /// once, then narrow the range.
    pub async fn exchange_fee_history(
        &self,
        range: BusinessDateRange,
        count: u8,
        cursor: Option<&str>,
    ) -> Result<ExchangeFeePage> {
        let _ = (range, count, cursor);
        todo!()
    }

    /// `POST /private/v1/account/transfer`. The exchange allows one transfer
    /// per three minutes.
    ///
    /// # Preconditions
    /// Carried by [`Amount`] (not negative). The three-minute spacing is the
    /// exchange's to enforce; this crate does not throttle.
    ///
    /// # Postconditions
    /// `data` held exactly one entry (`Protocol(Postcondition(ExpectedOneEntry))`
    /// otherwise); its `transferredAmount` is returned.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors); ERR-754 over the transferable amount.
    pub async fn transfer(&self, amount: Amount, direction: TransferType) -> Result<Amount> {
        let _ = (amount, direction);
        todo!()
    }
}
