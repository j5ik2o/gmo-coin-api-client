//! Private REST: 口座の状態、入出金・手数料の履歴、JPY の口座振替。
//!
//! すべてのメソッドは `Client<Authenticated>` にだけ存在する。型の上で資格情報
//! なしには呼べない。
//!
//! # パニック
//! このファイルの関数はパニックしない。
//!
//! # 例
//! ```no_run
//! # async fn run() -> gmo_coin_api_client::Result<()> {
//! use gmo_coin_api_client::{Client, Credentials};
//!
//! let client = Client::production().authenticate(Credentials::from_env()?);
//! for asset in client.assets().await? {
//!     println!("{} {}", asset.symbol, asset.amount);
//! }
//! # Ok(()) }
//! ```

use serde::{Deserialize, Serialize};

use super::{Answer, Call, endpoint};
use crate::client::{Authenticated, Client};
use crate::domain::{
    Amount, BusinessDate, BusinessDateRange, CryptoShortfallStatus, Decimal, FeeType,
    HistoryWindow, InvalidValue, MarginCallStatus, Price, Primitive, Size, Symbol, Timestamp,
    TransferStatus, TransferType, Violation,
};
use crate::error::{Postcondition, ProtocolErrorKind, Result};

/// `GET /account/margin`。符号付きの値: 追証状態の口座は負の額を取り得る。
///
/// # 不変条件
/// 各状態 (`crypto_shortfall_status`、`margin_call_status`) は、このクレートより
/// 新しい値を `Unknown` として保持する。
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

/// `GET /account/assets` の 1 行。資産銘柄ごとに 1 行 (`JPY`、`BTC`、`ZPG`...)。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub symbol: Symbol,
    pub amount: Amount,
    pub available: Amount,
    pub conversion_rate: Price,
}

/// `GET /account/tradingVolume`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradingVolume {
    pub jpy_volume: Amount,
    /// 1、2、3 のいずれか。送られたままで、階層の表は取引所のもの。
    pub tier_level: u8,
    pub limit: Vec<VolumeLimit>,
}

/// `limit` の 1 件。ドキュメントは 2 つの形を示し、どちらの数量フィールドが
/// あるかで見分ける。`#[serde(untagged)]` ではなくフィールドの有無で
/// デコードするので、両方の形を持つ、またはどちらも持たない項目は、たまたま
/// 最初に一致した variant ではなく `Protocol` エラーになる。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VolumeLimit {
    /// `todayLimitBuySize` と `todayLimitSellSize` を持つ。
    Spot {
        symbol: Symbol,
        today_limit_buy_size: Size,
        today_limit_sell_size: Size,
        taker_fee: Decimal,
        maker_fee: Decimal,
    },
    /// `todayLimitOpenSize` を持つ。銘柄はここでは `BTC/JPY` と綴られる。
    Leverage {
        symbol: Symbol,
        today_limit_open_size: Size,
        taker_fee: Decimal,
        maker_fee: Decimal,
    },
}

impl<'de> Deserialize<'de> for VolumeLimit {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Wire {
            symbol: Symbol,
            today_limit_open_size: Option<Size>,
            today_limit_buy_size: Option<Size>,
            today_limit_sell_size: Option<Size>,
            taker_fee: Decimal,
            maker_fee: Decimal,
        }
        let w = Wire::deserialize(d)?;
        match (
            w.today_limit_open_size,
            w.today_limit_buy_size,
            w.today_limit_sell_size,
        ) {
            (Some(open), None, None) => Ok(VolumeLimit::Leverage {
                symbol: w.symbol,
                today_limit_open_size: open,
                taker_fee: w.taker_fee,
                maker_fee: w.maker_fee,
            }),
            (None, Some(buy), Some(sell)) => Ok(VolumeLimit::Spot {
                symbol: w.symbol,
                today_limit_buy_size: buy,
                today_limit_sell_size: sell,
                taker_fee: w.taker_fee,
                maker_fee: w.maker_fee,
            }),
            _ => Err(serde::de::Error::custom(
                "ambiguous tradingVolume limit entry",
            )),
        }
    }
}

/// 日本円の入金または出金の記録。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct FiatTransfer {
    pub amount: Amount,
    pub fee: Amount,
    pub status: TransferStatus,
    pub symbol: Symbol,
    pub timestamp: Timestamp,
}

/// 暗号資産の入金または出金の記録。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CryptoTransfer {
    pub address: Box<str>,
    pub amount: Amount,
    /// 応答の表にはあるが、サンプルにはない。
    pub fee: Option<Amount>,
    pub status: TransferStatus,
    pub symbol: Symbol,
    pub timestamp: Timestamp,
    pub tx_hash: Box<str>,
}

/// `GET /account/exchangeFee/history` の 1 ページ (カーソル方式)。
///
/// # 不変条件
/// `next_cursor()` は、取引所が次ページがあると答えたときに限り `Some`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeFeePage {
    pub list: Vec<ExchangeFee>,
    /// 口座の初回の日次処理が終わるまでは存在しない。
    pub last_finalized_date: Option<BusinessDate>,
    /// 次ページがないとき、`pagination` 自体が省かれても、`cursor` だけが
    /// 省かれても受け付ける。
    #[serde(rename = "pagination", default)]
    next: Option<CursorField>,
}

impl ExchangeFeePage {
    /// 継続トークン。次ページがあるときだけ `Some`。同じ日付範囲で渡し直す。
    pub fn next_cursor(&self) -> Option<&str> {
        self.next.as_ref().and_then(|n| n.cursor.as_deref())
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
    /// 日本円。返金は正、徴収は負。
    pub delivery_amount: Decimal,
}

/// クエリに載せる値のパーセントエンコード。`cursor` だけが、このクレートの
/// 型が字句を保証しない値で、取引所から受け取ったものを呼び出し側が渡し直す。
/// 非予約文字以外をすべてエンコードするので、リクエスト行を壊せない。
fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn history_call(call: Call, window: HistoryWindow) -> Call {
    let call = call.query("fromTimestamp", window.from());
    match window.to() {
        Some(to) => call.query("toTimestamp", to),
        None => call,
    }
}

impl Client<Authenticated> {
    /// `GET /private/v1/account/margin`。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 符号付きの値は送られたまま。状態は `Unknown` になり得る。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// let margin = client.margin().await?;
    /// println!("{} {}", margin.available_amount, margin.margin_ratio);
    /// # Ok(()) }
    /// ```
    pub async fn margin(&self) -> Result<Margin> {
        self.private(Call::new(endpoint!(GET "/v1/account/margin")))
            .await?
            .data()
    }

    /// `GET /private/v1/account/assets`。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 資産銘柄ごとに 1 行。このクレートより新しい銘柄も含む。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// for asset in client.assets().await? {
    ///     println!("{} {}", asset.symbol, asset.available);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn assets(&self) -> Result<Vec<Asset>> {
        self.private(Call::new(endpoint!(GET "/v1/account/assets")))
            .await?
            .data()
    }

    /// `GET /private/v1/account/tradingVolume`。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `limit` の各項目は、文書化された 2 つの形のちょうど一方である。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。どちらの形でもない項目は
    /// `Protocol(Shape)`。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// let volume = client.trading_volume().await?;
    /// println!("tier {} / {}", volume.tier_level, volume.jpy_volume);
    /// # Ok(()) }
    /// ```
    pub async fn trading_volume(&self) -> Result<TradingVolume> {
        self.private(Call::new(endpoint!(GET "/v1/account/tradingVolume")))
            .await?
            .data()
    }

    /// `GET /private/v1/account/fiatDeposit/history`。
    ///
    /// # 事前条件
    /// [`HistoryWindow`] が保証する: 30 分以内、`to` は `from` より後。
    ///
    /// # 事後条件
    /// 送られたままの記録。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{HistoryWindow, Timestamp};
    /// let window = HistoryWindow::starting_at("2024-02-28T11:00:00.000Z".parse::<Timestamp>()?);
    /// let records = client.fiat_deposit_history(window).await?;
    /// println!("{}", records.len());
    /// # Ok(()) }
    /// ```
    pub async fn fiat_deposit_history(&self, window: HistoryWindow) -> Result<Vec<FiatTransfer>> {
        let call = history_call(
            Call::new(endpoint!(GET "/v1/account/fiatDeposit/history")),
            window,
        );
        self.private(call).await?.data()
    }

    /// `GET /private/v1/account/fiatWithdrawal/history`。
    ///
    /// # 事前条件
    /// [`HistoryWindow`] が保証する。
    ///
    /// # 事後条件
    /// 送られたままの記録。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{HistoryWindow, Timestamp};
    /// let window = HistoryWindow::starting_at("2024-02-28T11:00:00.000Z".parse::<Timestamp>()?);
    /// let records = client.fiat_withdrawal_history(window).await?;
    /// println!("{}", records.len());
    /// # Ok(()) }
    /// ```
    pub async fn fiat_withdrawal_history(
        &self,
        window: HistoryWindow,
    ) -> Result<Vec<FiatTransfer>> {
        let call = history_call(
            Call::new(endpoint!(GET "/v1/account/fiatWithdrawal/history")),
            window,
        );
        self.private(call).await?.data()
    }

    /// `GET /private/v1/account/deposit/history?symbol=&fromTimestamp=&toTimestamp=`。
    ///
    /// # 事前条件
    /// [`HistoryWindow`] が保証する。
    ///
    /// # 事後条件
    /// 送られたままの記録。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{HistoryWindow, Symbol, Timestamp};
    /// let window = HistoryWindow::starting_at("2024-02-28T11:00:00.000Z".parse::<Timestamp>()?);
    /// let records = client.deposit_history(Symbol::BTC, window).await?;
    /// println!("{}", records.len());
    /// # Ok(()) }
    /// ```
    pub async fn deposit_history(
        &self,
        symbol: Symbol,
        window: HistoryWindow,
    ) -> Result<Vec<CryptoTransfer>> {
        let call = history_call(
            Call::new(endpoint!(GET "/v1/account/deposit/history")).query("symbol", symbol),
            window,
        );
        self.private(call).await?.data()
    }

    /// `GET /private/v1/account/withdrawal/history`。
    ///
    /// # 事前条件
    /// [`HistoryWindow`] が保証する。
    ///
    /// # 事後条件
    /// 送られたままの記録。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{HistoryWindow, Symbol, Timestamp};
    /// let window = HistoryWindow::starting_at("2024-02-28T11:00:00.000Z".parse::<Timestamp>()?);
    /// let records = client.withdrawal_history(Symbol::BTC, window).await?;
    /// println!("{}", records.len());
    /// # Ok(()) }
    /// ```
    pub async fn withdrawal_history(
        &self,
        symbol: Symbol,
        window: HistoryWindow,
    ) -> Result<Vec<CryptoTransfer>> {
        let call = history_call(
            Call::new(endpoint!(GET "/v1/account/withdrawal/history")).query("symbol", symbol),
            window,
        );
        self.private(call).await?.data()
    }

    /// `GET /private/v1/account/exchangeFee/history`。
    ///
    /// # 事前条件
    /// [`BusinessDateRange`] が保証する (7 日以内、`from <= to`)。`count` は
    /// `1..=100` で、そうでなければ `Contract` (リクエストは作られない)。
    /// `cursor` は同じ `range` で取得したページのものでなければならない。
    ///
    /// # 事後条件
    /// `next_cursor()` は、取引所が次ページがあると報告したときに限り `Some`。
    /// `cursor` はパーセントエンコードして送る。
    ///
    /// # エラー
    /// - `Contract`: `count` が `1..=100` の外。
    /// - `Api` の ERR-5016: 取引所がタイムアウトした。ドキュメントは 1 回
    ///   再試行し、それでも駄目なら範囲を狭めるよう求めている。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::BusinessDate;
    /// use gmo_coin_api_client::BusinessDateRange;
    /// let range = BusinessDateRange::single(BusinessDate::parse("20260615")?);
    /// let mut cursor: Option<String> = None;
    /// loop {
    ///     let page = client.exchange_fee_history(range, 100, cursor.as_deref()).await?;
    ///     println!("{} 件", page.list.len());
    ///     match page.next_cursor() {
    ///         Some(next) => cursor = Some(next.to_owned()),
    ///         None => break,
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn exchange_fee_history(
        &self,
        range: BusinessDateRange,
        count: u8,
        cursor: Option<&str>,
    ) -> Result<ExchangeFeePage> {
        if count == 0 || count > 100 {
            return Err(InvalidValue {
                primitive: Primitive::PageRequest,
                violation: Violation::Semantics("count outside 1..=100"),
            }
            .into());
        }
        let mut call = Call::new(endpoint!(GET "/v1/account/exchangeFee/history"))
            .query("fromDate", range.from());
        if let Some(to) = range.to() {
            call = call.query("toDate", to);
        }
        call = call.query("count", count);
        if let Some(cursor) = cursor {
            call = call.query("cursor", percent_encode(cursor));
        }
        self.private(call).await?.data()
    }

    /// `POST /private/v1/account/transfer`。取引所は 3 分に 1 回しか受け付けない。
    ///
    /// # 事前条件
    /// [`Amount`] が保証する (負でない)。3 分の間隔は取引所が強制する。
    /// このクレートは絞らない。
    ///
    /// # 事後条件
    /// `data` がちょうど 1 件を持ち (そうでなければ
    /// `Protocol(Postcondition(ExpectedOneEntry))`)、その `transferredAmount`
    /// を返す。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。振替可能額を超えると ERR-754。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{Amount, TransferType};
    /// let moved = client.transfer(Amount::parse("100000")?, TransferType::Deposit).await?;
    /// println!("{moved}");
    /// # Ok(()) }
    /// ```
    pub async fn transfer(&self, amount: Amount, direction: TransferType) -> Result<Amount> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            amount: Amount,
            transfer_type: TransferType,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Entry {
            transferred_amount: Amount,
        }
        let call = Call::new(endpoint!(POST "/v1/account/transfer")).json(&Body {
            amount,
            transfer_type: direction,
        });
        let answer = self.private(call).await?;
        let mut entries = decode::<Vec<Entry>>(&answer)?;
        match (entries.pop(), entries.is_empty()) {
            (Some(entry), true) => Ok(entry.transferred_amount),
            (last, _) => Err(answer.protocol(ProtocolErrorKind::Postcondition(
                Postcondition::ExpectedOneEntry {
                    actual: entries.len() + usize::from(last.is_some()),
                },
            ))),
        }
    }
}

/// `Answer::data` と同じ規則で、答えを消費せずにデコードする。事後条件の
/// エラーが答えの文脈 (操作名) を必要とするため。
fn decode<T: serde::de::DeserializeOwned>(answer: &Answer) -> Result<T> {
    let Some(raw) = answer.raw() else {
        return Err(answer.protocol(ProtocolErrorKind::MissingData));
    };
    serde_json::from_str(raw).map_err(|e| answer.shape_error(&e))
}
