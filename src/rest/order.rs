//! 注文リクエスト: 呼び出し側が取引所に頼めるお金の動き。
//!
//! 注文系エンドポイントはドキュメント上それぞれ別の文法を持つので、
//! それぞれ専用の執行方法の列挙型を持つ。リクエスト自身のフィールドだけで
//! 決まる規則は、variant の形で表している。
//!
//! | 規則 (ドキュメント由来)                                 | 表現する場所               |
//! |---------------------------------------------------------|----------------------------|
//! | LIMIT と STOP は `price` が必須、MARKET は持たない       | variant のフィールド       |
//! | `timeInForce` は LIMIT のみ                              | `Limit` だけがフィールドを持つ |
//! | `SOK` (post-only) は LIMIT のみ                          | `LimitTimeInForce`         |
//! | `losscutPrice` は LIMIT/STOP のみ、決済注文では不可       | variant のフィールド       |
//! | `cancelBefore` は MARKET (FAK) のみ                      | `MarketCancelBefore` variant |
//! | `closeBulkOrder` に `cancelBefore` はない                | `BulkCloseExecution` が持たない |
//! | `closeOrder` が決済する建玉はちょうど 1 つ               | `PositionId` フィールド 1 つ |
//! | `clientOrderId` は 36 文字以内の英数字                   | `ClientOrderId`            |
//! | 数量・価格はゼロでない                                   | `OrderSize`、`OrderPrice`  |
//!
//! 銘柄の市場 (現物かレバレッジか) に依存する規則は、リクエストの形ではなく
//! データの性質なので、送信前に API の境界で検査し `Error::Contract` で
//! 失敗させる。
//! - `/order` の `cancelBefore` は現物銘柄の `Side::Sell` のみ。
//! - `losscutPrice` はレバレッジ銘柄のみ。
//!
//! 取引所の状態に依存する規則 (刻み値、数量の最小・最大、当日の残り枠、
//! 建玉の有無) は取引所が検査する。このクレートは写さないので、古くならない。
//!
//! # パニック
//! このファイルの関数はパニックしない。
//!
//! # 例
//! ```
//! use gmo_coin_api_client::rest::{NewOrder, OrderExecution};
//! use gmo_coin_api_client::{ClientOrderId, OrderSize, Side, Symbol};
//!
//! let order = NewOrder {
//!     client_order_id: ClientOrderId::generate(),
//!     symbol: Symbol::BTC,
//!     side: Side::Buy,
//!     size: OrderSize::parse("0.01").unwrap(),
//!     execution: OrderExecution::Market,
//! };
//! assert_eq!(order.side, Side::Buy);
//! // 数量ゼロの注文は作れない。
//! assert!(OrderSize::parse("0").is_err());
//! ```

use core::fmt::Display;

use serde::Serialize;

use crate::domain::{
    Batch, ClientOrderId, ExecutionId, InvalidValue, LimitTimeInForce, Market, OrderId, OrderPrice,
    OrderSize, PositionId, Primitive, SettleFilter, Side, Symbol, Violation,
};
use crate::error::ContractViolation;

/// `POST /v1/order`: 現物の売買、またはレバレッジの新規建て。
///
/// 公開フィールドを持つ素のデータ。どのフィールドも検証済みのプリミティブで、
/// フィールドをまたぐ規則は `Client::place_order` が送信前に検査する。
///
/// # 不変条件
/// `size` と、`execution` の価格はゼロでない (`OrderSize`、`OrderPrice`)。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOrder {
    /// ワイヤ上は任意だが、ここでは必須。結果不明の後に安全に再送できる
    /// ようになる。`ClientOrderId::generate()` を使う。
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub size: OrderSize,
    pub execution: OrderExecution,
}

/// `/order` の執行方法。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderExecution {
    /// MARKET。取引所は FAK で執行する。
    Market,
    /// `cancelBefore: true` 付きの MARKET。数量が収まるまで古い有効注文を
    /// 取り消してから、成行で売る (FAK)。現物の `Sell` のみ。
    MarketCancelBefore,
    /// LIMIT。`losscut_price` はレバレッジ銘柄のみ。
    Limit {
        price: OrderPrice,
        time_in_force: LimitTimeInForce,
        losscut_price: Option<OrderPrice>,
    },
    /// STOP (`price` で発動する逆指値の成行)。取引所は FAK で執行する。
    /// `losscut_price` はレバレッジ銘柄のみ。
    Stop {
        price: OrderPrice,
        losscut_price: Option<OrderPrice>,
    },
}

/// 3 つの注文エンドポイントが共有する、執行方法のワイヤ表現。
/// 文法上存在しないフィールドは `null` ではなく省略する。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireExecution {
    execution_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<LimitTimeInForce>,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<OrderPrice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancel_before: Option<bool>,
}

impl WireExecution {
    const MARKET: WireExecution = WireExecution {
        execution_type: "MARKET",
        time_in_force: None,
        price: None,
        cancel_before: None,
    };

    /// `timeInForce` は付けない。ドキュメントの `timeInForce` 欄
    /// (「FAK FAS FOK ((Post-onlyの場合はSOK) LIMITの場合のみ設定可能)」) は、
    /// 全体が LIMIT 専用とも SOK だけが LIMIT 専用とも読める。省略すれば
    /// MARKET の既定の FAK で執行され、`cancelBefore` の条件 (MARKET、FAK、
    /// SELL) をどちらの読み方でも満たす。明示すると、前者の読み方では
    /// ERR-5111 で拒否されうる。
    const MARKET_CANCEL_BEFORE: WireExecution = WireExecution {
        execution_type: "MARKET",
        time_in_force: None,
        price: None,
        cancel_before: Some(true),
    };

    fn limit(price: OrderPrice, time_in_force: LimitTimeInForce) -> Self {
        WireExecution {
            execution_type: "LIMIT",
            time_in_force: Some(time_in_force),
            price: Some(price),
            cancel_before: None,
        }
    }

    fn stop(price: OrderPrice) -> Self {
        WireExecution {
            execution_type: "STOP",
            time_in_force: None,
            price: Some(price),
            cancel_before: None,
        }
    }
}

impl NewOrder {
    /// 型では持てない事前条件。
    ///
    /// # エラー
    /// - `CancelBeforeRequiresSpotSell`: `MarketCancelBefore` なのにレバレッジ
    ///   銘柄、または `Side::Buy`。
    /// - `LosscutRequiresLeverageSymbol`: 現物銘柄に `losscut_price`。
    pub(crate) fn check(&self) -> Result<(), ContractViolation> {
        match &self.execution {
            OrderExecution::MarketCancelBefore
                if self.side != Side::Sell || self.symbol.market() != Market::Spot =>
            {
                Err(ContractViolation::CancelBeforeRequiresSpotSell)
            }
            OrderExecution::Limit {
                losscut_price: Some(_),
                ..
            }
            | OrderExecution::Stop {
                losscut_price: Some(_),
                ..
            } if self.symbol.market() != Market::Leverage => {
                Err(ContractViolation::LosscutRequiresLeverageSymbol)
            }
            _ => Ok(()),
        }
    }

    /// ワイヤのボディ。
    pub(crate) fn wire(&self) -> WireOrder<'_> {
        let (execution, losscut_price) = match &self.execution {
            OrderExecution::Market => (WireExecution::MARKET, None),
            OrderExecution::MarketCancelBefore => (WireExecution::MARKET_CANCEL_BEFORE, None),
            OrderExecution::Limit {
                price,
                time_in_force,
                losscut_price,
            } => (WireExecution::limit(*price, *time_in_force), *losscut_price),
            OrderExecution::Stop {
                price,
                losscut_price,
            } => (WireExecution::stop(*price), *losscut_price),
        };
        WireOrder {
            symbol: &self.symbol,
            side: self.side,
            execution,
            losscut_price,
            size: self.size,
            client_order_id: &self.client_order_id,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireOrder<'a> {
    symbol: &'a Symbol,
    side: Side,
    #[serde(flatten)]
    execution: WireExecution,
    #[serde(skip_serializing_if = "Option::is_none")]
    losscut_price: Option<OrderPrice>,
    size: OrderSize,
    client_order_id: &'a ClientOrderId,
}

/// `POST /v1/closeOrder`: レバレッジの建玉 1 つを、全部または一部決済する。
///
/// # 不変条件
/// `size` はゼロでない。決済する建玉はちょうど 1 つ。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClosePosition {
    pub client_order_id: ClientOrderId,
    /// レバレッジ銘柄でなければならない (`Error::Contract(RequiresLeverageSymbol)`)。
    pub symbol: Symbol,
    /// 決済する側: 建玉の売買と反対。取引所が検査する (ERR-70)。建玉の売買を
    /// 知っているのは取引所だけだからである。
    pub side: Side,
    /// `settlePosition` は建玉を 1 つだけ取る。これがその 1 つ。
    pub position_id: PositionId,
    pub size: OrderSize,
    pub execution: CloseExecution,
}

/// `/closeOrder` の執行方法。`losscutPrice` は文法上存在しない。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloseExecution {
    Market,
    /// `cancelBefore: true` (MARKET、FAK)。複数の建玉に紐づく注文には取引所が
    /// 拒否する。
    MarketCancelBefore,
    Limit {
        price: OrderPrice,
        time_in_force: LimitTimeInForce,
    },
    Stop {
        price: OrderPrice,
    },
}

impl ClosePosition {
    /// # エラー
    /// `RequiresLeverageSymbol`: 銘柄がレバレッジでない。
    pub(crate) fn check(&self) -> Result<(), ContractViolation> {
        require_leverage(&self.symbol)
    }

    pub(crate) fn wire(&self) -> WireClose<'_> {
        let execution = match &self.execution {
            CloseExecution::Market => WireExecution::MARKET,
            CloseExecution::MarketCancelBefore => WireExecution::MARKET_CANCEL_BEFORE,
            CloseExecution::Limit {
                price,
                time_in_force,
            } => WireExecution::limit(*price, *time_in_force),
            CloseExecution::Stop { price } => WireExecution::stop(*price),
        };
        WireClose {
            symbol: &self.symbol,
            side: self.side,
            execution,
            settle_position: [WireSettle {
                position_id: self.position_id,
                size: self.size,
            }],
            client_order_id: &self.client_order_id,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireClose<'a> {
    symbol: &'a Symbol,
    side: Side,
    #[serde(flatten)]
    execution: WireExecution,
    /// ドキュメントは配列で、要素は 1 つだけ許す。
    settle_position: [WireSettle; 1],
    client_order_id: &'a ClientOrderId,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireSettle {
    position_id: PositionId,
    size: OrderSize,
}

/// `POST /v1/closeBulkOrder`: 1 つの銘柄・売買区分の建玉を `size` まで決済する。
///
/// # 不変条件
/// `size` はゼロでない。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkClose {
    pub client_order_id: ClientOrderId,
    /// レバレッジ銘柄でなければならない。
    pub symbol: Symbol,
    pub side: Side,
    pub size: OrderSize,
    pub execution: BulkCloseExecution,
}

/// `/closeBulkOrder` の執行方法。`cancelBefore` は文法上存在しない。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BulkCloseExecution {
    Market,
    Limit {
        price: OrderPrice,
        time_in_force: LimitTimeInForce,
    },
    Stop {
        price: OrderPrice,
    },
}

impl BulkClose {
    /// # エラー
    /// `RequiresLeverageSymbol`: 銘柄がレバレッジでない。
    pub(crate) fn check(&self) -> Result<(), ContractViolation> {
        require_leverage(&self.symbol)
    }

    pub(crate) fn wire(&self) -> WireBulkClose<'_> {
        let execution = match &self.execution {
            BulkCloseExecution::Market => WireExecution::MARKET,
            BulkCloseExecution::Limit {
                price,
                time_in_force,
            } => WireExecution::limit(*price, *time_in_force),
            BulkCloseExecution::Stop { price } => WireExecution::stop(*price),
        };
        WireBulkClose {
            symbol: &self.symbol,
            side: self.side,
            execution,
            size: self.size,
            client_order_id: &self.client_order_id,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireBulkClose<'a> {
    symbol: &'a Symbol,
    side: Side,
    #[serde(flatten)]
    execution: WireExecution,
    size: OrderSize,
    client_order_id: &'a ClientOrderId,
}

pub(crate) fn require_leverage(symbol: &Symbol) -> Result<(), ContractViolation> {
    if symbol.market() == Market::Leverage {
        Ok(())
    } else {
        Err(ContractViolation::RequiresLeverageSymbol)
    }
}

/// `POST /v1/cancelBulkOrder`: 条件に合う有効注文を最大 10 件まで取り消す。
///
/// # 不変条件
/// 型では `symbols` が空でないことを保証できない。`Client::cancel_bulk_orders`
/// が送信前に検査する。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkCancel {
    pub symbols: Vec<Symbol>,
    pub side: Option<Side>,
    pub settle_type: Option<SettleFilter>,
    /// `desc`: 古い順ではなく新しい順に取り消す。
    pub newest_first: bool,
}

impl BulkCancel {
    /// # エラー
    /// `InvalidValue`: `symbols` が空。件数の上限はドキュメントにないので
    /// `max` は `usize::MAX` と報告する。
    pub(crate) fn check(&self) -> Result<(), ContractViolation> {
        if self.symbols.is_empty() {
            return Err(ContractViolation::InvalidValue(InvalidValue {
                primitive: Primitive::Batch,
                violation: Violation::Size {
                    min: 1,
                    max: usize::MAX,
                    actual: 0,
                },
            }));
        }
        Ok(())
    }

    pub(crate) fn wire(&self) -> WireBulkCancel<'_> {
        WireBulkCancel {
            symbols: &self.symbols,
            side: self.side,
            settle_type: self.settle_type,
            desc: self.newest_first.then_some(true),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireBulkCancel<'a> {
    symbols: &'a [Symbol],
    #[serde(skip_serializing_if = "Option::is_none")]
    side: Option<Side>,
    #[serde(skip_serializing_if = "Option::is_none")]
    settle_type: Option<SettleFilter>,
    /// 既定 (`false`) は省略する。
    #[serde(skip_serializing_if = "Option::is_none")]
    desc: Option<bool>,
}

/// `GET /v1/orders`: `orderId` か `clientOrderId` のどちらか一方、各 10 件まで。
/// 列挙型により「両方」も「どちらもなし」も表現できず、`Batch` により
/// 「11 件以上」も表現できない。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderLookup {
    ByOrderIds(Batch<OrderId, 10>),
    ByClientOrderIds(Batch<ClientOrderId, 10>),
}

impl OrderLookup {
    pub(crate) fn query(&self) -> (&'static str, String) {
        match self {
            OrderLookup::ByOrderIds(ids) => ("orderId", comma_separated(ids.as_slice())),
            OrderLookup::ByClientOrderIds(ids) => {
                ("clientOrderId", comma_separated(ids.as_slice()))
            }
        }
    }
}

/// `GET /v1/executions`: `orderId` (1 件)、`executionId` (10 件まで)、
/// `clientOrderId` (1 件、カンマ区切り不可) のうちちょうど 1 つ。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionLookup {
    ByOrderId(OrderId),
    ByExecutionIds(Batch<ExecutionId, 10>),
    ByClientOrderId(ClientOrderId),
}

impl ExecutionLookup {
    pub(crate) fn query(&self) -> (&'static str, String) {
        match self {
            ExecutionLookup::ByOrderId(id) => ("orderId", id.to_string()),
            ExecutionLookup::ByExecutionIds(ids) => {
                ("executionId", comma_separated(ids.as_slice()))
            }
            ExecutionLookup::ByClientOrderId(id) => ("clientOrderId", id.to_string()),
        }
    }
}

/// id の字句規則 (数字、または英数字) は `,` と `%` を含まないので、
/// パーセントエンコードは要らない。
fn comma_separated<T: Display>(items: &[T]) -> String {
    let mut text = String::new();
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            text.push(',');
        }
        text.push_str(&item.to_string());
    }
    text
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn body(v: &impl Serialize) -> Value {
        serde_json::to_value(v).expect("serializes")
    }

    fn price(text: &str) -> OrderPrice {
        OrderPrice::parse(text).unwrap()
    }

    fn order(symbol: Symbol, side: Side, execution: OrderExecution) -> NewOrder {
        NewOrder {
            client_order_id: ClientOrderId::parse("abc123").unwrap(),
            symbol,
            side,
            size: OrderSize::parse("0.02").unwrap(),
            execution,
        }
    }

    #[test]
    fn market_has_no_price_and_no_time_in_force() {
        let o = order(Symbol::BTC, Side::Buy, OrderExecution::Market);
        assert_eq!(
            body(&o.wire()),
            json!({"symbol":"BTC","side":"BUY","executionType":"MARKET","size":"0.02","clientOrderId":"abc123"})
        );
    }

    #[test]
    fn cancel_before_is_a_market_order_without_time_in_force() {
        let o = order(Symbol::BTC, Side::Sell, OrderExecution::MarketCancelBefore);
        assert_eq!(
            body(&o.wire()),
            json!({"symbol":"BTC","side":"SELL","executionType":"MARKET","cancelBefore":true,"size":"0.02","clientOrderId":"abc123"})
        );
        let c = ClosePosition {
            client_order_id: ClientOrderId::parse("c1").unwrap(),
            symbol: Symbol::BTC_JPY,
            side: Side::Sell,
            position_id: PositionId::new(1234567).unwrap(),
            size: OrderSize::parse("0.1").unwrap(),
            execution: CloseExecution::MarketCancelBefore,
        };
        assert_eq!(
            body(&c.wire()),
            json!({"symbol":"BTC_JPY","side":"SELL","executionType":"MARKET","cancelBefore":true,
                   "settlePosition":[{"positionId":1234567,"size":"0.1"}],"clientOrderId":"c1"})
        );
    }

    #[test]
    fn stop_carries_a_price_but_no_time_in_force() {
        let o = order(
            Symbol::BTC_JPY,
            Side::Sell,
            OrderExecution::Stop {
                price: price("900"),
                losscut_price: Some(price("800")),
            },
        );
        assert_eq!(
            body(&o.wire()),
            json!({"symbol":"BTC_JPY","side":"SELL","executionType":"STOP","price":"900","losscutPrice":"800","size":"0.02","clientOrderId":"abc123"})
        );
    }

    #[test]
    fn close_position_nests_exactly_one_settle_entry() {
        let c = ClosePosition {
            client_order_id: ClientOrderId::parse("c1").unwrap(),
            symbol: Symbol::BTC_JPY,
            side: Side::Sell,
            position_id: PositionId::new(1234567).unwrap(),
            size: OrderSize::parse("0.1").unwrap(),
            execution: CloseExecution::Limit {
                price: price("1000"),
                time_in_force: LimitTimeInForce::PostOnly,
            },
        };
        assert_eq!(
            body(&c.wire()),
            json!({"symbol":"BTC_JPY","side":"SELL","executionType":"LIMIT","timeInForce":"SOK","price":"1000",
                   "settlePosition":[{"positionId":1234567,"size":"0.1"}],"clientOrderId":"c1"})
        );
    }

    #[test]
    fn bulk_cancel_omits_unset_filters() {
        let all = BulkCancel {
            symbols: vec![Symbol::BTC],
            side: None,
            settle_type: None,
            newest_first: false,
        };
        assert_eq!(body(&all.wire()), json!({"symbols":["BTC"]}));
        let narrowed = BulkCancel {
            symbols: vec![Symbol::BTC, Symbol::BTC_JPY],
            side: Some(Side::Buy),
            settle_type: Some(SettleFilter::Close),
            newest_first: true,
        };
        assert_eq!(
            body(&narrowed.wire()),
            json!({"symbols":["BTC","BTC_JPY"],"side":"BUY","settleType":"CLOSE","desc":true})
        );
    }

    #[test]
    fn spot_symbols_cannot_carry_a_losscut_price() {
        let o = order(
            Symbol::BTC,
            Side::Buy,
            OrderExecution::Limit {
                price: price("1"),
                time_in_force: LimitTimeInForce::Fas,
                losscut_price: Some(price("1")),
            },
        );
        assert_eq!(
            o.check(),
            Err(ContractViolation::LosscutRequiresLeverageSymbol)
        );
    }

    #[test]
    fn lookups_join_ids_with_commas() {
        let ids = Batch::new(vec![OrderId::new(1).unwrap(), OrderId::new(22).unwrap()]).unwrap();
        assert_eq!(
            OrderLookup::ByOrderIds(ids).query(),
            ("orderId", "1,22".to_owned())
        );
        let one = ExecutionLookup::ByClientOrderId(ClientOrderId::parse("x9").unwrap());
        assert_eq!(one.query(), ("clientOrderId", "x9".to_owned()));
    }
}
