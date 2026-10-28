//! WebSocket メッセージのペイロード。Public の ticker は `rest::Ticker` を
//! そのまま使う (フィールドが同一)。それ以外は REST の対応物と形が違う。
//!
//! どの型もデコードはフェイルクローズドである: 文書化されたフィールドが
//! 欠けるか形が違えば、フィードは `Protocol(WsMessage)` で終わる。開いた
//! 列挙 (`OrderStatus`、`CancelType`、各 `MsgType`) は未知の値を
//! `Unknown(UnknownValue)` として保持する。

use serde::Deserialize;

use crate::domain::{
    CancelType, ClientOrderId, Decimal, ExecutionId, ExecutionMsgType, ExecutionType, OrderId,
    OrderMsgType, OrderStatus, PositionId, PositionMsgType, Price, SettleType, Side, Size,
    SummaryMsgType, Symbol, TimeInForce, Timestamp,
};
use crate::rest::OrderBookLevel;

/// `orderbooks`: 毎回完全なスナップショット。再接続の後は次のスナップショットを
/// 待つ以外の修復は要らない。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct OrderBookUpdate {
    /// 取扱銘柄。
    pub symbol: Symbol,
    /// 売り注文の情報（最良気配から昇順）。
    pub asks: Vec<OrderBookLevel>,
    /// 買い注文の情報（最良気配から降順）。
    pub bids: Vec<OrderBookLevel>,
    /// 配信日時。
    pub timestamp: Timestamp,
}

/// `trades`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct TradeEvent {
    /// 取扱銘柄。
    pub symbol: Symbol,
    /// 約定価格。
    pub price: Price,
    /// 売買区分。
    pub side: Side,
    /// 約定数量。
    pub size: Size,
    /// 約定日時。
    pub timestamp: Timestamp,
}

/// `executionEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionEvent {
    /// 注文 ID。
    pub order_id: OrderId,
    /// 顧客注文 ID。設定した場合のみ返る。該当しないときは `None`。
    pub client_order_id: Option<ClientOrderId>,
    /// 約定 ID。
    pub execution_id: ExecutionId,
    /// 取扱銘柄。
    pub symbol: Symbol,
    /// 決済区分。
    pub settle_type: SettleType,
    /// 注文タイプ。
    pub execution_type: ExecutionType,
    /// 売買区分。
    pub side: Side,
    /// 約定レート。
    pub execution_price: Price,
    /// 約定数量。
    pub execution_size: Size,
    /// 建玉 ID。レバレッジの場合のみ。該当しないときは `None`。
    pub position_id: Option<PositionId>,
    /// 注文日時。
    pub order_timestamp: Timestamp,
    /// 約定日時。
    pub execution_timestamp: Timestamp,
    /// 決済損益。
    pub loss_gain: Decimal,
    /// 取引手数料。Taker は正の値、Maker は負の値。
    pub fee: Decimal,
    /// MARKET 注文では欠ける。
    pub order_price: Option<Price>,
    /// 注文数量。
    pub order_size: Size,
    /// 約定済み数量。
    pub order_executed_size: Size,
    /// 執行数量条件。
    pub time_in_force: TimeInForce,
    /// メッセージタイプ。
    pub msg_type: ExecutionMsgType,
}

/// `orderEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderEvent {
    /// 注文 ID。
    pub order_id: OrderId,
    /// 顧客注文 ID。設定した場合のみ返る。該当しないときは `None`。
    pub client_order_id: Option<ClientOrderId>,
    /// 取扱銘柄。
    pub symbol: Symbol,
    /// 決済区分。
    pub settle_type: SettleType,
    /// 注文タイプ。
    pub execution_type: ExecutionType,
    /// 売買区分。
    pub side: Side,
    /// 注文ステータス。逆指値注文では `WAITING` が有効。
    pub order_status: OrderStatus,
    /// CANCELED と EXPIRED のとき。ORDERED では `SOK_TAKER` と `PRICE_LIMIT`。
    pub cancel_type: Option<CancelType>,
    /// 注文日時。
    pub order_timestamp: Timestamp,
    /// 注文価格。MARKET 注文では欠けるので `None`。
    pub order_price: Option<Price>,
    /// 注文数量。
    pub order_size: Size,
    /// 約定済み数量。
    pub order_executed_size: Size,
    /// ロスカットレート (現物取引や未設定の場合は"0")。
    pub losscut_price: Price,
    /// 執行数量条件。
    pub time_in_force: TimeInForce,
    /// メッセージタイプ。
    pub msg_type: OrderMsgType,
}

/// `positionEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionEvent {
    /// 建玉 ID。
    pub position_id: PositionId,
    /// レバレッジ取扱銘柄。
    pub symbol: Symbol,
    /// 売買区分。
    pub side: Side,
    /// 建玉数量。
    pub size: Size,
    /// 取引所の綴りは `orderdSize` (原文のまま)。
    #[serde(rename = "orderdSize")]
    pub ordered_size: Size,
    /// 建玉レート。
    pub price: Price,
    /// 評価損益。
    pub loss_gain: Decimal,
    /// レバレッジ。
    pub leverage: Decimal,
    /// ロスカットレート。
    pub losscut_price: Price,
    /// 約定日時。
    pub timestamp: Timestamp,
    /// メッセージタイプ。
    pub msg_type: PositionMsgType,
}

/// `positionSummaryEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionSummaryEvent {
    /// レバレッジ取扱銘柄。
    pub symbol: Symbol,
    /// 売買区分。
    pub side: Side,
    /// 平均建玉レート。
    pub average_position_rate: Price,
    /// 評価損益。
    pub position_loss_gain: Decimal,
    /// 発注中数量。
    pub sum_order_quantity: Size,
    /// 建玉数量。
    pub sum_position_quantity: Size,
    /// 配信日時。
    pub timestamp: Timestamp,
    /// メッセージタイプ。
    pub msg_type: SummaryMsgType,
}
