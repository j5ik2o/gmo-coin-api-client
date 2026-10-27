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
    pub symbol: Symbol,
    pub asks: Vec<OrderBookLevel>,
    pub bids: Vec<OrderBookLevel>,
    pub timestamp: Timestamp,
}

/// `trades`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct TradeEvent {
    pub symbol: Symbol,
    pub price: Price,
    pub side: Side,
    pub size: Size,
    pub timestamp: Timestamp,
}

/// `executionEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionEvent {
    pub order_id: OrderId,
    pub client_order_id: Option<ClientOrderId>,
    pub execution_id: ExecutionId,
    pub symbol: Symbol,
    pub settle_type: SettleType,
    pub execution_type: ExecutionType,
    pub side: Side,
    pub execution_price: Price,
    pub execution_size: Size,
    pub position_id: Option<PositionId>,
    pub order_timestamp: Timestamp,
    pub execution_timestamp: Timestamp,
    pub loss_gain: Decimal,
    pub fee: Decimal,
    /// MARKET 注文では欠ける。
    pub order_price: Option<Price>,
    pub order_size: Size,
    pub order_executed_size: Size,
    pub time_in_force: TimeInForce,
    pub msg_type: ExecutionMsgType,
}

/// `orderEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderEvent {
    pub order_id: OrderId,
    pub client_order_id: Option<ClientOrderId>,
    pub symbol: Symbol,
    pub settle_type: SettleType,
    pub execution_type: ExecutionType,
    pub side: Side,
    pub order_status: OrderStatus,
    /// CANCELED と EXPIRED のとき。ORDERED では `SOK_TAKER` と `PRICE_LIMIT`。
    pub cancel_type: Option<CancelType>,
    pub order_timestamp: Timestamp,
    pub order_price: Option<Price>,
    pub order_size: Size,
    pub order_executed_size: Size,
    pub losscut_price: Price,
    pub time_in_force: TimeInForce,
    pub msg_type: OrderMsgType,
}

/// `positionEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionEvent {
    pub position_id: PositionId,
    pub symbol: Symbol,
    pub side: Side,
    pub size: Size,
    /// 取引所の綴りは `orderdSize` (原文のまま)。
    #[serde(rename = "orderdSize")]
    pub ordered_size: Size,
    pub price: Price,
    pub loss_gain: Decimal,
    pub leverage: Decimal,
    pub losscut_price: Price,
    pub timestamp: Timestamp,
    pub msg_type: PositionMsgType,
}

/// `positionSummaryEvents`。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionSummaryEvent {
    pub symbol: Symbol,
    pub side: Side,
    pub average_position_rate: Price,
    pub position_loss_gain: Decimal,
    pub sum_order_quantity: Size,
    pub sum_position_quantity: Size,
    pub timestamp: Timestamp,
    pub msg_type: SummaryMsgType,
}
