//! WebSocket message payloads. The public ticker reuses `rest::Ticker`
//! (identical fields); the others differ from their REST cousins.

use serde::Deserialize;

use crate::domain::{
    CancelType, ClientOrderId, Decimal, ExecutionId, ExecutionMsgType, ExecutionType, OrderId,
    OrderMsgType, OrderStatus, PositionId, PositionMsgType, Price, SettleType, Side, Size,
    SummaryMsgType, Symbol, TimeInForce, Timestamp,
};
use crate::rest::OrderBookLevel;

/// `orderbooks`: a full snapshot each time, so a reconnect needs no repair
/// beyond waiting for the next one.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct OrderBookUpdate {
    pub symbol: Symbol,
    pub asks: Vec<OrderBookLevel>,
    pub bids: Vec<OrderBookLevel>,
    pub timestamp: Timestamp,
}

/// `trades`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct TradeEvent {
    pub symbol: Symbol,
    pub price: Price,
    pub side: Side,
    pub size: Size,
    pub timestamp: Timestamp,
}

/// `executionEvents`.
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
    /// Absent for MARKET orders.
    pub order_price: Option<Price>,
    pub order_size: Size,
    pub order_executed_size: Size,
    pub time_in_force: TimeInForce,
    pub msg_type: ExecutionMsgType,
}

/// `orderEvents`.
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
    /// For CANCELED/EXPIRED, and `SOK_TAKER`/`PRICE_LIMIT` for ORDERED.
    pub cancel_type: Option<CancelType>,
    pub order_timestamp: Timestamp,
    pub order_price: Option<Price>,
    pub order_size: Size,
    pub order_executed_size: Size,
    pub losscut_price: Price,
    pub time_in_force: TimeInForce,
    pub msg_type: OrderMsgType,
}

/// `positionEvents`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionEvent {
    pub position_id: PositionId,
    pub symbol: Symbol,
    pub side: Side,
    pub size: Size,
    #[serde(rename = "orderdSize")]
    pub ordered_size: Size,
    pub price: Price,
    pub loss_gain: Decimal,
    pub leverage: Decimal,
    pub losscut_price: Price,
    pub timestamp: Timestamp,
    pub msg_type: PositionMsgType,
}

/// `positionSummaryEvents`.
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
