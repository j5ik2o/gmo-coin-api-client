//! Private REST: orders, executions, positions. Every method here can move
//! money or reveals account state, so they exist only on
//! `Client<Authenticated>`.

use serde::{Deserialize, Serialize};

use super::envelope::{WireMessage, message};
use super::order::{BulkCancel, BulkClose, ClosePosition, ExecutionLookup, NewOrder, OrderLookup};
use super::{Call, Page, PageRequest, endpoint};
use crate::client::{Authenticated, Client};
use crate::domain::{
    Batch, CancelType, ClientOrderId, Decimal, ExecutionId, ExecutionType, Market, OrderId,
    OrderStatus, OrderType, PositionId, Price, SettleType, Side, Size, Symbol, TimeInForce,
    Timestamp,
};
use crate::error::{ApiMessage, ContractViolation, Postcondition, ProtocolErrorKind, Result};

/// An order as the exchange reports it (`GET /orders`, `GET /activeOrders`).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub root_order_id: OrderId,
    pub order_id: OrderId,
    /// Present only when the order was placed with one.
    pub client_order_id: Option<ClientOrderId>,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub execution_type: ExecutionType,
    pub settle_type: SettleType,
    pub size: Size,
    pub executed_size: Size,
    /// Absent for MARKET orders.
    pub price: Option<Price>,
    /// `"0"` for spot orders and when unset.
    pub losscut_price: Price,
    pub status: OrderStatus,
    /// Present only for CANCELLING, CANCELED, EXPIRED.
    pub cancel_type: Option<CancelType>,
    pub time_in_force: TimeInForce,
    pub timestamp: Timestamp,
}

/// A fill (`GET /executions`, `GET /latestExecutions`).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Execution {
    pub execution_id: ExecutionId,
    pub order_id: OrderId,
    pub client_order_id: Option<ClientOrderId>,
    /// Leverage only.
    pub position_id: Option<PositionId>,
    pub symbol: Symbol,
    pub side: Side,
    pub settle_type: SettleType,
    pub size: Size,
    pub price: Price,
    pub loss_gain: Decimal,
    /// Positive for taker, negative for maker.
    pub fee: Decimal,
    pub timestamp: Timestamp,
}

/// An open leverage position (`GET /openPositions`).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub position_id: PositionId,
    pub symbol: Symbol,
    pub side: Side,
    pub size: Size,
    /// Spelled `orderdSize` on the wire.
    #[serde(rename = "orderdSize")]
    pub ordered_size: Size,
    pub price: Price,
    pub loss_gain: Decimal,
    pub leverage: Decimal,
    pub losscut_price: Price,
    pub timestamp: Timestamp,
}

/// `GET /positionSummary`, one row per symbol and side.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionSummary {
    pub symbol: Symbol,
    pub side: Side,
    pub average_position_rate: Price,
    pub position_loss_gain: Decimal,
    pub sum_order_quantity: Size,
    pub sum_position_quantity: Size,
}

/// `cancelOrders`' partial success. Not an error: some ids can be cancelled
/// while others cannot, and the caller needs both lists.
///
/// # Invariants (checked when the answer is parsed)
/// - Every id in `cancelled` and `rejected` is one the caller sent.
/// - No id appears twice across both lists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrdersOutcome {
    cancelled: Vec<OrderId>,
    rejected: Vec<CancelRejection>,
}

impl CancelOrdersOutcome {
    /// Accepted for cancellation (`data.success`). Accepted is not yet
    /// cancelled: watch `orderEvents` or query `orders` for the final state.
    pub fn cancelled(&self) -> &[OrderId] {
        &self.cancelled
    }

    /// Refused, with the exchange's reason (`data.failed`).
    pub fn rejected(&self) -> &[CancelRejection] {
        &self.rejected
    }

    /// Checks the postconditions above against what was sent.
    fn from_wire(
        sent: &Batch<OrderId, 10>,
        wire: WireCancelOrders<'_>,
    ) -> core::result::Result<Self, ProtocolErrorKind> {
        let violation = |p| ProtocolErrorKind::Postcondition(p);
        let mut seen: Vec<OrderId> = Vec::with_capacity(sent.as_slice().len());
        let mut note = |id: OrderId| {
            if !sent.contains(&id) {
                return Err(violation(Postcondition::CancelResultIdNotRequested(id)));
            }
            if seen.contains(&id) {
                return Err(violation(Postcondition::CancelResultIdRepeated(id)));
            }
            seen.push(id);
            Ok(())
        };
        for id in &wire.success {
            note(*id)?;
        }
        let mut rejected = Vec::with_capacity(wire.failed.len());
        for f in &wire.failed {
            note(f.order_id)?;
            let message = message(&f.message).ok_or(ProtocolErrorKind::MalformedErrorEnvelope)?;
            rejected.push(CancelRejection {
                order_id: f.order_id,
                message,
            });
        }
        Ok(Self {
            cancelled: wire.success,
            rejected,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelRejection {
    pub order_id: OrderId,
    pub message: ApiMessage,
}

#[derive(Deserialize)]
struct WireCancelOrders<'a> {
    success: Vec<OrderId>,
    #[serde(borrow)]
    failed: Vec<WireFailure<'a>>,
}

#[derive(Deserialize)]
struct WireFailure<'a> {
    #[serde(rename = "orderId")]
    order_id: OrderId,
    #[serde(flatten, borrow)]
    message: WireMessage<'a>,
}

impl Client<Authenticated> {
    /// `GET /private/v1/orders`.
    ///
    /// # Preconditions
    /// Carried by [`OrderLookup`]: exactly one id kind, 1 to 10 ids.
    ///
    /// # Postconditions
    /// Every returned order matches one requested id
    /// (`Protocol(Postcondition(OrderNotRequested))` otherwise). An id the
    /// exchange does not know is simply absent.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn orders(&self, lookup: &OrderLookup) -> Result<Vec<Order>> {
        let _ = lookup;
        todo!()
    }

    /// `GET /private/v1/activeOrders?symbol=&page=&count=`. The example of a
    /// paginated private GET.
    ///
    /// # Preconditions
    /// Carried by [`PageRequest`].
    ///
    /// # Postconditions
    /// At most `page.count()` items. An empty result is `Ok` with no items,
    /// whichever of the two empty shapes the exchange sent.
    ///
    /// # Errors
    /// `Api` (ERR-5003 when over the account's tier limit), `Protocol`,
    /// `Transport`.
    pub async fn active_orders(&self, symbol: Symbol, page: PageRequest) -> Result<Page<Order>> {
        let call = Call::new(endpoint!(GET "/v1/activeOrders"))
            .query("symbol", symbol)
            .query("page", page.page())
            .query("count", page.count());
        self.private(call).await?.data()
    }

    /// `GET /private/v1/executions`.
    ///
    /// # Preconditions
    /// Carried by [`ExecutionLookup`]: exactly one id kind, executionIds 1 to 10.
    ///
    /// # Postconditions
    /// Every returned execution matches the lookup.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn executions(&self, lookup: &ExecutionLookup) -> Result<Vec<Execution>> {
        let _ = lookup;
        todo!()
    }

    /// `GET /private/v1/latestExecutions?symbol=&page=&count=`: the last day.
    ///
    /// # Preconditions
    /// Carried by [`PageRequest`].
    ///
    /// # Postconditions
    /// At most `page.count()` items, newest first.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn latest_executions(
        &self,
        symbol: Symbol,
        page: PageRequest,
    ) -> Result<Page<Execution>> {
        let _ = (symbol, page);
        todo!()
    }

    /// `GET /private/v1/openPositions?symbol=&page=&count=`.
    ///
    /// # Preconditions
    /// `symbol` is a leverage symbol (`Contract(RequiresLeverageSymbol)`);
    /// `page` carried by [`PageRequest`].
    ///
    /// # Postconditions
    /// At most `page.count()` items, all for `symbol`.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn open_positions(
        &self,
        symbol: Symbol,
        page: PageRequest,
    ) -> Result<Page<Position>> {
        let _ = (symbol, page);
        todo!()
    }

    /// `GET /private/v1/positionSummary[?symbol=]`.
    ///
    /// # Preconditions
    /// `symbol`, when given, is a leverage symbol (`Contract(RequiresLeverageSymbol)`).
    ///
    /// # Postconditions
    /// One row per symbol and side held.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn position_summary(&self, symbol: Option<Symbol>) -> Result<Vec<PositionSummary>> {
        let _ = symbol;
        todo!()
    }

    /// `POST /private/v1/order`.
    ///
    /// # Preconditions
    /// Carried by [`NewOrder`] and [`OrderExecution`](super::OrderExecution)
    /// (price iff LIMIT/STOP, `timeInForce` only on LIMIT, a valid
    /// `ClientOrderId`), plus two checked here before sending:
    /// `MarketCancelBefore` needs a spot symbol and `Side::Sell`, and
    /// `losscut_price` needs a leverage symbol.
    ///
    /// # Postconditions
    /// `Ok(id)` means the exchange accepted the order, not that it filled.
    ///
    /// # Errors
    /// - `Contract(..)` for a failed precondition. Nothing was sent.
    /// - `Api` with ERR-915 when `client_order_id` was already used: the
    ///   order exists; look it up with `OrderLookup::ByClientOrderIds`.
    /// - `Transport` with `outcome_unknown() == true`: resend the same
    ///   `NewOrder` (same `client_order_id`). It is placed at most once.
    pub async fn place_order(&self, order: &NewOrder) -> Result<OrderId> {
        order.check()?;
        let call = Call::new(endpoint!(POST "/v1/order")).json(&order.wire());
        self.private(call).await?.data()
    }

    /// `POST /private/v1/changeOrder`.
    ///
    /// # Preconditions
    /// `losscut_price` only for leverage orders (the exchange answers ERR-5118 otherwise; the order's market is not known locally).
    ///
    /// # Postconditions
    /// `Ok` means the change was accepted.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn change_order(
        &self,
        order_id: OrderId,
        price: Price,
        losscut_price: Option<Price>,
    ) -> Result<()> {
        let _ = (order_id, price, losscut_price);
        todo!()
    }

    /// `POST /private/v1/cancelOrder`.
    ///
    /// # Preconditions
    /// None beyond the argument types.
    ///
    /// # Postconditions
    /// `Ok` means the cancel was accepted. The order may still fill before it
    /// is cancelled; the final state arrives as an `orderEvents` message.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors); ERR-5122 when the order can no longer be cancelled.
    pub async fn cancel_order(&self, order_id: OrderId) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            order_id: OrderId,
        }
        let call = Call::new(endpoint!(POST "/v1/cancelOrder")).json(&Body { order_id });
        self.private(call).await?.empty()
    }

    /// `POST /private/v1/cancelOrders`: up to 10 orders, partial success.
    ///
    /// # Preconditions
    /// Carried by `Batch<OrderId, 10>`: 1 to 10 distinct ids. Building a
    /// batch of 11 fails before any request exists.
    ///
    /// # Postconditions
    /// Every id in the outcome is one of `order_ids`, and none appears twice;
    /// otherwise `Protocol(Postcondition(..))`. A mix of cancelled and
    /// rejected ids is `Ok`.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn cancel_orders(
        &self,
        order_ids: &Batch<OrderId, 10>,
    ) -> Result<CancelOrdersOutcome> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            order_ids: &'a [OrderId],
        }
        let call = Call::new(endpoint!(POST "/v1/cancelOrders")).json(&Body {
            order_ids: order_ids.as_slice(),
        });
        let answer = self.private(call).await?;
        // TODO decode WireCancelOrders from the answer's raw data (borrowed),
        // then CancelOrdersOutcome::from_wire(order_ids, wire)
        //     .map_err(|kind| answer.protocol(kind))
        let _ = (answer, CancelOrdersOutcome::from_wire);
        todo!()
    }

    /// `POST /private/v1/cancelBulkOrder`: ids accepted for cancellation.
    ///
    /// # Preconditions
    /// `filter.symbols` is non-empty.
    ///
    /// # Postconditions
    /// At most 10 ids.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn cancel_bulk_orders(&self, filter: &BulkCancel) -> Result<Vec<OrderId>> {
        let _ = filter;
        todo!()
    }

    /// `POST /private/v1/closeOrder`.
    ///
    /// # Preconditions
    /// `request.symbol` is a leverage symbol (`Contract(RequiresLeverageSymbol)`).
    ///
    /// # Postconditions
    /// `Ok(id)` means the close order was accepted.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors); ERR-70 for the wrong closing side,
    /// ERR-254 for an unknown position, ERR-915 for a reused `client_order_id`.
    pub async fn close_position(&self, request: &ClosePosition) -> Result<OrderId> {
        if request.symbol.market() != Market::Leverage {
            return Err(ContractViolation::RequiresLeverageSymbol.into());
        }
        todo!()
    }

    /// `POST /private/v1/closeBulkOrder`.
    ///
    /// # Preconditions
    /// `request.symbol` is a leverage symbol (`Contract(RequiresLeverageSymbol)`).
    ///
    /// # Postconditions
    /// `Ok(id)` means the close order was accepted.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn close_bulk(&self, request: &BulkClose) -> Result<OrderId> {
        let _ = request;
        todo!()
    }

    /// `POST /private/v1/changeLosscutPrice`.
    ///
    /// # Preconditions
    /// None beyond the argument types.
    ///
    /// # Postconditions
    /// `Ok` means the change was accepted.
    ///
    /// # Errors
    /// The [REST set](crate::rest#errors).
    pub async fn change_losscut_price(
        &self,
        position_id: PositionId,
        losscut_price: Price,
    ) -> Result<()> {
        let _ = (position_id, losscut_price);
        todo!()
    }
}
