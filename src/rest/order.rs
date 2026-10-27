//! Order requests: what a caller may ask the exchange to do with money.
//!
//! Each order endpoint has its own grammar in the docs, so each has its own
//! execution enum. The variants encode the rules that depend only on the
//! request's own fields:
//!
//! | rule (from the docs)                                   | where it lives            |
//! |--------------------------------------------------------|---------------------------|
//! | LIMIT and STOP need `price`; MARKET must not have one   | variant fields            |
//! | `timeInForce` only on LIMIT                             | only `Limit` has the field|
//! | `SOK` (post-only) only on LIMIT                         | `LimitTimeInForce`        |
//! | `losscutPrice` only on LIMIT/STOP, and not on close     | variant fields            |
//! | `cancelBefore` only on MARKET (FAK)                     | `MarketCancelBefore` variant |
//! | `cancelBefore` not on `closeBulkOrder`                  | `BulkCloseExecution` lacks it |
//! | `closeOrder` settles exactly one position               | one `PositionId` field    |
//! | `clientOrderId` at most 36 alphanumerics                | `ClientOrderId`           |
//!
//! Two rules depend on the symbol's market, a property of data rather than
//! of the request's shape, so they are checked at the API boundary before
//! sending and fail with `Error::Contract`:
//! - `cancelBefore` on `/order` needs a spot symbol and `Side::Sell`.
//! - `losscutPrice` needs a leverage symbol.
//!
//! Rules that depend on exchange state (tick size, min and max size, today's
//! remaining limit, whether a position exists) are the exchange's to check.
//! This crate does not copy them, so they cannot go stale.

use serde::Serialize;

use crate::domain::{
    Batch, ClientOrderId, ExecutionId, LimitTimeInForce, Market, OrderId, PositionId, Price,
    SettleFilter, Side, Size, Symbol,
};
use crate::error::ContractViolation;

/// `POST /v1/order`: a spot buy or sell, or opening a leverage position.
///
/// Plain data with public fields: every field is a validated primitive, and
/// the cross-field rules are checked by `Client::place_order` before sending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOrder {
    /// Required here, though optional on the wire: it makes the order safely
    /// resendable after an unknown outcome. Use `ClientOrderId::generate()`.
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub size: Size,
    pub execution: OrderExecution,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderExecution {
    /// MARKET. The exchange executes it FAK.
    Market,
    /// MARKET with `cancelBefore: true`: cancel the oldest active orders
    /// until the size fits, then sell at market (FAK). Spot `Sell` only.
    MarketCancelBefore,
    /// LIMIT. `losscut_price` is for leverage symbols only.
    Limit {
        price: Price,
        time_in_force: LimitTimeInForce,
        losscut_price: Option<Price>,
    },
    /// STOP (a stop-market order triggered at `price`). The exchange executes
    /// it FAK. `losscut_price` is for leverage symbols only.
    Stop {
        price: Price,
        losscut_price: Option<Price>,
    },
}

impl NewOrder {
    /// The preconditions that the types cannot carry.
    ///
    /// # Errors
    /// - `CancelBeforeRequiresSpotSell` for `MarketCancelBefore` with a
    ///   leverage symbol or `Side::Buy`.
    /// - `LosscutRequiresLeverageSymbol` for a `losscut_price` on a spot symbol.
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

    /// The wire body. Fields absent by grammar are omitted, never sent as null.
    pub(crate) fn wire(&self) -> impl Serialize + '_ {
        // TODO WireOrder { symbol, side, executionType, timeInForce?, price?,
        // losscutPrice?, size, clientOrderId, cancelBefore? } with
        // #[serde(skip_serializing_if = "Option::is_none")] on the optionals.
        // MarketCancelBefore => executionType MARKET, cancelBefore true,
        // no timeInForce (the exchange's MARKET default is FAK, which is what
        // cancelBefore requires).
        WireOrder { order: self }
    }
}

struct WireOrder<'a> {
    order: &'a NewOrder,
}

impl Serialize for WireOrder<'_> {
    fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
        let _ = self.order;
        todo!()
    }
}

/// `POST /v1/closeOrder`: close one leverage position, fully or partly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClosePosition {
    pub client_order_id: ClientOrderId,
    /// Must be a leverage symbol (`Error::Contract(RequiresLeverageSymbol)`).
    pub symbol: Symbol,
    /// The closing side: the opposite of the position's side. The exchange
    /// checks it (ERR-70), since only it knows the position's side.
    pub side: Side,
    /// `settlePosition` takes exactly one position; this is that one.
    pub position_id: PositionId,
    pub size: Size,
    pub execution: CloseExecution,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloseExecution {
    Market,
    /// `cancelBefore: true` (MARKET, FAK). The exchange refuses it for orders
    /// tied to several positions.
    MarketCancelBefore,
    Limit {
        price: Price,
        time_in_force: LimitTimeInForce,
    },
    Stop {
        price: Price,
    },
}

/// `POST /v1/closeBulkOrder`: close positions of one symbol and side up to `size`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkClose {
    pub client_order_id: ClientOrderId,
    /// Must be a leverage symbol.
    pub symbol: Symbol,
    pub side: Side,
    pub size: Size,
    pub execution: BulkCloseExecution,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BulkCloseExecution {
    Market,
    Limit {
        price: Price,
        time_in_force: LimitTimeInForce,
    },
    Stop {
        price: Price,
    },
}

/// `POST /v1/cancelBulkOrder`: cancel up to 10 active orders by filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkCancel {
    pub symbols: Vec<Symbol>,
    pub side: Option<Side>,
    pub settle_type: Option<SettleFilter>,
    /// `desc`: cancel newest first instead of oldest first.
    pub newest_first: bool,
}

/// `GET /v1/orders`: exactly one of `orderId` or `clientOrderId`, up to 10
/// each. The enum makes "both" and "neither" unrepresentable; `Batch` makes
/// "more than 10" unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderLookup {
    ByOrderIds(Batch<OrderId, 10>),
    ByClientOrderIds(Batch<ClientOrderId, 10>),
}

/// `GET /v1/executions`: exactly one of `orderId` (one), `executionId` (up
/// to 10), or `clientOrderId` (one, no comma list).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionLookup {
    ByOrderId(OrderId),
    ByExecutionIds(Batch<ExecutionId, 10>),
    ByClientOrderId(ClientOrderId),
}
