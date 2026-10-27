//! REST request and response types, and the one request path every endpoint
//! method uses.
//!
//! The endpoint methods are inherent methods on [`Client`], one per
//! documented operation, grouped by file:
//!
//! | file         | receiver                  | operations |
//! |--------------|---------------------------|------------|
//! | `public.rs`  | `Client<A>` for any `A`   | status, ticker, orderbooks, trades, klines, symbols |
//! | `account.rs` | `Client<Authenticated>`   | account/margin, account/assets, account/tradingVolume, account/fiatDeposit/history, account/fiatWithdrawal/history, account/deposit/history, account/withdrawal/history, account/exchangeFee/history, account/transfer |
//! | `trading.rs` | `Client<Authenticated>`   | orders, activeOrders, executions, latestExecutions, openPositions, positionSummary, order, changeOrder, cancelOrder, cancelOrders, cancelBulkOrder, closeOrder, closeBulkOrder, changeLosscutPrice |
//! | `ws_auth.rs` | crate-private             | ws-auth POST, PUT, DELETE (owned by the private feed) |
//!
//! # Errors
//!
//! Every REST method can fail with the same set, and each method's own
//! `# Errors` section adds only what is specific to it:
//! - `Transport`: no complete answer. Check `Error::outcome_unknown` before
//!   resending anything that moves money.
//! - `Protocol`: an answer that is not the documented shape (fail closed).
//! - `Api`: `status != 0`, for example ERR-5003 (`Error::is_rate_limited`),
//!   ERR-5201/5202 (maintenance), ERR-5008/5009 (local clock skew).
//!
//! Each method body is the same four steps: check the preconditions the
//! types could not carry, build a `Call`, send it (the envelope is checked
//! once, in `Client::send`), then decode `data` and check postconditions.

mod account;
mod envelope;
mod order;
mod public;
mod trading;
pub(crate) mod ws_auth;

pub use account::{
    Asset, CryptoTransfer, ExchangeFee, ExchangeFeePage, FiatTransfer, Margin, TradingVolume,
    VolumeLimit,
};
pub use order::{
    BulkCancel, BulkClose, BulkCloseExecution, CloseExecution, ClosePosition, ExecutionLookup,
    NewOrder, OrderExecution, OrderLookup,
};
pub use public::{Kline, OrderBook, OrderBookLevel, SymbolRule, Ticker, Trade};
pub use trading::{
    CancelOrdersOutcome, CancelRejection, Execution, Order, Position, PositionSummary,
};

use core::num::NonZeroU32;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use zeroize::Zeroizing;

use crate::client::{Access, Authenticated, Client};
use crate::domain::{InvalidValue, Primitive, Violation};
use crate::error::{ApiGroup, Error, ProtocolError, ProtocolErrorKind, Result};
use crate::secret::{Credentials, SignedPath};
use crate::wire::http::Method;

/// One documented operation.
#[derive(Clone, Copy)]
pub(crate) struct Endpoint {
    pub(crate) method: Method,
    pub(crate) path: SignedPath,
    /// `"POST /v1/order"`: the only operation identity errors carry.
    pub(crate) operation: &'static str,
}

/// Declares an endpoint once; the signed path and the error label are both
/// derived from the same literal, so they cannot disagree.
macro_rules! endpoint {
    (GET $path:literal) => { $crate::rest::endpoint!(@ Get "GET" $path) };
    (POST $path:literal) => { $crate::rest::endpoint!(@ Post "POST" $path) };
    (PUT $path:literal) => { $crate::rest::endpoint!(@ Put "PUT" $path) };
    (DELETE $path:literal) => { $crate::rest::endpoint!(@ Delete "DELETE" $path) };
    (@ $m:ident $name:literal $path:literal) => {
        $crate::rest::Endpoint {
            method: $crate::wire::http::Method::$m,
            path: $crate::secret::SignedPath::new($path),
            operation: concat!($name, " ", $path),
        }
    };
}
pub(crate) use endpoint;

/// A request ready to send. The body is serialized exactly once, here, and
/// the same buffer is signed and written. Zeroized on drop: the `ws-auth`
/// PUT and DELETE bodies hold the access token.
pub(crate) struct Call {
    pub(crate) endpoint: Endpoint,
    pub(crate) query: Vec<(&'static str, String)>,
    pub(crate) body: Zeroizing<Vec<u8>>,
}

impl Call {
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            query: Vec::new(),
            body: Zeroizing::new(Vec::new()),
        }
    }

    /// Query values come from domain primitives whose lexical rules exclude
    /// every byte that would need percent-encoding.
    pub(crate) fn query(mut self, key: &'static str, value: impl core::fmt::Display) -> Self {
        self.query.push((key, value.to_string()));
        self
    }

    /// # Postconditions
    /// `self.body` holds the JSON bytes that will be both signed and sent.
    ///
    /// # Panics
    /// If `body` fails to serialize, which request types built from domain
    /// primitives cannot (no non-string map keys, no floats). Panicking sends
    /// nothing; substituting an empty body would sign and send the wrong request.
    pub(crate) fn json<B: Serialize>(mut self, body: &B) -> Self {
        let bytes =
            serde_json::to_vec(body).expect("request bodies are built from domain primitives");
        self.body = Zeroizing::new(bytes);
        debug_assert!(
            self.body.first() == Some(&b'{'),
            "every documented request body is a JSON object"
        );
        self
    }
}

/// A success envelope (`status == 0`), with `data` not yet decoded.
pub(crate) struct Answer {
    api: ApiGroup,
    operation: &'static str,
    data: Option<Box<RawValue>>,
}

impl Answer {
    /// Decodes `data` as `T`.
    ///
    /// # Errors
    /// `Protocol(MissingData)` when `data` is absent; `Protocol(Shape)` when
    /// it does not match `T`. Never substitutes a default.
    pub(crate) fn data<T: DeserializeOwned>(self) -> Result<T> {
        let Some(raw) = &self.data else {
            return Err(self.protocol(ProtocolErrorKind::MissingData));
        };
        serde_json::from_str(raw.get()).map_err(|e| {
            self.protocol(ProtocolErrorKind::Shape {
                line: e.line(),
                column: e.column(),
            })
        })
    }

    /// For operations documented with no `data` (changeOrder, cancelOrder,
    /// changeLosscutPrice, ws-auth PUT and DELETE). A `data` field, if the
    /// exchange ever adds one, is ignored: it carries no state this call needs.
    pub(crate) fn empty(self) -> Result<()> {
        Ok(())
    }

    pub(crate) fn protocol(&self, kind: ProtocolErrorKind) -> Error {
        Error::Protocol(ProtocolError {
            api: self.api,
            operation: self.operation,
            kind,
        })
    }
}

impl<A: Access> Client<A> {
    /// Sends a public REST call.
    pub(crate) async fn public(&self, call: Call) -> Result<Answer> {
        self.send(ApiGroup::PublicRest, None, call).await
    }

    /// The single request path: build target, sign if `credentials`, round
    /// trip, then check the envelope.
    ///
    /// # Postconditions
    /// `Ok` only for `status == 0`. `status != 0` is `Error::Api`; anything
    /// that is not an envelope is `Error::Protocol`.
    async fn send(
        &self,
        api: ApiGroup,
        credentials: Option<&Credentials>,
        call: Call,
    ) -> Result<Answer> {
        // TODO
        // let (base, pool) = match api { PublicRest => (&endpoints.public_rest, &public_pool), PrivateRest => (private..) };
        // let target = format!("{}{}{}", base.prefix, call.endpoint.path.as_str(), encode_query(&call.query));
        // let route = Route { api, operation: call.endpoint.operation, method: call.endpoint.method, base, pool };
        // let response = wire::http::round_trip(&inner.connector, &route, &config,
        //     &mut |out| {   // re-run on a stale-connection GET retry: fresh timestamp, fresh signature
        //         let auth = credentials.map(|c| c.sign_request(now_ms(), method, call.endpoint.path, &call.body));
        //         encode_request(&host, &Request { method, target: &target, auth, body: &call.body }, out);
        //     }).await.map_err(Error::Transport)?;
        // envelope::parse(api, call.endpoint.operation, response.status, &response.body)
        let _ = (api, credentials, call, envelope::parse);
        todo!()
    }
}

impl Client<Authenticated> {
    /// Sends a signed private REST call.
    pub(crate) async fn private(&self, call: Call) -> Result<Answer> {
        let credentials = self.access.0.clone();
        self.send(ApiGroup::PrivateRest, Some(&credentials), call)
            .await
    }
}

/// `page` and `count` for the page-numbered lists (trades, activeOrders,
/// latestExecutions, openPositions).
///
/// # Invariants
/// `page >= 1`, `1 <= count <= 100`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRequest {
    page: NonZeroU32,
    count: u8,
}

impl PageRequest {
    pub const MAX_COUNT: u8 = 100;

    /// Page 1, 100 items: the exchange's own default.
    pub const FIRST: PageRequest = PageRequest {
        page: NonZeroU32::MIN,
        count: Self::MAX_COUNT,
    };

    /// A page number and a page size.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `page() == page` and `count() == count`.
    /// # Errors
    /// `Semantics` when `page == 0` or `count` is outside `1..=100`.
    pub fn new(page: u32, count: u8) -> core::result::Result<Self, InvalidValue> {
        let invalid = |why| InvalidValue {
            primitive: Primitive::PageRequest,
            violation: Violation::Semantics(why),
        };
        let page = NonZeroU32::new(page).ok_or(invalid("page 0"))?;
        if count == 0 || count > Self::MAX_COUNT {
            return Err(invalid("count outside 1..=100"));
        }
        Ok(Self { page, count })
    }

    pub fn next(self) -> Option<Self> {
        self.page.checked_add(1).map(|page| Self { page, ..self })
    }

    pub fn page(self) -> u32 {
        self.page.get()
    }

    pub fn count(self) -> u8 {
        self.count
    }
}

/// One page of a page-numbered list.
///
/// The exchange sends two shapes and both are accepted explicitly:
/// `{"pagination": {...}, "list": [...]}`, and `{}` for an empty result.
/// The second is not in the official docs; it is how the production API is
/// recalled to answer an empty `activeOrders` and must be confirmed against
/// production before release (see DESIGN.md, open questions). Any other
/// shape is a `Protocol` error.
///
/// # Invariants
/// `pagination.is_none()` implies `items.is_empty()`.
#[derive(Clone, Debug)]
pub struct Page<T> {
    items: Vec<T>,
    pagination: Option<Pagination>,
}

impl<T> Page<T> {
    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn into_items(self) -> Vec<T> {
        self.items
    }

    /// `None` exactly when the exchange answered `{}`.
    pub fn pagination(&self) -> Option<Pagination> {
        self.pagination
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pagination {
    pub current_page: u32,
    pub count: u32,
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Page<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        // TODO visitor over a map: no keys => Page { items: vec![], pagination: None };
        // exactly {pagination, list} (other keys ignored) => Some(..);
        // only one of the two => error "pagination and list must come together".
        let _ = d;
        todo!()
    }
}
