//! # gmo-coin-api-client
//!
//! An async client for the [GMO Coin](https://api.coin.z.com/docs/) exchange
//! API: public and private REST, public and private WebSocket, on tokio.
//!
//! ```no_run
//! use gmo_coin_api_client::{Client, Credentials, Symbol, Price, Size, ClientOrderId, LimitTimeInForce, Side};
//! use gmo_coin_api_client::rest::{NewOrder, OrderExecution};
//!
//! # async fn run() -> gmo_coin_api_client::Result<()> {
//! let client = Client::production();
//! let btc = client.ticker(Symbol::BTC).await?;
//! println!("BTC last {}", btc.last);
//!
//! let client = client.authenticate(Credentials::from_env()?);
//! let id = client.place_order(&NewOrder {
//!     client_order_id: ClientOrderId::generate(),
//!     symbol: Symbol::BTC,
//!     side: Side::Buy,
//!     size: Size::parse("0.001")?,
//!     execution: OrderExecution::Limit {
//!         price: Price::parse("9000000")?,
//!         time_in_force: LimitTimeInForce::Fas,
//!         losscut_price: None,
//!     },
//! }).await?;
//! # let _ = id; Ok(()) }
//! ```
//!
//! ## Module map
//!
//! | module            | owns                                                         |
//! |-------------------|--------------------------------------------------------------|
//! | crate root        | `Client`, `Endpoints`, `Credentials`, `Config`, `Error`, domain primitives |
//! | [`rest`]          | request and response types for all 32 REST operations         |
//! | [`ws`]            | `Feed`, channels, events                                      |
//! | [`error`]         | error detail types                                            |
//! | `domain` (private)| primitive definitions, re-exported at the root               |
//! | `secret` (private)| key, secret, token, signing                                   |
//! | `wire` (private)  | TLS connector, HTTP/1.1 codec and pool, RFC 6455 codec        |

#![forbid(unsafe_code)]
// Sketch stage: bodies are `todo!()`, so most crate-private items have no caller yet.
#![allow(dead_code)]

mod client;
mod domain;
mod endpoint;
pub mod error;
pub mod rest;
mod secret;
mod wire;
pub mod ws;

pub use client::{
    Access, Anonymous, Authenticated, Backoff, ByteLimit, Client, Config, Limits, Timeout, Timeouts,
};
pub use domain::{
    Amount, Batch, BusinessDate, BusinessDateRange, CancelType, ClientOrderId,
    CryptoShortfallStatus, Decimal, ExchangeStatus, ExecutionId, ExecutionMsgType, ExecutionType,
    FeeType, HistoryWindow, IntradayInterval, InvalidValue, KlineSpan, LimitTimeInForce,
    LongInterval, MarginCallStatus, Market, OrderId, OrderMsgType, OrderStatus, OrderType,
    PositionId, PositionMsgType, Price, Primitive, SettleFilter, SettleType, Side, Size,
    SummaryMsgType, Symbol, TimeInForce, Timestamp, TransferStatus, TransferType, UnknownValue,
    Violation,
};
pub use endpoint::{Endpoints, RestBase, WsBase};
pub use error::{Error, Result};
pub use rest::{Page, PageRequest, Pagination};
pub use secret::{ApiKey, ApiSecret, Credentials};
