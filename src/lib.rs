//! # gmo-coin-api-client
//!
//! [GMO コイン](https://api.coin.z.com/docs/) の取引所 API (Public/Private REST、
//! Public/Private WebSocket) を tokio 上で呼び出す非同期クライアント。
//!
//! ```no_run
//! use gmo_coin_api_client::{Client, ClientOrderId, Credentials, LimitTimeInForce, OrderPrice, OrderSize, Side, Symbol};
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
//!     size: OrderSize::parse("0.001")?,
//!     execution: OrderExecution::Limit {
//!         price: OrderPrice::parse("9000000")?,
//!         time_in_force: LimitTimeInForce::Fas,
//!         losscut_price: None,
//!     },
//! }).await?;
//! # let _ = id; Ok(()) }
//! ```
//!
//! ## モジュール構成
//!
//! | モジュール          | 所有するもの                                                   |
//! |---------------------|----------------------------------------------------------------|
//! | クレートルート      | `Client`、`Endpoints`、`Credentials`、`Config`、`Error`、ドメインプリミティブ |
//! | [`rest`]            | 32 の REST 操作すべてのリクエスト・応答型                       |
//! | [`ws`]              | `Feed`、チャネル、イベント                                     |
//! | [`error`]           | エラーの詳細型                                                 |
//! | `domain` (非公開)   | プリミティブの定義。ルートで再公開される                       |
//! | `secret` (非公開)   | キー、シークレット、トークン、署名                             |
//! | `transport` (非公開)| TLS コネクタ、hyper の HTTP/1.1 プールと往復、tokio-tungstenite の接続 |
//!
//! ## 設計上の約束
//!
//! - `Decimal` は `i128` の仮数と `u8` のスケールで、正確に比較し、受け取った
//!   文字列をそのまま出力する。十進数クレートは使わない。
//! - 秘密 (`ApiKey`、`ApiSecret`、`Credentials`、アクセストークン) は `Debug` で
//!   秘匿され、`Clone` できず、drop 時にゼロ埋めされる。
//! - 平文の `http`/`ws` はループバック IP リテラルに対してだけ受け付ける。
//! - REST の呼び出しは絞らない。ERR-5003 は `Error::is_rate_limited` で見える。

#![forbid(unsafe_code)]

mod client;
mod domain;
mod endpoint;
pub mod error;
pub mod rest;
mod secret;
mod transport;
pub mod ws;

// クレート内部の呼び出し (ws-auth) をモック取引所に対して試すため、
// 結合テスト用のモックを単体テストからも使えるようにする。
#[cfg(test)]
extern crate self as gmo_coin_api_client;
#[cfg(test)]
#[path = "../tests/support/mod.rs"]
pub(crate) mod mock;

pub use client::{
    Access, Anonymous, Authenticated, Backoff, ByteLimit, Client, Config, Limits, Timeout, Timeouts,
};
pub use domain::{
    Amount, Batch, BusinessDate, BusinessDateRange, CancelType, ClientOrderId,
    CryptoShortfallStatus, Decimal, ExchangeStatus, ExecutionId, ExecutionMsgType, ExecutionType,
    FeeType, HistoryWindow, IntradayInterval, InvalidValue, KlineSpan, LimitTimeInForce,
    LongInterval, MarginCallStatus, Market, OrderId, OrderMsgType, OrderPrice, OrderSize,
    OrderStatus, OrderType, PositionId, PositionMsgType, Price, Primitive, SettleFilter,
    SettleType, Side, Size, SummaryMsgType, Symbol, TimeInForce, Timestamp, TransferStatus,
    TransferType, UnknownValue, Violation,
};
pub use endpoint::{Endpoints, RestBase, WsBase};
pub use error::{Error, Result};
pub use rest::{Page, PageRequest, Pagination};
pub use secret::{ApiKey, ApiSecret, Credentials};
