//! Private REST: 注文・約定・建玉。このファイルのメソッドはお金を動かすか
//! 口座の状態を明かすので、`Client<Authenticated>` にだけ存在する。
//!
//! # パニック
//! このファイルの関数はパニックしない。
//!
//! # 例
//! ```no_run
//! # async fn run() -> gmo_coin_api_client::Result<()> {
//! use gmo_coin_api_client::{Client, Credentials, PageRequest, Symbol};
//!
//! let client = Client::production().authenticate(Credentials::from_env()?);
//! let active = client.active_orders(Symbol::BTC, PageRequest::FIRST).await?;
//! println!("{}", active.items().len());
//! # Ok(()) }
//! ```

use serde::{Deserialize, Serialize};

use super::envelope::{WireMessage, message};
use super::order::{
    BulkCancel, BulkClose, ClosePosition, ExecutionLookup, NewOrder, OrderLookup, require_leverage,
};
use super::{Answer, Call, Page, PageRequest, WireListing, endpoint};
use crate::client::{Authenticated, Client};
use crate::domain::{
    Batch, CancelType, ClientOrderId, Decimal, ExecutionId, ExecutionType, OrderId, OrderPrice,
    OrderStatus, OrderType, PositionId, Price, SettleType, Side, Size, Symbol, TimeInForce,
    Timestamp,
};
use crate::error::{ApiMessage, Postcondition, ProtocolErrorKind, Result};

/// 取引所が報告する注文 (`GET /orders`、`GET /activeOrders`)。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub root_order_id: OrderId,
    pub order_id: OrderId,
    /// 注文時に指定したときだけ存在する。
    pub client_order_id: Option<ClientOrderId>,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub execution_type: ExecutionType,
    pub settle_type: SettleType,
    pub size: Size,
    pub executed_size: Size,
    /// MARKET 注文では存在しない。
    pub price: Option<Price>,
    /// 現物注文と未設定のときは `"0"`。
    pub losscut_price: Price,
    pub status: OrderStatus,
    /// CANCELLING、CANCELED、EXPIRED のときだけ存在する。
    pub cancel_type: Option<CancelType>,
    pub time_in_force: TimeInForce,
    pub timestamp: Timestamp,
}

/// 約定 (`GET /executions`、`GET /latestExecutions`)。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Execution {
    pub execution_id: ExecutionId,
    pub order_id: OrderId,
    pub client_order_id: Option<ClientOrderId>,
    /// レバレッジのみ。
    pub position_id: Option<PositionId>,
    pub symbol: Symbol,
    pub side: Side,
    pub settle_type: SettleType,
    pub size: Size,
    pub price: Price,
    pub loss_gain: Decimal,
    /// taker は正、maker は負。
    pub fee: Decimal,
    pub timestamp: Timestamp,
}

/// 建玉中のレバレッジ建玉 (`GET /openPositions`)。
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub position_id: PositionId,
    pub symbol: Symbol,
    pub side: Side,
    pub size: Size,
    /// ワイヤ上の綴りは `orderdSize`。
    #[serde(rename = "orderdSize")]
    pub ordered_size: Size,
    pub price: Price,
    pub loss_gain: Decimal,
    pub leverage: Decimal,
    pub losscut_price: Price,
    pub timestamp: Timestamp,
}

/// `GET /positionSummary`。銘柄と売買区分ごとに 1 行。
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

/// `cancelOrders` の部分成功。エラーではない。取り消せる id と取り消せない id が
/// 混ざり得るので、呼び出し側は両方のリストを必要とする。
///
/// # 不変条件
/// 答えをパースするときに検査する。
/// - `cancelled` と `rejected` の id はどれも、呼び出し側が送ったもの。
/// - 同じ id が 2 つのリストをまたいで二度現れない。
/// - `unaccounted` は、送ったのにどちらのリストにも現れなかった id。取消済みと
///   みなしてはならない。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrdersOutcome {
    cancelled: Vec<OrderId>,
    rejected: Vec<CancelRejection>,
    unaccounted: Vec<OrderId>,
}

impl CancelOrdersOutcome {
    /// 取消を受け付けた id (`data.success`)。受付は取消済みではない。最終状態は
    /// `orderEvents` を見るか `orders` で調べる。
    pub fn cancelled(&self) -> &[OrderId] {
        &self.cancelled
    }

    /// 取引所の理由つきで拒否された id (`data.failed`)。
    pub fn rejected(&self) -> &[CancelRejection] {
        &self.rejected
    }

    /// 送ったのに `success` にも `failed` にも現れなかった id。取引所は結果を
    /// 答えていないので、取消済みとみなさず、`orders` で調べること。
    pub fn unaccounted(&self) -> &[OrderId] {
        &self.unaccounted
    }

    fn from_wire(
        sent: &Batch<OrderId, 10>,
        wire: WireCancelOrders<'_>,
    ) -> core::result::Result<Self, ProtocolErrorKind> {
        let mut failed = Vec::with_capacity(wire.failed.len());
        for f in &wire.failed {
            let message = message(&f.message).ok_or(ProtocolErrorKind::Postcondition(
                Postcondition::CancelRejectionMalformed(f.order_id),
            ))?;
            failed.push(CancelRejection {
                order_id: f.order_id,
                message,
            });
        }
        check_cancel_outcome(sent.as_slice(), wire.success, failed)
            .map_err(ProtocolErrorKind::Postcondition)
    }
}

/// `cancelBulkOrder` が返した id の事後条件を検査する純粋関数。
///
/// # 事後条件
/// `Ok` なら、id は 10 件以下で、どれも二度現れない。
///
/// # エラー
/// - `BulkCancelTooManyIds`: 11 件以上ある。
/// - `BulkCancelIdRepeated`: 同じ id が二度ある。
fn check_bulk_cancel_ids(ids: &[OrderId]) -> core::result::Result<(), Postcondition> {
    if ids.len() > 10 {
        return Err(Postcondition::BulkCancelTooManyIds { actual: ids.len() });
    }
    let mut remaining = ids;
    while let Some((id, rest)) = remaining.split_first() {
        if rest.contains(id) {
            return Err(Postcondition::BulkCancelIdRepeated(*id));
        }
        remaining = rest;
    }
    Ok(())
}

/// `cancelOrders` の事後条件を、送った id に照らして検査する純粋関数。
///
/// # 事前条件
/// `sent` は呼び出し側が送った id (重複なし)。
///
/// # 事後条件
/// `Ok` なら、`success` と `failed` のすべての id は `sent` にあり、どの id も
/// 二度現れず、`unaccounted` は `sent` のうちどちらにも現れなかった id を
/// `sent` の順に並べたもの。
///
/// # エラー
/// - `CancelResultIdNotRequested`: 送っていない id が結果にある。
/// - `CancelResultIdRepeated`: 同じ id が二度ある (`success` の中、`failed` の
///   中、またはその間)。
fn check_cancel_outcome(
    sent: &[OrderId],
    success: Vec<OrderId>,
    failed: Vec<CancelRejection>,
) -> core::result::Result<CancelOrdersOutcome, Postcondition> {
    let mut seen: Vec<OrderId> = Vec::with_capacity(sent.len());
    let ids = success
        .iter()
        .copied()
        .chain(failed.iter().map(|f| f.order_id));
    for id in ids {
        if !sent.contains(&id) {
            return Err(Postcondition::CancelResultIdNotRequested(id));
        }
        if seen.contains(&id) {
            return Err(Postcondition::CancelResultIdRepeated(id));
        }
        seen.push(id);
    }
    let unaccounted = sent
        .iter()
        .copied()
        .filter(|id| !seen.contains(id))
        .collect();
    Ok(CancelOrdersOutcome {
        cancelled: success,
        rejected: failed,
        unaccounted,
    })
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

/// ページ番号を持たない `{"list": [...]}` の形 (`orders`、`executions`、
/// `positionSummary`)。`Page` と同じく、空の結果の `{}` も受け付ける。
/// 公式ドキュメントにない形で、本番で確かめる必要がある (未解決)。
struct List<T> {
    list: Vec<T>,
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for List<T> {
    /// `list` があればそれで、他のキーは無視する。`{}` は空のリスト。
    /// `list` を欠いて他のキーを持つオブジェクトは形の誤り。
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        let wire = WireListing::<T>::deserialize(d)?;
        match wire.list {
            Some(list) => Ok(List { list }),
            None if wire.pagination.is_none() && wire.other_keys == 0 => {
                Ok(List { list: Vec::new() })
            }
            None => Err(serde::de::Error::custom(
                "expected a list or an empty object",
            )),
        }
    }
}

impl Client<Authenticated> {
    /// `GET /private/v1/orders`。
    ///
    /// # 事前条件
    /// [`OrderLookup`] が保証する: id の種類はちょうど 1 つ、1 件から 10 件。
    ///
    /// # 事後条件
    /// 返された注文はどれも要求した id のどれかに一致する (そうでなければ
    /// `Protocol(Postcondition(OrderNotRequested))`)。取引所が知らない id は
    /// 単に含まれない。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{Batch, ClientOrderId};
    /// use gmo_coin_api_client::rest::OrderLookup;
    /// let ids = Batch::one(ClientOrderId::parse("abc123")?);
    /// let orders = client.orders(&OrderLookup::ByClientOrderIds(ids)).await?;
    /// println!("{}", orders.len());
    /// # Ok(()) }
    /// ```
    pub async fn orders(&self, lookup: &OrderLookup) -> Result<Vec<Order>> {
        let (key, value) = lookup.query();
        let call = Call::new(endpoint!(GET "/v1/orders")).query(key, value);
        let answer = self.private(call).await?;
        let wrong = |order: &Order| {
            answer.protocol(ProtocolErrorKind::Postcondition(
                Postcondition::OrderNotRequested(order.order_id),
            ))
        };
        let orders: Vec<Order> = decode_list(&answer)?;
        let requested = |order: &Order| match lookup {
            OrderLookup::ByOrderIds(ids) => ids.contains(&order.order_id),
            OrderLookup::ByClientOrderIds(ids) => order
                .client_order_id
                .as_ref()
                .is_some_and(|id| ids.contains(id)),
        };
        match orders.iter().find(|o| !requested(o)) {
            Some(stray) => Err(wrong(stray)),
            None => Ok(orders),
        }
    }

    /// `GET /private/v1/activeOrders?symbol=&page=&count=`。ページ番号で区切られた
    /// Private GET の例。
    ///
    /// # 事前条件
    /// [`PageRequest`] が保証する。
    ///
    /// # 事後条件
    /// 空の結果は、取引所がどちらの空の形で答えても、項目なしの `Ok`。
    ///
    /// # エラー
    /// `Api` (口座の階層の上限を超えると ERR-5003)、`Protocol`、`Transport`。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{PageRequest, Symbol};
    /// let page = client.active_orders(Symbol::BTC, PageRequest::FIRST).await?;
    /// println!("{}", page.items().len());
    /// # Ok(()) }
    /// ```
    pub async fn active_orders(&self, symbol: Symbol, page: PageRequest) -> Result<Page<Order>> {
        let call = Call::new(endpoint!(GET "/v1/activeOrders"))
            .query("symbol", symbol)
            .query("page", page.page())
            .query("count", page.count());
        self.private(call).await?.data()
    }

    /// `GET /private/v1/executions`。
    ///
    /// # 事前条件
    /// [`ExecutionLookup`] が保証する: id の種類はちょうど 1 つ、`executionId` は
    /// 1 件から 10 件。
    ///
    /// # 事後条件
    /// 返された約定はどれも検索条件に一致する。注文 id の検索なら `orderId`、
    /// クライアント注文 id の検索なら `clientOrderId` (応答に無ければ不一致)、
    /// 約定 id の検索なら `executionId` が要求のどれかと等しい。そうでなければ
    /// `Protocol(Postcondition(ExecutionNotRequested))`。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::OrderId;
    /// use gmo_coin_api_client::rest::ExecutionLookup;
    /// let fills = client.executions(&ExecutionLookup::ByOrderId(OrderId::new(223456789)?)).await?;
    /// println!("{}", fills.len());
    /// # Ok(()) }
    /// ```
    pub async fn executions(&self, lookup: &ExecutionLookup) -> Result<Vec<Execution>> {
        let (key, value) = lookup.query();
        let call = Call::new(endpoint!(GET "/v1/executions")).query(key, value);
        let answer = self.private(call).await?;
        let executions: Vec<Execution> = decode_list(&answer)?;
        let requested = |e: &Execution| match lookup {
            ExecutionLookup::ByOrderId(id) => e.order_id == *id,
            ExecutionLookup::ByExecutionIds(ids) => ids.contains(&e.execution_id),
            ExecutionLookup::ByClientOrderId(id) => e.client_order_id.as_ref() == Some(id),
        };
        match executions.iter().find(|e| !requested(e)) {
            Some(stray) => Err(answer.protocol(ProtocolErrorKind::Postcondition(
                Postcondition::ExecutionNotRequested(stray.execution_id),
            ))),
            None => Ok(executions),
        }
    }

    /// `GET /private/v1/latestExecutions?symbol=&page=&count=`: 直近 1 日。
    ///
    /// # 事前条件
    /// [`PageRequest`] が保証する。
    ///
    /// # 事後条件
    /// 送られたままの順序 (取引所は新しい順で返す)。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{PageRequest, Symbol};
    /// let page = client.latest_executions(Symbol::BTC, PageRequest::FIRST).await?;
    /// println!("{}", page.items().len());
    /// # Ok(()) }
    /// ```
    pub async fn latest_executions(
        &self,
        symbol: Symbol,
        page: PageRequest,
    ) -> Result<Page<Execution>> {
        let call = Call::new(endpoint!(GET "/v1/latestExecutions"))
            .query("symbol", symbol)
            .query("page", page.page())
            .query("count", page.count());
        self.private(call).await?.data()
    }

    /// `GET /private/v1/openPositions?symbol=&page=&count=`。
    ///
    /// # 事前条件
    /// `symbol` はレバレッジ銘柄 (そうでなければ
    /// `Contract(RequiresLeverageSymbol)`)。`page` は [`PageRequest`] が保証する。
    ///
    /// # 事後条件
    /// 空の結果は項目なしの `Ok`。
    ///
    /// # エラー
    /// `Contract`: 現物銘柄。リクエストは作られない。ほかは
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{PageRequest, Symbol};
    /// let page = client.open_positions(Symbol::BTC_JPY, PageRequest::FIRST).await?;
    /// println!("{}", page.items().len());
    /// # Ok(()) }
    /// ```
    pub async fn open_positions(
        &self,
        symbol: Symbol,
        page: PageRequest,
    ) -> Result<Page<Position>> {
        require_leverage(&symbol)?;
        let call = Call::new(endpoint!(GET "/v1/openPositions"))
            .query("symbol", symbol)
            .query("page", page.page())
            .query("count", page.count());
        self.private(call).await?.data()
    }

    /// `GET /private/v1/positionSummary[?symbol=]`。
    ///
    /// # 事前条件
    /// 指定する場合、`symbol` はレバレッジ銘柄 (そうでなければ
    /// `Contract(RequiresLeverageSymbol)`)。
    ///
    /// # 事後条件
    /// 保有している銘柄と売買区分ごとに 1 行。
    ///
    /// # エラー
    /// `Contract`: 現物銘柄。リクエストは作られない。ほかは
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// let rows = client.position_summary(None).await?;
    /// println!("{}", rows.len());
    /// # Ok(()) }
    /// ```
    pub async fn position_summary(&self, symbol: Option<Symbol>) -> Result<Vec<PositionSummary>> {
        let mut call = Call::new(endpoint!(GET "/v1/positionSummary"));
        if let Some(symbol) = symbol {
            require_leverage(&symbol)?;
            call = call.query("symbol", symbol);
        }
        decode_list(&self.private(call).await?)
    }

    /// `POST /private/v1/order`。
    ///
    /// # 事前条件
    /// [`NewOrder`] と [`OrderExecution`](super::OrderExecution) が保証する
    /// (LIMIT/STOP にだけ価格、LIMIT にだけ `timeInForce`、ゼロでない数量と価格、
    /// 正しい `ClientOrderId`)。加えて、送信前に次の 2 つを検査する。
    /// `MarketCancelBefore` は現物銘柄の `Side::Sell` のみ、`losscut_price` は
    /// レバレッジ銘柄のみ。
    ///
    /// # 事後条件
    /// `Ok(id)` は、取引所が注文を受け付けたことを意味する。約定したことではない。
    ///
    /// # エラー
    /// - `Contract(..)`: 事前条件の違反。何も送られていない。
    /// - `Api` の ERR-915: `client_order_id` が使用済み。注文は存在するので、
    ///   `OrderLookup::ByClientOrderIds` で調べる。
    /// - `outcome_unknown() == true` (`Transport` の `MaybeSent`、または答えを
    ///   読めなかった `Protocol`): 同じ `NewOrder` (同じ `client_order_id`) を
    ///   再送する。発注されるのは高々一度で、最初の試行が発注されていれば
    ///   再送は ERR-915 で拒否される。`accepted_but_unreadable()` も真なら
    ///   発注は確定しているので、再送せずに照会してよい。
    ///
    /// # 例
    /// 結果不明なら同じ注文を再送し、ERR-915 なら顧客注文 ID で照会する。
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> Result<(), Box<dyn std::error::Error>> {
    /// use gmo_coin_api_client::error::ErrorCode;
    /// use gmo_coin_api_client::rest::{NewOrder, OrderExecution, OrderLookup};
    /// use gmo_coin_api_client::{Batch, ClientOrderId, Error, LimitTimeInForce, OrderPrice, OrderSize, Side, Symbol};
    /// let order = NewOrder {
    ///     client_order_id: ClientOrderId::generate(),
    ///     symbol: Symbol::BTC,
    ///     side: Side::Buy,
    ///     size: OrderSize::parse("0.001")?,
    ///     execution: OrderExecution::Limit {
    ///         price: OrderPrice::parse("9000000")?,
    ///         time_in_force: LimitTimeInForce::Fas,
    ///         losscut_price: None,
    ///     },
    /// };
    /// let id = match client.place_order(&order).await {
    ///     Ok(id) => id,
    ///     Err(e) if e.outcome_unknown() => match client.place_order(&order).await {
    ///         Ok(id) => id,
    ///         Err(Error::Api(api)) if api.has(ErrorCode::CLIENT_ORDER_ID_USED) => {
    ///             let lookup = OrderLookup::ByClientOrderIds(Batch::one(order.client_order_id.clone()));
    ///             let orders = client.orders(&lookup).await?;
    ///             orders.first().ok_or("ERR-915 なのに注文が照会できない")?.order_id
    ///         }
    ///         Err(e) => return Err(e.into()),
    ///     },
    ///     Err(e) => return Err(e.into()),
    /// };
    /// println!("{id}");
    /// # Ok(()) }
    /// ```
    pub async fn place_order(&self, order: &NewOrder) -> Result<OrderId> {
        order.check()?;
        let call = Call::new(endpoint!(POST "/v1/order")).json(&order.wire());
        self.private(call).await?.data()
    }

    /// `POST /private/v1/changeOrder`。
    ///
    /// # 事前条件
    /// `price` と `losscut_price` はゼロでない ([`OrderPrice`])。`losscut_price` は
    /// レバレッジの注文のみ。注文の市場はローカルでは分からないので、
    /// 取引所が ERR-5118 で答える。
    ///
    /// # 事後条件
    /// `Ok` は、変更が受け付けられたことを意味する。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{OrderId, OrderPrice};
    /// client.change_order(OrderId::new(123456789)?, OrderPrice::parse("9100000")?, None).await?;
    /// # Ok(()) }
    /// ```
    pub async fn change_order(
        &self,
        order_id: OrderId,
        price: OrderPrice,
        losscut_price: Option<OrderPrice>,
    ) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            order_id: OrderId,
            price: OrderPrice,
            #[serde(skip_serializing_if = "Option::is_none")]
            losscut_price: Option<OrderPrice>,
        }
        let call = Call::new(endpoint!(POST "/v1/changeOrder")).json(&Body {
            order_id,
            price,
            losscut_price,
        });
        self.private(call).await?.empty()
    }

    /// `POST /private/v1/cancelOrder`。
    ///
    /// # 事前条件
    /// 引数の型以外になし。
    ///
    /// # 事後条件
    /// `Ok` は、取消が受け付けられたことを意味する。取り消される前に約定する
    /// こともある。最終状態は `orderEvents` のメッセージで届く。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。注文がもう取り消せないとき ERR-5122。
    /// `outcome_unknown()` なら取消は受け付けられたかもしれない。同じ id で
    /// 再送してよい。取消済みの注文には ERR-5122 が返るだけで害はない。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// client.cancel_order(gmo_coin_api_client::OrderId::new(123456789)?).await?;
    /// # Ok(()) }
    /// ```
    pub async fn cancel_order(&self, order_id: OrderId) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            order_id: OrderId,
        }
        let call = Call::new(endpoint!(POST "/v1/cancelOrder")).json(&Body { order_id });
        self.private(call).await?.empty()
    }

    /// `POST /private/v1/cancelOrders`: 最大 10 件、部分成功あり。
    ///
    /// # 事前条件
    /// `Batch<OrderId, 10>` が保証する: 1 件から 10 件の相異なる id。11 件の
    /// バッチは、リクエストが存在する前に作れない。
    ///
    /// # 事後条件
    /// 結果の id はどれも `order_ids` のどれかで、二度現れない (そうでなければ
    /// `Protocol(Postcondition(..))`)。取消済みと拒否が混ざっていても `Ok`。
    /// 送ったのに結果に現れなかった id は `unaccounted()` に入る。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。`outcome_unknown()` なら、どの id が
    /// 取り消されたかは分からない。同じバッチを再送するか、`orders` で調べる。
    /// 事後条件違反 (`accepted_but_unreadable()`) は取引所が取消を処理した後
    /// なので、`orders` で調べる。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{Batch, OrderId};
    /// let ids = Batch::new(vec![OrderId::new(1)?, OrderId::new(2)?])?;
    /// let outcome = client.cancel_orders(&ids).await?;
    /// println!("{} 件受付, {} 件拒否", outcome.cancelled().len(), outcome.rejected().len());
    /// # Ok(()) }
    /// ```
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
        let wire: WireCancelOrders<'_> = answer.data()?;
        CancelOrdersOutcome::from_wire(order_ids, wire).map_err(|kind| answer.protocol(kind))
    }

    /// `POST /private/v1/cancelBulkOrder`: 取消を受け付けた id。
    ///
    /// # 事前条件
    /// `filter.symbols` は空でない (そうでなければ `Contract(InvalidValue)`)。
    ///
    /// # 事後条件
    /// 取引所は 1 回に最大 10 件を取り消すので、返る id は 10 件以下で重複しない。
    /// 11 件以上なら `Protocol(Postcondition(BulkCancelTooManyIds))`、同じ id が
    /// 二度あれば `Protocol(Postcondition(BulkCancelIdRepeated))`。
    ///
    /// # エラー
    /// `Contract`: `symbols` が空。リクエストは作られない。ほかは
    /// [REST 共通の集合](crate::rest#エラー)。`outcome_unknown()` なら、取り消された
    /// id は分からない。`active_orders` で残りを調べてから再送する。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::Symbol;
    /// use gmo_coin_api_client::rest::BulkCancel;
    /// let filter = BulkCancel { symbols: vec![Symbol::BTC], side: None, settle_type: None, newest_first: false };
    /// let ids = client.cancel_bulk_orders(&filter).await?;
    /// println!("{}", ids.len());
    /// # Ok(()) }
    /// ```
    pub async fn cancel_bulk_orders(&self, filter: &BulkCancel) -> Result<Vec<OrderId>> {
        filter.check()?;
        let call = Call::new(endpoint!(POST "/v1/cancelBulkOrder")).json(&filter.wire());
        let answer = self.private(call).await?;
        let ids: Vec<OrderId> = answer.data()?;
        check_bulk_cancel_ids(&ids)
            .map_err(|p| answer.protocol(ProtocolErrorKind::Postcondition(p)))?;
        Ok(ids)
    }

    /// `POST /private/v1/closeOrder`。
    ///
    /// # 事前条件
    /// `request.symbol` はレバレッジ銘柄 (そうでなければ
    /// `Contract(RequiresLeverageSymbol)`)。
    ///
    /// # 事後条件
    /// `Ok(id)` は、決済注文が受け付けられたことを意味する。
    ///
    /// # エラー
    /// `Contract`: 現物銘柄。リクエストは作られない。ほかは
    /// [REST 共通の集合](crate::rest#エラー)。決済する側が違うとき ERR-70、
    /// 建玉が存在しないとき ERR-254、`client_order_id` の再利用で ERR-915。
    /// `outcome_unknown()` のときは `place_order` と同じ手順: 同じ
    /// `client_order_id` で再送し、ERR-915 なら `orders` で照会する。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{ClientOrderId, OrderSize, PositionId, Side, Symbol};
    /// use gmo_coin_api_client::rest::{CloseExecution, ClosePosition};
    /// let id = client.close_position(&ClosePosition {
    ///     client_order_id: ClientOrderId::generate(),
    ///     symbol: Symbol::BTC_JPY,
    ///     side: Side::Sell,
    ///     position_id: PositionId::new(1234567)?,
    ///     size: OrderSize::parse("0.01")?,
    ///     execution: CloseExecution::Market,
    /// }).await?;
    /// println!("{id}");
    /// # Ok(()) }
    /// ```
    pub async fn close_position(&self, request: &ClosePosition) -> Result<OrderId> {
        request.check()?;
        let call = Call::new(endpoint!(POST "/v1/closeOrder")).json(&request.wire());
        self.private(call).await?.data()
    }

    /// `POST /private/v1/closeBulkOrder`。
    ///
    /// # 事前条件
    /// `request.symbol` はレバレッジ銘柄 (そうでなければ
    /// `Contract(RequiresLeverageSymbol)`)。
    ///
    /// # 事後条件
    /// `Ok(id)` は、決済注文が受け付けられたことを意味する。
    ///
    /// # エラー
    /// `Contract`: 現物銘柄。リクエストは作られない。ほかは
    /// [REST 共通の集合](crate::rest#エラー)。`outcome_unknown()` のときは
    /// `place_order` と同じ手順: 同じ `client_order_id` で再送し、ERR-915 なら
    /// `orders` で照会する。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{ClientOrderId, OrderSize, Side, Symbol};
    /// use gmo_coin_api_client::rest::{BulkClose, BulkCloseExecution};
    /// let id = client.close_bulk(&BulkClose {
    ///     client_order_id: ClientOrderId::generate(),
    ///     symbol: Symbol::BTC_JPY,
    ///     side: Side::Sell,
    ///     size: OrderSize::parse("0.5")?,
    ///     execution: BulkCloseExecution::Market,
    /// }).await?;
    /// println!("{id}");
    /// # Ok(()) }
    /// ```
    pub async fn close_bulk(&self, request: &BulkClose) -> Result<OrderId> {
        request.check()?;
        let call = Call::new(endpoint!(POST "/v1/closeBulkOrder")).json(&request.wire());
        self.private(call).await?.data()
    }

    /// `POST /private/v1/changeLosscutPrice`。
    ///
    /// # 事前条件
    /// `losscut_price` はゼロでない ([`OrderPrice`])。
    ///
    /// # 事後条件
    /// `Ok` は、変更が受け付けられたことを意味する。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    ///
    /// # 例
    /// ```no_run
    /// # async fn run(client: gmo_coin_api_client::Client<gmo_coin_api_client::Authenticated>) -> gmo_coin_api_client::Result<()> {
    /// use gmo_coin_api_client::{OrderPrice, PositionId};
    /// client.change_losscut_price(PositionId::new(1234567)?, OrderPrice::parse("7000000")?).await?;
    /// # Ok(()) }
    /// ```
    pub async fn change_losscut_price(
        &self,
        position_id: PositionId,
        losscut_price: OrderPrice,
    ) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            position_id: PositionId,
            losscut_price: OrderPrice,
        }
        let call = Call::new(endpoint!(POST "/v1/changeLosscutPrice")).json(&Body {
            position_id,
            losscut_price,
        });
        self.private(call).await?.empty()
    }
}

/// `{"list": [...]}` の `list`。
fn decode_list<T: serde::de::DeserializeOwned>(answer: &Answer) -> Result<Vec<T>> {
    answer.data::<List<T>>().map(|l| l.list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode as Code;

    fn id(n: u64) -> OrderId {
        OrderId::new(n).unwrap()
    }

    fn ids(ns: &[u64]) -> Vec<OrderId> {
        ns.iter().map(|&n| id(n)).collect()
    }

    fn rejection(n: u64) -> CancelRejection {
        CancelRejection {
            order_id: id(n),
            message: ApiMessage {
                code: Code(5122),
                text: "refused".into(),
            },
        }
    }

    #[test]
    #[expect(
        clippy::assert_is_empty,
        reason = "空かどうかを見る意図を is_empty で直接書く。assert_eq! に直すと空配列の型注釈が要り、読みにくくなる"
    )]
    fn a_mix_of_accepted_and_rejected_ids_is_a_value() {
        let outcome = check_cancel_outcome(
            &ids(&[1, 2, 3, 4]),
            ids(&[3, 4]),
            vec![rejection(1), rejection(2)],
        )
        .unwrap();
        assert_eq!(outcome.cancelled(), ids(&[3, 4]));
        assert_eq!(outcome.rejected().len(), 2);
        assert!(outcome.unaccounted().is_empty());
    }

    #[test]
    fn an_id_sent_but_never_answered_is_unaccounted_not_cancelled() {
        let outcome =
            check_cancel_outcome(&ids(&[1, 2, 3, 4]), ids(&[3]), vec![rejection(1)]).unwrap();
        assert_eq!(outcome.unaccounted(), ids(&[2, 4]));
        assert!(!outcome.cancelled().contains(&id(2)));
    }

    #[test]
    fn an_id_that_was_never_sent_is_refused_wherever_it_appears() {
        assert_eq!(
            check_cancel_outcome(&ids(&[1, 2]), ids(&[1, 9]), vec![]).unwrap_err(),
            Postcondition::CancelResultIdNotRequested(id(9))
        );
        assert_eq!(
            check_cancel_outcome(&ids(&[1, 2]), ids(&[1]), vec![rejection(7)]).unwrap_err(),
            Postcondition::CancelResultIdNotRequested(id(7))
        );
    }

    #[test]
    fn an_id_reported_twice_is_refused_within_and_across_lists() {
        let repeated = |r: core::result::Result<CancelOrdersOutcome, Postcondition>| r.unwrap_err();
        assert_eq!(
            repeated(check_cancel_outcome(&ids(&[1, 2]), ids(&[1, 1]), vec![])),
            Postcondition::CancelResultIdRepeated(id(1))
        );
        assert_eq!(
            repeated(check_cancel_outcome(
                &ids(&[1, 2]),
                ids(&[1]),
                vec![rejection(1)]
            )),
            Postcondition::CancelResultIdRepeated(id(1))
        );
        assert_eq!(
            repeated(check_cancel_outcome(
                &ids(&[1, 2]),
                vec![],
                vec![rejection(2), rejection(2)]
            )),
            Postcondition::CancelResultIdRepeated(id(2))
        );
    }

    #[test]
    fn an_empty_answer_leaves_every_sent_id_unaccounted() {
        let outcome = check_cancel_outcome(&ids(&[5, 6]), vec![], vec![]).unwrap();
        assert_eq!(outcome.unaccounted(), ids(&[5, 6]));
    }

    #[test]
    #[expect(
        clippy::assert_is_empty,
        reason = "空かどうかを見る意図を is_empty で直接書く。assert_eq! に直すと空配列の型注釈が要り、読みにくくなる"
    )]
    fn the_list_shape_accepts_a_list_and_the_empty_object() {
        let full: List<u32> = serde_json::from_str(r#"{"list":[1,2]}"#).unwrap();
        assert_eq!(full.list, [1, 2]);
        let empty: List<u32> = serde_json::from_str("{}").unwrap();
        assert!(empty.list.is_empty());
        let extra: List<u32> = serde_json::from_str(r#"{"list":[3],"extra":1}"#).unwrap();
        assert_eq!(extra.list, [3], "unknown keys next to a list are ignored");
        assert!(serde_json::from_str::<List<u32>>("[1]").is_err());
        assert!(
            serde_json::from_str::<List<u32>>(r#"{"items":[1]}"#).is_err(),
            "a renamed list is a shape error, not an empty list"
        );
        assert!(serde_json::from_str::<List<u32>>(r#"{"foo":1}"#).is_err());
        assert!(
            serde_json::from_str::<List<u32>>(r#"{"pagination":{"currentPage":1,"count":1}}"#)
                .is_err()
        );
    }
}
