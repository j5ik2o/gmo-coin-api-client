//! REST のリクエスト・応答型と、すべてのエンドポイントメソッドが使う
//! 唯一のリクエスト経路。
//!
//! エンドポイントメソッドは [`Client`] の固有メソッドで、文書化された操作
//! 1 つにつき 1 つ、ファイルごとに次のように分かれている。
//!
//! | ファイル     | レシーバ                  | 操作 |
//! |--------------|---------------------------|------|
//! | `public.rs`  | 任意の `A` の `Client<A>` | status, ticker, orderbooks, trades, klines, symbols |
//! | `account.rs` | `Client<Authenticated>`   | account/margin, account/assets, account/tradingVolume, account/fiatDeposit/history, account/fiatWithdrawal/history, account/deposit/history, account/withdrawal/history, account/exchangeFee/history, account/transfer |
//! | `trading.rs` | `Client<Authenticated>`   | orders, activeOrders, executions, latestExecutions, openPositions, positionSummary, order, changeOrder, cancelOrder, cancelOrders, cancelBulkOrder, closeOrder, closeBulkOrder, changeLosscutPrice |
//! | `ws_auth.rs` | クレート内部              | ws-auth の POST、PUT、DELETE (Private フィードが所有) |
//!
//! # エラー
//!
//! すべての REST メソッドは次の共通集合で失敗し得る。各メソッド自身の
//! `# エラー` 節は、そのメソッドに固有のものだけを加える。
//! - `Transport`: 完全な答えがない。
//! - `Protocol`: 文書化された形でない答え (フェイルクローズド)。
//! - `Api`: `status != 0`。たとえば ERR-5003 (`Error::is_rate_limited`)、
//!   ERR-5201/5202 (メンテナンス)、ERR-5008/5009 (ローカル時計のずれ)。
//!
//! 状態を変える呼び出し (GET 以外) では、`Transport` と `Protocol` のどちらも
//! 「取引所が適用したかもしれない」を意味し得る。再送や照会の前に
//! [`Error::outcome_unknown`] を見ること。`status == 0` は読めたが `data` が
//! 読めなかった、または事後条件に反したときは [`Error::accepted_but_unreadable`]
//! も真で、適用されたことは確定している。`Api` は取引所が拒否した答えなので
//! 適用されていない。
//!
//! 各メソッドの本体は同じ 4 段階である。型では表せない事前条件を検査し、
//! `Call` を組み立て、送り (エンベロープは `Client::send` の中で一度だけ検査
//! される)、`data` をデコードして事後条件を検査する。

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
use std::time::SystemTime;

use http_body_util::Full;
use hyper::body::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

use crate::client::{Access, Authenticated, Client};
use crate::domain::{InvalidValue, Primitive, Violation};
use crate::error::{ApiGroup, Error, ProtocolError, ProtocolErrorKind, Result};
use crate::secret::{Credentials, SignedPath};
use crate::transport::http::{Method, Route, round_trip};

/// 文書化された操作 1 つ。
#[derive(Clone, Copy)]
pub(crate) struct Endpoint {
    pub(crate) method: Method,
    pub(crate) path: SignedPath,
    /// `"POST /v1/order"`: エラーが持つ唯一の操作の識別子。
    pub(crate) operation: &'static str,
}

/// エンドポイントを一度だけ宣言する。署名対象のパスとエラーのラベルは
/// 同じリテラルから導かれるので、食い違うことがない。
macro_rules! endpoint {
    (GET $path:literal) => { $crate::rest::endpoint!(@ Get "GET" $path) };
    (POST $path:literal) => { $crate::rest::endpoint!(@ Post "POST" $path) };
    (PUT $path:literal) => { $crate::rest::endpoint!(@ Put "PUT" $path) };
    (DELETE $path:literal) => { $crate::rest::endpoint!(@ Delete "DELETE" $path) };
    (@ $m:ident $name:literal $path:literal) => {
        $crate::rest::Endpoint {
            method: $crate::transport::http::Method::$m,
            path: $crate::secret::SignedPath::new($path),
            operation: concat!($name, " ", $path),
        }
    };
}
pub(crate) use endpoint;

/// 送信準備のできたリクエスト。ボディはここで一度だけシリアライズされ、
/// 同じバッファが署名され、送られる。drop 時にゼロ埋めされる: `ws-auth` の
/// PUT と DELETE のボディはアクセストークンを含む。
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

    /// クエリの値はドメインプリミティブから来る。その字句規則はパーセント
    /// エンコードが必要なバイトをすべて除外している。
    pub(crate) fn query(mut self, key: &'static str, value: impl core::fmt::Display) -> Self {
        self.query.push((key, value.to_string()));
        self
    }

    /// # 事後条件
    /// `self.body` は、署名され送られる JSON バイト列を保持する。
    ///
    /// # パニック
    /// `body` のシリアライズに失敗したとき。ドメインプリミティブから組み立てた
    /// リクエスト型では起きない (文字列でないマップキーも浮動小数点もない)。
    /// パニックすれば何も送られない。空のボディで代用すると、誤ったリクエストに
    /// 署名して送ってしまう。
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

    /// `prefix + path + ?query`。
    fn target(&self, prefix: &str) -> String {
        let mut target = format!("{prefix}{}", self.endpoint.path.as_str());
        for (i, (key, value)) in self.query.iter().enumerate() {
            target.push(if i == 0 { '?' } else { '&' });
            target.push_str(key);
            target.push('=');
            target.push_str(value);
        }
        target
    }
}

/// 成功エンベロープ (`status == 0`)。`data` はまだデコードされていない。
///
/// `data` の生テキストはゼロ埋めされるバッファに保持する。`ws-auth` の
/// 答えではそれがアクセストークンそのものだからである。
pub(crate) struct Answer {
    api: ApiGroup,
    operation: &'static str,
    data: Option<Zeroizing<String>>,
}

impl core::fmt::Debug for Answer {
    /// `data` は出力しない。`ws-auth` ではそれがトークンである。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Answer")
            .field("api", &self.api)
            .field("operation", &self.operation)
            .field("has_data", &self.data.is_some())
            .finish()
    }
}

impl Answer {
    /// `data` を `T` としてデコードする。
    ///
    /// # エラー
    /// `data` がなければ `Protocol(MissingData)`、`T` と一致しなければ
    /// `Protocol(Shape)`。既定値を補うことはない。
    pub(crate) fn data<T: DeserializeOwned>(self) -> Result<T> {
        let Some(raw) = &self.data else {
            return Err(self.protocol(ProtocolErrorKind::MissingData));
        };
        serde_json::from_str(raw).map_err(|e| self.shape_error(&e))
    }

    /// `data` の生テキスト。借用デコード (`WireCancelOrders<'a>` など) や、
    /// serde を通さずに値を取り出す呼び出し元のため。
    pub(crate) fn raw(&self) -> Option<&str> {
        self.data.as_deref().map(String::as_str)
    }

    /// `data` を持たないと文書化された操作 (changeOrder、cancelOrder、
    /// changeLosscutPrice、ws-auth の PUT と DELETE) のため。取引所が将来
    /// `data` を加えても無視する。この呼び出しが必要とする状態は含まれない。
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

    /// serde のエラーを位置だけに落とす。メッセージは値を引用するので捨てる。
    pub(crate) fn shape_error(&self, e: &serde_json::Error) -> Error {
        self.protocol(ProtocolErrorKind::Shape {
            line: e.line(),
            column: e.column(),
        })
    }
}

impl<A: Access> Client<A> {
    /// Public REST 呼び出しを送る。
    pub(crate) async fn public(&self, call: Call) -> Result<Answer> {
        self.send(ApiGroup::PublicRest, None, call).await
    }

    /// 唯一のリクエスト経路: 宛先を組み立て、`credentials` があれば署名し、
    /// 往復し、エンベロープを検査する。
    ///
    /// # 事後条件
    /// `Ok` は `status == 0` のときだけ。`status != 0` は `Error::Api`、
    /// エンベロープでないものはすべて `Error::Protocol`。
    async fn send(
        &self,
        api: ApiGroup,
        credentials: Option<&Credentials>,
        call: Call,
    ) -> Result<Answer> {
        let inner = &self.inner;
        let (base, pool) = match api {
            ApiGroup::PrivateRest => (inner.endpoints.private_rest.base(), &inner.private_pool),
            _ => (inner.endpoints.public_rest.base(), &inner.public_pool),
        };
        let route = Route {
            api,
            operation: call.endpoint.operation,
            base,
            pool,
        };
        let target = call.target(&base.prefix);
        let host = base.host_header();
        let mut encode = || {
            let mut request = http::Request::builder()
                .method(call.endpoint.method.to_http())
                .uri(target.as_str())
                .header(http::header::HOST, host.as_str())
                .header(
                    http::header::USER_AGENT,
                    concat!("gmo-coin-api-client/", env!("CARGO_PKG_VERSION")),
                )
                .header(http::header::ACCEPT, "application/json");
            if call.endpoint.method != Method::Get {
                request = request.header(http::header::CONTENT_TYPE, "application/json");
            }
            if let Some(credentials) = credentials {
                let auth = credentials.sign_request(
                    now_ms(),
                    call.endpoint.method,
                    call.endpoint.path,
                    &call.body,
                );
                request = request
                    .header("API-KEY", auth.key.header_value())
                    .header("API-TIMESTAMP", auth.timestamp.as_str())
                    .header("API-SIGN", auth.sign.as_str());
            }
            request
                .body(Full::new(Bytes::copy_from_slice(&call.body)))
                .expect("the target and headers are built from validated primitives")
        };
        let response = round_trip(&inner.connector, &route, &inner.config, &mut encode)
            .await
            .map_err(Error::Transport)?;
        envelope::parse(
            api,
            call.endpoint.operation,
            response.status,
            &response.body,
        )
    }
}

impl Client<Authenticated> {
    /// 署名付きの Private REST 呼び出しを送る。
    pub(crate) async fn private(&self, call: Call) -> Result<Answer> {
        let credentials = self.access.0.clone();
        self.send(ApiGroup::PrivateRest, Some(&credentials), call)
            .await
    }
}

/// `API-TIMESTAMP`: 現在時刻のミリ秒 Unix 時刻。
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// ページ番号で区切られたリスト (trades、activeOrders、latestExecutions、
/// openPositions) の `page` と `count`。
///
/// # 不変条件
/// `page >= 1`、`1 <= count <= 100`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRequest {
    page: NonZeroU32,
    count: u8,
}

impl PageRequest {
    pub const MAX_COUNT: u8 = 100;

    /// 1 ページ目、100 件: 取引所自身の既定。
    pub const FIRST: PageRequest = PageRequest {
        page: NonZeroU32::MIN,
        count: Self::MAX_COUNT,
    };

    /// ページ番号とページサイズ。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `page() == page` かつ `count() == count`。
    ///
    /// # エラー
    /// `page == 0`、または `count` が `1..=100` の外のとき `Semantics`。
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

/// 取引所のリスト型オブジェクトの生の形。`list` と `pagination` を取り出し、
/// それ以外のキーは数えるだけで捨てる。
///
/// 空の結果の形は `{}` だけなので、[`Page`] と `List` (`trading.rs`) は
/// `other_keys` を見て「空」と「形の誤り」を分ける。`{"items": [...]}` の
/// ように `list` を欠いて他のキーを持つオブジェクトを、空の結果として
/// 読んでしまわないためである。`list` がある場合、他のキーは無視する。
pub(crate) struct WireListing<T> {
    pub(crate) list: Option<Vec<T>>,
    pub(crate) pagination: Option<Pagination>,
    pub(crate) other_keys: usize,
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for WireListing<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        use serde::de::{Error, IgnoredAny, MapAccess, Visitor};

        #[derive(serde::Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Key {
            List,
            Pagination,
            #[serde(other)]
            Other,
        }

        struct ListingVisitor<T>(core::marker::PhantomData<T>);

        impl<'de, T: serde::Deserialize<'de>> Visitor<'de> for ListingVisitor<T> {
            type Value = WireListing<T>;

            fn expecting(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str("an object with optional list and pagination keys")
            }

            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> core::result::Result<Self::Value, M::Error> {
                let mut listing = WireListing {
                    list: None,
                    pagination: None,
                    other_keys: 0,
                };
                while let Some(key) = map.next_key::<Key>()? {
                    match key {
                        Key::List => {
                            if listing.list.is_some() {
                                return Err(M::Error::duplicate_field("list"));
                            }
                            listing.list = Some(map.next_value()?);
                        }
                        Key::Pagination => {
                            if listing.pagination.is_some() {
                                return Err(M::Error::duplicate_field("pagination"));
                            }
                            listing.pagination = Some(map.next_value()?);
                        }
                        Key::Other => {
                            map.next_value::<IgnoredAny>()?;
                            listing.other_keys += 1;
                        }
                    }
                }
                Ok(listing)
            }
        }

        d.deserialize_map(ListingVisitor(core::marker::PhantomData))
    }
}

/// ページ番号で区切られたリストの 1 ページ。
///
/// 取引所は 2 つの形を送り、どちらも明示的に受け付ける:
/// `{"pagination": {...}, "list": [...]}` と、空の結果に対する `{}`。
/// 後者は公式ドキュメントにない。本番 API が空の `activeOrders` にそう答えた
/// 記憶に基づくもので、本番で確かめる必要がある (未解決)。それ以外の形は
/// `Protocol` エラーである。
///
/// # 不変条件
/// `pagination.is_none()` ならば `items.is_empty()`。
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

    /// 取引所が `{}` と答えたときに限り `None`。
    pub fn pagination(&self) -> Option<Pagination> {
        self.pagination
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    pub current_page: u32,
    pub count: u32,
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Page<T> {
    /// `{}` は空のページ。`pagination` と `list` がそろっていればそのページで、
    /// 他のキーは無視する。片方だけ、または `{}` 以外で両方欠けていれば形の誤り。
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        let wire = WireListing::<T>::deserialize(d)?;
        match (wire.pagination, wire.list) {
            (None, None) if wire.other_keys == 0 => Ok(Page {
                items: Vec::new(),
                pagination: None,
            }),
            (Some(pagination), Some(items)) => Ok(Page {
                items,
                pagination: Some(pagination),
            }),
            _ => Err(serde::de::Error::custom(
                "expected pagination and list together, or an empty object",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_request_bounds() {
        assert!(PageRequest::new(0, 10).is_err());
        assert!(PageRequest::new(1, 0).is_err());
        assert!(PageRequest::new(1, 101).is_err());
        let p = PageRequest::new(2, 50).unwrap();
        assert_eq!((p.page(), p.count()), (2, 50));
        assert_eq!(p.next().unwrap().page(), 3);
        assert_eq!(
            (PageRequest::FIRST.page(), PageRequest::FIRST.count()),
            (1, 100)
        );
    }

    #[test]
    fn page_accepts_exactly_two_shapes() {
        let full: Page<u32> =
            serde_json::from_str(r#"{"pagination":{"currentPage":2,"count":3},"list":[1,2,3]}"#)
                .unwrap();
        assert_eq!(full.items(), [1, 2, 3]);
        assert_eq!(
            full.pagination(),
            Some(Pagination {
                current_page: 2,
                count: 3
            })
        );
        let empty: Page<u32> = serde_json::from_str("{}").unwrap();
        assert!(empty.items().is_empty() && empty.pagination().is_none());
        let extra: Page<u32> = serde_json::from_str(
            r#"{"pagination":{"currentPage":1,"count":1},"list":[7],"extra":true}"#,
        )
        .unwrap();
        assert_eq!(
            extra.items(),
            [7],
            "unknown keys next to a list are ignored"
        );
        assert!(serde_json::from_str::<Page<u32>>(r#"{"list":[1]}"#).is_err());
        assert!(
            serde_json::from_str::<Page<u32>>(r#"{"pagination":{"currentPage":1,"count":0}}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<Page<u32>>("[]").is_err());
        assert!(
            serde_json::from_str::<Page<u32>>(r#"{"items":[1]}"#).is_err(),
            "a renamed list is a shape error, not an empty page"
        );
        assert!(serde_json::from_str::<Page<u32>>(r#"{"foo":1}"#).is_err());
    }

    #[test]
    fn call_target_joins_prefix_path_and_query() {
        let call = Call::new(endpoint!(GET "/v1/trades"))
            .query("symbol", "BTC")
            .query("page", 2);
        assert_eq!(
            call.target("/public"),
            "/public/v1/trades?symbol=BTC&page=2"
        );
        assert_eq!(
            Call::new(endpoint!(GET "/v1/status")).target("/public"),
            "/public/v1/status"
        );
        assert_eq!(endpoint!(POST "/v1/order").operation, "POST /v1/order");
    }

    /// `Error::outcome_unknown` は操作名の先頭の語をメソッドとして読む。
    #[test]
    fn operation_starts_with_its_method() {
        for endpoint in [
            endpoint!(GET "/v1/x"),
            endpoint!(POST "/v1/x"),
            endpoint!(PUT "/v1/x"),
            endpoint!(DELETE "/v1/x"),
        ] {
            assert_eq!(
                endpoint.operation,
                format!("{} /v1/x", endpoint.method.as_str())
            );
        }
    }
}
