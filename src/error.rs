//! このクレート唯一のエラー型。
//!
//! すべての variant は「誰の責任か」と「何かが送られたか」に答える。
//!
//! | variant      | 責任                       | リクエストは送られたか     | 適用されたか                         |
//! |--------------|----------------------------|----------------------------|--------------------------------------|
//! | `Contract`   | 呼び出し側の入力           | 決して送られない           | いいえ                               |
//! | `Api`        | 取引所が拒否した           | 送られ、処理された         | いいえ                               |
//! | `Protocol`   | 取引所が形を破った         | 送られた                   | 不明 ([`Error::outcome_unknown`])    |
//! | `Transport`  | ネットワーク、TLS、上限    | [`Delivery`] を参照        | `MaybeSent` なら不明                 |
//! | `FeedClosed` | フィードは既に終了         | 該当しない                 | 該当しない                           |
//!
//! 「適用されたか」は GET 以外の REST 呼び出しについての問いである。答えは
//! [`Error::outcome_unknown`] と [`Error::accepted_but_unreadable`] が返す。
//!
//! 秘匿は構造によって保証される。エラーが持つのは `&'static str` の操作名
//! (`"POST /v1/order"`)、`ApiGroup`、数値コード、`io::ErrorKind` だけである。
//! URL、ヘッダ、リクエストボディ、serde のメッセージは決して持たない
//! (serde のメッセージは問題の JSON 値を引用し、`ws-auth` の応答値は
//! アクセストークンである)。`'static` な文字列は実行時のトークンを含み得ない。

use core::fmt;
use std::io;

use crate::domain::{ExecutionId, InvalidValue, OrderId, Timestamp};

/// このクレートの公開 API が返す `Result`。エラー型の既定値は [`Error`] である。
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// このクレートのエラーをまとめる型。どの variant かで責任の所在が分かる (モジュールの表を参照)。
/// [`InvalidValue`] や [`ContractViolation`] を直接返す API もあり、どちらも `From` でこの型に変換できる。
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// ドキュメント化された事前条件が満たされなかった。何も送られていない。
    Contract(ContractViolation),
    /// 取引所が `status != 0` で答えた。
    Api(ApiError),
    /// 取引所は答えたが、ドキュメントの形ではなかった。または答えに対する
    /// 事後条件が満たされなかった。フェイルクローズド: 欠けた部分や未知の
    /// 部分に既定値を補うことはない。
    Protocol(ProtocolError),
    /// 取引所はまったく答えていないかもしれない。
    Transport(TransportError),
    /// フィード終了後 (`close`、または先行する終端エラー) の `Feed::next`。
    FeedClosed,
}

impl Error {
    /// 取引所が呼び出し回数の上限超過として拒否した (ERR-5003)。このクレートは
    /// REST 呼び出しを決して絞らない。これは呼び出し側への合図である。
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Error::Api(e) if e.has(ErrorCode::RATE_LIMIT))
    }

    /// 状態を変えるリクエストが効いたかどうか分からない。注文なら同じ
    /// `ClientOrderId` で再送する。ERR-915 が返れば最初の試行は発注されて
    /// おり、`orders(OrderLookup::ByClientOrderIds)` がそれを返す。
    ///
    /// 真になるのは次の 2 つである。
    /// - `Transport` で [`Delivery::MaybeSent`]: 書き込みが始まった後に失敗した
    ///   (GET でも真になる。これは送られた事実の記録である)。
    /// - GET 以外の REST 呼び出しの後の `Protocol`: 答えは届いたが読めなかった。
    ///   ゲートウェイの 502、エンベロープでない本文、`status == 0` なのに
    ///   `data` が読めない、事後条件違反。どれも取引所が適用した可能性がある。
    ///   `status == 0` を読めた場合は [`Error::accepted_but_unreadable`] も真で、
    ///   適用されたことは確定している。
    ///
    /// `Api` (取引所が拒否した) と `Contract` (送っていない) では偽。
    pub fn outcome_unknown(&self) -> bool {
        match self {
            Error::Transport(t) => t.delivery == Delivery::MaybeSent,
            Error::Protocol(p) => p.after_state_change(),
            Error::Contract(_) | Error::Api(_) | Error::FeedClosed => false,
        }
    }

    /// GET 以外の REST 呼び出しで、取引所は `status == 0` で受け付けたが、
    /// 答え (`data`) を読めなかった。リクエストは適用されている。注文なら
    /// 発注済みで、`OrderId` だけが失われたので、
    /// `orders(OrderLookup::ByClientOrderIds)` で引く。再送は不要で、しても
    /// ERR-915 が返るだけである。`outcome_unknown` も真になる。
    ///
    /// GET の後の同じ種類のエラー (たとえば `ticker()` の `WrongSymbol`) では
    /// 偽。読み取りは何も適用しないので、問い自体が成り立たない。
    pub fn accepted_but_unreadable(&self) -> bool {
        matches!(
            self,
            Error::Protocol(p) if p.after_state_change() && p.kind.after_acceptance()
        )
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Contract(v) => write!(f, "事前条件違反のため送信しませんでした: {v}"),
            Error::Api(e) => write!(f, "{e}"),
            Error::Protocol(e) => write!(f, "{e}"),
            Error::Transport(e) => write!(f, "{e}"),
            Error::FeedClosed => f.write_str("フィードは既に終了しています"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ContractViolation> for Error {
    fn from(v: ContractViolation) -> Self {
        Error::Contract(v)
    }
}

/// 引数の型では表せない事前条件。何かを送る前に、公開 API の境界で検査される。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContractViolation {
    /// ドメインプリミティブが値を拒否した (たとえば 11 件の id を持つ `Batch`)。
    InvalidValue(InvalidValue),
    /// `/order` の `cancelBefore` には現物銘柄と `Side::Sell` が必要。
    CancelBeforeRequiresSpotSell,
    /// `losscutPrice` はレバレッジ銘柄でのみ受け付けられる。
    LosscutRequiresLeverageSymbol,
    /// `closeOrder`、`closeBulkOrder`、`openPositions` はレバレッジ銘柄のみ。
    RequiresLeverageSymbol,
    /// フィードが既に `Limits::max_subscriptions` 件のチャネルを持っている。
    TooManySubscriptions {
        /// フィードが上限として許す購読数 (`Limits::max_subscriptions`)。
        max: u8,
    },
}

impl fmt::Display for ContractViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContractViolation::InvalidValue(v) => write!(f, "{v}"),
            ContractViolation::CancelBeforeRequiresSpotSell => {
                f.write_str("cancelBefore は現物銘柄の売り注文でのみ指定できます")
            }
            ContractViolation::LosscutRequiresLeverageSymbol => {
                f.write_str("losscutPrice はレバレッジ銘柄でのみ指定できます")
            }
            ContractViolation::RequiresLeverageSymbol => {
                f.write_str("この操作はレバレッジ銘柄でのみ使えます")
            }
            ContractViolation::TooManySubscriptions { max } => {
                write!(f, "購読数が上限 ({max} 件) に達しています")
            }
        }
    }
}

impl std::error::Error for ContractViolation {}

impl From<InvalidValue> for ContractViolation {
    fn from(v: InvalidValue) -> Self {
        ContractViolation::InvalidValue(v)
    }
}

impl From<InvalidValue> for Error {
    fn from(v: InvalidValue) -> Self {
        Error::Contract(ContractViolation::InvalidValue(v))
    }
}

/// `status != 0`。
///
/// # 不変条件
/// `messages` は空でない。メッセージのないエラーエンベロープは `Protocol`
/// エラーであり、中身のない `ApiError` にはならない。
#[derive(Clone, Debug)]
pub struct ApiError {
    pub(crate) operation: &'static str,
    pub(crate) status: i64,
    pub(crate) messages: Vec<ApiMessage>,
    pub(crate) response_time: Option<Timestamp>,
}

impl ApiError {
    /// エラーが起きた操作の名前。`"POST /v1/order"` のようにメソッドとパスから成る。
    pub fn operation(&self) -> &'static str {
        self.operation
    }

    /// エンベロープのゼロでない `status`。
    pub fn status(&self) -> i64 {
        self.status
    }

    /// 取引所が返したメッセージ。空でない。
    pub fn messages(&self) -> &[ApiMessage] {
        &self.messages
    }

    /// `code` を持つメッセージが 1 件でも含まれているか。
    pub fn has(&self, code: ErrorCode) -> bool {
        self.messages.iter().any(|m| m.code == code)
    }

    /// 一部のエラーでは欠けている (`error_without_responsetime.json`)。
    pub fn response_time(&self) -> Option<Timestamp> {
        self.response_time
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} に対して取引所がエラーを返しました (status {}):",
            self.operation, self.status
        )?;
        for m in &self.messages {
            write!(f, " ERR-{} {}", m.code.0, m.text)?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

/// `{message_code, message_string}` の 1 組。エラーエンベロープ、または
/// `cancelOrders` の失敗エントリから来る。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiMessage {
    /// 取引所が返したエラーコード。
    pub code: ErrorCode,
    /// サーバが書いた文章。最大 512 バイト (それより長ければ `Protocol` エラー)。
    pub text: Box<str>,
}

/// `ERR-<n>` を数値にしたもの。構造上開いている: 未知のコードも単なる値で
/// あり、文書化されたコードは `match` で使える定数である。5 桁 (99999 まで)
/// を収めるので `u32`。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ErrorCode(pub u32);

impl ErrorCode {
    /// ERR-70。決済注文で、指定した建玉の売買区分が誤っている。
    pub const WRONG_POSITION_SIDE: ErrorCode = ErrorCode(70);
    /// ERR-189。決済注文の数量が、決済可能な保有数量を超えている。
    pub const CLOSE_SIZE_OVER_POSITION: ErrorCode = ErrorCode(189);
    /// ERR-200。すでに有効注文があり、注文数量が発注可能な数量を超えている。決済注文をしたい場合は、数量を変えるか、有効注文を取り消してから再度注文する。
    pub const ACTIVE_ORDERS_EXCEED_SIZE: ErrorCode = ErrorCode(200);
    /// ERR-201。注文時に取引余力が不足している。
    pub const INSUFFICIENT_BALANCE: ErrorCode = ErrorCode(201);
    /// ERR-208。注文可能な保有数量が不足している。保有数量と有効注文を確認する。
    pub const INSUFFICIENT_HOLDINGS: ErrorCode = ErrorCode(208);
    /// ERR-254。指定した建玉が存在しない。
    pub const POSITION_NOT_FOUND: ErrorCode = ErrorCode(254);
    /// ERR-422。一括決済注文で、建玉が存在しない。銘柄と売買区分を確認する。
    pub const BULK_CLOSE_NO_POSITION: ErrorCode = ErrorCode(422);
    /// ERR-423。一括決済注文の数量が、決済可能な建玉の合計数量を超えている。
    pub const BULK_CLOSE_SIZE_OVER_POSITIONS: ErrorCode = ErrorCode(423);
    /// ERR-430。約定情報の取得で、`orderId` と `executionId` の指定が不正である。
    pub const INVALID_EXECUTION_LOOKUP: ErrorCode = ErrorCode(430);
    /// ERR-554。サーバーが利用できない状態である。
    pub const SERVER_UNAVAILABLE: ErrorCode = ErrorCode(554);
    /// ERR-626。サーバーへのアクセスが集中している。しばらく待ってから再試行する。
    pub const SERVER_BUSY: ErrorCode = ErrorCode(626);
    /// ERR-635。有効注文の件数が上限に達している。有効注文を取り消してから再度注文する。
    pub const ACTIVE_ORDER_LIMIT: ErrorCode = ErrorCode(635);
    /// ERR-682。追加証拠金が発生しているため、指定した操作を実行できない。
    pub const MARGIN_CALL_IN_EFFECT: ErrorCode = ErrorCode(682);
    /// ERR-683。追加証拠金の処理が実行中のため、指定した操作を実行できない。
    pub const MARGIN_CALL_PROCESSING: ErrorCode = ErrorCode(683);
    /// ERR-754。振替可能額より大きい値を振り替えようとした。
    pub const TRANSFER_OVER_TRANSFERABLE: ErrorCode = ErrorCode(754);
    /// ERR-846。`fromTimestamp` と `toTimestamp` の差が最大 30 分を超えている。
    pub const HISTORY_RANGE_TOO_WIDE: ErrorCode = ErrorCode(846);
    /// ERR-913。代用数量の全量を売却するか、売却後の代用数量が最小代用数量以上になるように売却する必要がある。
    pub const COLLATERAL_MINIMUM: ErrorCode = ErrorCode(913);
    /// ERR-915。注文、決済注文、一括決済注文で、指定した `clientOrderId` がすでに使われている。
    pub const CLIENT_ORDER_ID_USED: ErrorCode = ErrorCode(915);
    /// ERR-5003。API の呼び出し上限を超えた。
    pub const RATE_LIMIT: ErrorCode = ErrorCode(5003);
    /// ERR-5007。`API-TIMESTAMP` ヘッダがない、または値が数値でない。
    pub const TIMESTAMP_MISSING: ErrorCode = ErrorCode(5007);
    /// ERR-5008。`API-TIMESTAMP` が公開 API のシステム時刻より遅い。
    pub const TIMESTAMP_TOO_LATE: ErrorCode = ErrorCode(5008);
    /// ERR-5009。`API-TIMESTAMP` が公開 API のシステム時刻より早い。
    pub const TIMESTAMP_TOO_EARLY: ErrorCode = ErrorCode(5009);
    /// ERR-5010。`API-SIGN` (署名) が不正である。
    pub const BAD_SIGNATURE: ErrorCode = ErrorCode(5010);
    /// ERR-5011。`API-KEY` ヘッダが設定されていない。
    pub const API_KEY_MISSING: ErrorCode = ErrorCode(5011);
    /// ERR-5012。`API-KEY` が認証エラーになった場合などに返る。
    pub const API_KEY_REJECTED: ErrorCode = ErrorCode(5012);
    /// ERR-5014。同意が必要な処理で、未確認または未同意の事項がある。
    pub const CONSENT_REQUIRED: ErrorCode = ErrorCode(5014);
    /// ERR-5016。履歴取得処理がタイムアウトした。再実行するか、期間を狭める。
    pub const HISTORY_TIMEOUT: ErrorCode = ErrorCode(5016);
    /// ERR-5106。パラメーターが不正である。
    pub const INVALID_PARAMETER: ErrorCode = ErrorCode(5106);
    /// ERR-5111。リクエストボディの `timeInForce` が誤っている。
    pub const INVALID_TIME_IN_FORCE: ErrorCode = ErrorCode(5111);
    /// ERR-5114。小数点以下の桁数が有効桁数を超えている。
    pub const TOO_MANY_DECIMALS: ErrorCode = ErrorCode(5114);
    /// ERR-5118。リクエストボディに `losscutPrice` を指定できない。
    pub const LOSSCUT_NOT_ALLOWED: ErrorCode = ErrorCode(5118);
    /// ERR-5121。注文価格が低すぎて注文できない。
    pub const PRICE_TOO_LOW: ErrorCode = ErrorCode(5121);
    /// ERR-5122。注文がすでに変更中、取消中、取消済、全量約定、失効のいずれかで、変更も取消もできない。
    pub const ORDER_NOT_MODIFIABLE: ErrorCode = ErrorCode(5122);
    /// ERR-5123。指定した注文番号が存在しない。
    pub const ORDER_NOT_FOUND: ErrorCode = ErrorCode(5123);
    /// ERR-5125。API の取引規制がかかっている。
    pub const API_TRADING_RESTRICTED: ErrorCode = ErrorCode(5125);
    /// ERR-5126。1 回あたりの最大注文数量を超えている、または最小注文数量を下回っている。
    pub const SIZE_OUT_OF_RANGE: ErrorCode = ErrorCode(5126);
    /// ERR-5127。お客様に取引規制がかかっている。
    pub const ACCOUNT_TRADING_RESTRICTED: ErrorCode = ErrorCode(5127);
    /// ERR-5129。逆指値注文で、即座に約定する価格が指定された。
    pub const STOP_PRICE_WOULD_EXECUTE: ErrorCode = ErrorCode(5129);
    /// ERR-5130。注文情報取得、約定情報取得、注文の複数キャンセルで、指定した `orderId` が上限の件数を超えている。
    pub const TOO_MANY_ORDER_IDS: ErrorCode = ErrorCode(5130);
    /// ERR-5132。約定情報取得で、指定した `executionId` が上限の件数を超えている。
    pub const TOO_MANY_EXECUTION_IDS: ErrorCode = ErrorCode(5132);
    /// ERR-5133。振替規制中のため、口座振替ができない。
    pub const TRANSFER_RESTRICTED: ErrorCode = ErrorCode(5133);
    /// ERR-5135。外国為替 FX 用の口座が開設されていない状態で口座振替をした。
    pub const FX_ACCOUNT_MISSING: ErrorCode = ErrorCode(5135);
    /// ERR-5145。注文情報取得、約定情報取得で、指定した `clientOrderId` が上限の件数を超えている。
    pub const TOO_MANY_CLIENT_ORDER_IDS: ErrorCode = ErrorCode(5145);
    /// ERR-5201。定期メンテナンス中に Public/Private API を呼び出した。
    pub const SCHEDULED_MAINTENANCE: ErrorCode = ErrorCode(5201);
    /// ERR-5202。緊急メンテナンス中に Public/Private API を呼び出した。
    pub const EMERGENCY_MAINTENANCE: ErrorCode = ErrorCode(5202);
    /// ERR-5203。取引所のプレオープン中に注文または注文変更をした。
    pub const PRE_OPEN: ErrorCode = ErrorCode(5203);
    /// ERR-5204。リクエスト先の API の URL が不正である。
    pub const INVALID_URL: ErrorCode = ErrorCode(5204);
    /// ERR-5206。注文ごとの変更可能回数が上限に達している。取消してから新規に注文する。
    pub const CHANGE_LIMIT_REACHED: ErrorCode = ErrorCode(5206);
    /// ERR-5207。リクエストボディの `symbol`、`interval`、`date` のいずれかが不正である。
    pub const INVALID_SYMBOL_INTERVAL_DATE: ErrorCode = ErrorCode(5207);
    /// ERR-5208。約定情報取得で、`orderId` と `executionId` が両方指定された。
    pub const BOTH_ORDER_AND_EXECUTION_ID: ErrorCode = ErrorCode(5208);
    /// ERR-5212。注文情報取得、約定情報取得で、`orderId` と `clientOrderId` が同時に指定された。
    pub const BOTH_ORDER_AND_CLIENT_ORDER_ID: ErrorCode = ErrorCode(5212);

    /// `ERR-<1 から 5 桁の数字>` を解析する。それ以外は `Protocol` エラーになる。
    pub(crate) fn parse(text: &str) -> Option<ErrorCode> {
        let digits = text.strip_prefix("ERR-")?;
        if digits.is_empty() || digits.len() > 5 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok().map(ErrorCode)
    }
}

/// 操作が属する 4 つの API のどれか。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiGroup {
    /// 公開 REST API。
    PublicRest,
    /// 認証が必要な REST API。
    PrivateRest,
    /// 公開 WebSocket API。
    PublicWs,
    /// 認証が必要な WebSocket API。
    PrivateWs,
}

impl fmt::Display for ApiGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ApiGroup::PublicRest => "Public REST",
            ApiGroup::PrivateRest => "Private REST",
            ApiGroup::PublicWs => "Public WebSocket",
            ApiGroup::PrivateWs => "Private WebSocket",
        })
    }
}

/// 取引所の答えが文書化された形でなかった、または答えに対する事後条件が満たされなかった。
#[derive(Debug)]
pub struct ProtocolError {
    pub(crate) api: ApiGroup,
    pub(crate) operation: &'static str,
    pub(crate) kind: ProtocolErrorKind,
}

impl ProtocolError {
    /// 操作が属する API。
    pub fn api(&self) -> ApiGroup {
        self.api
    }
    /// エラーが起きた操作の名前。[`api`](Self::api) が REST なら `"POST /v1/order"` のように
    /// メソッドとパスから成り、WebSocket ならハンドシェイクのパス `"/v1"` である。
    pub fn operation(&self) -> &'static str {
        self.operation
    }
    /// 何が規約に反したかの種類。
    pub fn kind(&self) -> &ProtocolErrorKind {
        &self.kind
    }

    /// GET 以外の REST 呼び出しの後に起きたか。`operation` は `endpoint!` が
    /// メソッド名と空白とパスから組み立てるので、先頭の語がメソッドである
    /// (`rest` のテスト `operation_starts_with_its_method` が固定している)。
    /// WebSocket の操作名はこの形でないので `api` で除く。
    fn after_state_change(&self) -> bool {
        matches!(self.api, ApiGroup::PublicRest | ApiGroup::PrivateRest)
            && !self.operation.starts_with("GET ")
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} の {} で応答の形が規約と違います: ",
            self.api, self.operation
        )?;
        match &self.kind {
            ProtocolErrorKind::HttpStatus(status) => {
                write!(f, "HTTP {status} でエンベロープがありません")
            }
            ProtocolErrorKind::NotAnEnvelope => f.write_str("本文がエンベロープではありません"),
            ProtocolErrorKind::MalformedErrorEnvelope => {
                f.write_str("status != 0 なのに messages が欠けているか不正です")
            }
            ProtocolErrorKind::MissingData => f.write_str("data がありません"),
            ProtocolErrorKind::Shape { line, column } => {
                write!(
                    f,
                    "data が文書化された形と一致しません ({line} 行 {column} 列)"
                )
            }
            ProtocolErrorKind::Postcondition(p) => write!(f, "事後条件違反 ({p})"),
            ProtocolErrorKind::WsHandshake { http_status } => {
                write!(
                    f,
                    "WebSocket ハンドシェイクが HTTP {http_status} で拒否されました"
                )
            }
            ProtocolErrorKind::WsMessage { line, column } => {
                write!(
                    f,
                    "WebSocket メッセージをデコードできません ({line} 行 {column} 列)"
                )
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

/// [`ProtocolError`] の原因。`Shape` と `WsMessage` は位置 (行と列) だけを持ち、問題の JSON 値は持たない。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProtocolErrorKind {
    /// HTTP 状態が 2xx の外で、本文がエンベロープでない (404、503)、または
    /// 本文が `status == 0` のエンベロープだった (2xx の外の成功は受付でない)。
    HttpStatus(u16),
    /// 本文が整数 `status` を持つ JSON オブジェクトでない。
    NotAnEnvelope,
    /// `status != 0` だが `messages` が欠けている、空、または不正。
    MalformedErrorEnvelope,
    /// `status == 0` だがエンドポイントの `data` が欠けている。
    MissingData,
    /// `data` が文書化された形と一致しない。位置だけ: serde のメッセージは
    /// 値を引用するので捨てる。
    Shape {
        /// `data` の中で不一致が見つかった行 (1 始まり)。
        line: usize,
        /// `data` の中で不一致が見つかった列 (1 始まり)。
        column: usize,
    },
    /// 解析済みの答えに対する事後条件が満たされなかった。
    Postcondition(Postcondition),
    /// WebSocket ハンドシェイクが `101` 以外で答えた。
    WsHandshake {
        /// ハンドシェイクの応答が返した HTTP 状態コード。
        http_status: u16,
    },
    /// 購読中のチャネルの WebSocket テキストメッセージがデコードできなかった。
    WsMessage {
        /// デコードに失敗した位置の行 (1 始まり)。
        line: usize,
        /// デコードに失敗した位置の列 (1 始まり)。
        column: usize,
    },
}

impl ProtocolErrorKind {
    /// `status == 0` のエンベロープを受け取った後にしか起きない種類か。
    /// これらは `Answer` (成功エンベロープ) からしか作られない。逆に
    /// `HttpStatus`、`NotAnEnvelope`、`MalformedErrorEnvelope` はエンベロープの
    /// 検査 (`envelope::parse`) からしか作られない。
    fn after_acceptance(&self) -> bool {
        matches!(
            self,
            ProtocolErrorKind::MissingData
                | ProtocolErrorKind::Shape { .. }
                | ProtocolErrorKind::Postcondition(_)
        )
    }
}

/// 名前付きの事後条件。テストと呼び出し側が正確な規則で照合できる。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Postcondition {
    /// `cancelOrders` が、呼び出し側が送っていない id を報告した。
    CancelResultIdNotRequested(OrderId),
    /// `cancelOrders` が 1 つの id を取消済みと失敗の両方、または二度報告した。
    CancelResultIdRepeated(OrderId),
    /// `cancelOrders` の `failed` エントリの理由が、`ERR-<n>` のコードと
    /// 512 バイト以下の文章という形でなかった。
    CancelRejectionMalformed(OrderId),
    /// `ticker(symbol)` / `order_book(symbol)` が別の銘柄で答えた、または
    /// エントリがちょうど 1 件でなかった。
    WrongSymbol,
    /// `orders(..)` が、検索で指定していない注文を返した。
    OrderNotRequested(OrderId),
    /// `executions(..)` が、検索で指定していない約定を返した。
    ExecutionNotRequested(ExecutionId),
    /// `cancel_bulk_orders` が、1 回の上限 (10 件) を超える id を返した。
    BulkCancelTooManyIds {
        /// 取引所が返した id の件数。
        actual: usize,
    },
    /// `cancel_bulk_orders` が同じ id を二度返した。
    BulkCancelIdRepeated(OrderId),
    /// 1 件のはずの `data` 配列 (`account/transfer`) が別の長さだった。
    ExpectedOneEntry {
        /// 実際に返ってきたエントリの件数。
        actual: usize,
    },
}

impl fmt::Display for Postcondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Postcondition::CancelResultIdNotRequested(id) => {
                write!(f, "送っていない注文 id {id} が結果に含まれています")
            }
            Postcondition::CancelResultIdRepeated(id) => {
                write!(f, "注文 id {id} が結果に二度現れました")
            }
            Postcondition::CancelRejectionMalformed(id) => {
                write!(f, "注文 id {id} の拒否理由が規約の形ではありません")
            }
            Postcondition::WrongSymbol => f.write_str("要求した銘柄と応答の銘柄が一致しません"),
            Postcondition::OrderNotRequested(id) => {
                write!(f, "要求していない注文 {id} が返されました")
            }
            Postcondition::ExecutionNotRequested(id) => {
                write!(f, "要求していない約定 {id} が返されました")
            }
            Postcondition::BulkCancelTooManyIds { actual } => {
                write!(f, "一括取消の結果が上限 10 件を超えて {actual} 件でした")
            }
            Postcondition::BulkCancelIdRepeated(id) => {
                write!(f, "一括取消の結果に注文 id {id} が二度現れました")
            }
            Postcondition::ExpectedOneEntry { actual } => {
                write!(f, "エントリは 1 件のはずが {actual} 件でした")
            }
        }
    }
}

/// 通信の失敗。原因の種類は [`TransportErrorKind`] が表す。
#[derive(Debug)]
pub struct TransportError {
    pub(crate) api: ApiGroup,
    pub(crate) operation: &'static str,
    pub(crate) kind: TransportErrorKind,
    pub(crate) delivery: Delivery,
}

impl TransportError {
    /// 失敗した API。
    pub fn api(&self) -> ApiGroup {
        self.api
    }
    /// 失敗した操作の名前。[`api`](Self::api) が REST なら `"POST /v1/order"` のように
    /// メソッドとパスから成り、WebSocket ならハンドシェイクのパス `"/v1"` である。
    pub fn operation(&self) -> &'static str {
        self.operation
    }
    /// 失敗の種類。
    pub fn kind(&self) -> &TransportErrorKind {
        &self.kind
    }
    /// リクエストが取引所に届いた可能性があるか。[`Error::outcome_unknown`] の判断の元になる。
    pub fn delivery(&self) -> Delivery {
        self.delivery
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} の {} で通信に失敗しました: ",
            self.api, self.operation
        )?;
        match &self.kind {
            TransportErrorKind::Io(kind) => write!(f, "入出力エラー ({kind:?})")?,
            TransportErrorKind::Tls => f.write_str("TLS ハンドシェイクに失敗しました")?,
            TransportErrorKind::Timeout(phase) => write!(f, "タイムアウト ({phase})")?,
            TransportErrorKind::TooLarge { limit } => {
                write!(f, "応答が上限 {limit} バイトを超えました")?;
            }
            TransportErrorKind::Framing(what) => {
                write!(f, "サーバのフレーミングが不正です ({what})")?;
            }
            TransportErrorKind::ClosedEarly => {
                f.write_str("応答が完了する前に接続が閉じられました")?;
            }
        }
        match self.delivery {
            Delivery::NotSent => f.write_str("。リクエストは送信されていません"),
            Delivery::MaybeSent => f.write_str("。リクエストが届いたかどうかは不明です"),
        }
    }
}

impl std::error::Error for TransportError {}

/// リクエストが取引所に届いた可能性があるか。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// リクエストのバイトが 1 つも書き込まれる前に失敗した。
    NotSent,
    /// リクエストの書き込みが始まった。結果は不明である。
    MaybeSent,
}

/// [`TransportError`] の原因。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportErrorKind {
    /// 種類だけ。`io::Error` のペイロードは任意の文章を持ち得る。
    Io(io::ErrorKind),
    /// TLS ハンドシェイクに失敗した。
    Tls,
    /// どの段階でタイムアウトしたか。
    Timeout(Phase),
    /// `Limits` のサイズ上限を、データをバッファする前に超えた。
    TooLarge {
        /// 超えたサイズ上限 (バイト)。
        limit: u32,
    },
    /// サーバからの HTTP または WebSocket のフレーミングが不正だった。
    Framing(&'static str),
    /// 応答が完了する前にサーバが接続を閉じた。
    ClosedEarly,
}

/// タイムアウトが起きた通信の段階。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// 接続の確立。
    Connect,
    /// HTTP リクエスト。
    Request,
    /// WebSocket ハンドシェイク。
    WsHandshake,
    /// WebSocket の書き込み。
    WsWrite,
    /// WebSocket のアイドル (受信がない状態)。
    WsIdle,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Phase::Connect => "接続",
            Phase::Request => "リクエスト",
            Phase::WsHandshake => "WebSocket ハンドシェイク",
            Phase::WsWrite => "WebSocket 書き込み",
            Phase::WsIdle => "WebSocket アイドル",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 呼び出し側が `Box<dyn std::error::Error>` を返す関数で `?` を使えること。
    /// `Feed::subscribe` は `ContractViolation` をそのまま返す唯一の公開 API で、
    /// I/O なしに呼べる。
    #[test]
    fn every_detail_type_is_an_error_so_question_mark_boxes_it() {
        fn subscribe_with_question_mark() -> Result<(), Box<dyn std::error::Error>> {
            use crate::ws::PublicChannel;
            let mut feed = crate::Client::production().public_feed();
            feed.subscribe(PublicChannel::Ticker(crate::Symbol::BTC))?;
            Ok(())
        }
        fn is_error<E: std::error::Error + Send + Sync + 'static>() {}
        is_error::<ContractViolation>();
        is_error::<ApiError>();
        is_error::<ProtocolError>();
        is_error::<TransportError>();
        subscribe_with_question_mark().unwrap();
    }

    #[test]
    fn error_codes_parse_only_the_documented_shape() {
        assert_eq!(ErrorCode::parse("ERR-5003"), Some(ErrorCode::RATE_LIMIT));
        assert_eq!(
            ErrorCode::parse("ERR-70"),
            Some(ErrorCode::WRONG_POSITION_SIDE)
        );
        assert_eq!(ErrorCode::parse("ERR-"), None);
        assert_eq!(
            ErrorCode::parse("ERR-70000"),
            Some(ErrorCode(70000)),
            "five digits reach past u16"
        );
        assert_eq!(ErrorCode::parse("ERR-123456"), None);
        assert_eq!(ErrorCode::parse("5003"), None);
        assert_eq!(ErrorCode::parse("ERR-5x"), None);
    }

    #[test]
    fn classification_helpers() {
        let api = Error::Api(ApiError {
            operation: "GET /v1/status",
            status: 1,
            messages: vec![ApiMessage {
                code: ErrorCode::RATE_LIMIT,
                text: "Requests are too many.".into(),
            }],
            response_time: None,
        });
        assert!(api.is_rate_limited());
        assert!(!api.outcome_unknown());
        assert!(!api.accepted_but_unreadable());
        let text = api.to_string();
        assert!(text.contains("GET /v1/status") && text.contains("ERR-5003"));

        let protocol = |api, operation, kind| {
            Error::Protocol(ProtocolError {
                api,
                operation,
                kind,
            })
        };
        let post_gateway = protocol(
            ApiGroup::PrivateRest,
            "POST /v1/order",
            ProtocolErrorKind::HttpStatus(502),
        );
        assert!(post_gateway.outcome_unknown());
        assert!(!post_gateway.accepted_but_unreadable());
        let post_unreadable = protocol(
            ApiGroup::PrivateRest,
            "POST /v1/order",
            ProtocolErrorKind::Shape { line: 1, column: 1 },
        );
        assert!(post_unreadable.outcome_unknown());
        assert!(post_unreadable.accepted_but_unreadable());
        let delete_missing = protocol(
            ApiGroup::PrivateRest,
            "DELETE /v1/ws-auth",
            ProtocolErrorKind::MissingData,
        );
        assert!(delete_missing.outcome_unknown() && delete_missing.accepted_but_unreadable());
        let get_gateway = protocol(
            ApiGroup::PublicRest,
            "GET /v1/status",
            ProtocolErrorKind::HttpStatus(502),
        );
        assert!(!get_gateway.outcome_unknown());
        let get_unreadable = protocol(
            ApiGroup::PublicRest,
            "GET /v1/ticker",
            ProtocolErrorKind::Postcondition(Postcondition::WrongSymbol),
        );
        assert!(
            !get_unreadable.outcome_unknown() && !get_unreadable.accepted_but_unreadable(),
            "a read changes nothing, so neither predicate applies"
        );
        let ws = protocol(
            ApiGroup::PrivateWs,
            "/v1",
            ProtocolErrorKind::WsHandshake { http_status: 503 },
        );
        assert!(!ws.outcome_unknown() && !ws.accepted_but_unreadable());

        let unknown = Error::Transport(TransportError {
            api: ApiGroup::PrivateRest,
            operation: "POST /v1/order",
            kind: TransportErrorKind::ClosedEarly,
            delivery: Delivery::MaybeSent,
        });
        assert!(unknown.outcome_unknown());
        assert!(unknown.to_string().contains("不明"));
        let not_sent = Error::Transport(TransportError {
            api: ApiGroup::PrivateRest,
            operation: "POST /v1/order",
            kind: TransportErrorKind::Timeout(Phase::Connect),
            delivery: Delivery::NotSent,
        });
        assert!(!not_sent.outcome_unknown());
        assert!(not_sent.to_string().contains("送信されていません"));
    }
}
