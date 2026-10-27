//! The crate's one error type.
//!
//! Every variant answers "who is at fault and was anything sent":
//!
//! | variant      | at fault             | request sent?              |
//! |--------------|----------------------|----------------------------|
//! | `Contract`   | the caller's input   | never                      |
//! | `Api`        | exchange said no     | yes, and it was processed  |
//! | `Protocol`   | exchange broke shape | yes                        |
//! | `Transport`  | network, TLS, limits | see [`Delivery`]           |
//! | `FeedClosed` | feed already ended   | not applicable             |
//!
//! Redaction is structural: errors hold `&'static str` operation names
//! (`"POST /v1/order"`), `ApiGroup`, numeric codes, and `io::ErrorKind`. They
//! never hold a URL, a header, a request body, or a serde message (serde
//! messages quote the offending JSON value, and the `ws-auth` response value
//! is the access token). A `'static` string cannot contain a runtime token.

use core::fmt;
use std::io;

use crate::domain::{InvalidValue, OrderId, Timestamp};

pub type Result<T, E = Error> = core::result::Result<T, E>;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A documented precondition failed. Nothing was sent.
    Contract(ContractViolation),
    /// The exchange answered `status != 0`.
    Api(ApiError),
    /// The exchange answered, but not in the documented shape, or a
    /// postcondition on the answer failed. Fails closed: no default is
    /// substituted for a missing or unknown part.
    Protocol(ProtocolError),
    /// The exchange may not have answered at all.
    Transport(TransportError),
    /// `Feed::next` after the feed ended (`close`, or an earlier terminal error).
    FeedClosed,
}

impl Error {
    /// The exchange rejected the call for exceeding its rate limit (ERR-5003).
    /// The crate never throttles REST calls; this is the caller's signal.
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Error::Api(e) if e.has(ErrorCode::RATE_LIMIT))
    }

    /// A state-changing request may or may not have taken effect. For an
    /// order, resend with the same `ClientOrderId`: ERR-915 then means the
    /// first attempt was placed, and `orders(OrderLookup::ByClientOrderIds)`
    /// returns it.
    pub fn outcome_unknown(&self) -> bool {
        matches!(self, Error::Transport(t) if t.delivery == Delivery::MaybeSent)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

impl From<ContractViolation> for Error {
    fn from(v: ContractViolation) -> Self {
        Error::Contract(v)
    }
}

/// A precondition that the argument types could not carry, checked at the
/// public API boundary before any byte is sent.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContractViolation {
    /// A domain primitive rejected a value (for example `Batch` with 11 ids).
    InvalidValue(InvalidValue),
    /// `cancelBefore` on `/order` requires a spot symbol and `Side::Sell`.
    CancelBeforeRequiresSpotSell,
    /// `losscutPrice` is accepted only for leverage symbols.
    LosscutRequiresLeverageSymbol,
    /// `closeOrder`, `closeBulkOrder`, and `openPositions` take leverage symbols only.
    RequiresLeverageSymbol,
    /// A feed already holds `Limits::max_subscriptions` channels.
    TooManySubscriptions { max: u8 },
}

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

/// `status != 0`.
///
/// # Invariants
/// `messages` is non-empty. An error envelope without messages is a
/// `Protocol` error, not an `ApiError` with nothing in it.
#[derive(Clone, Debug)]
pub struct ApiError {
    pub(crate) operation: &'static str,
    pub(crate) status: i64,
    pub(crate) messages: Vec<ApiMessage>,
    pub(crate) response_time: Option<Timestamp>,
}

impl ApiError {
    pub fn operation(&self) -> &'static str {
        self.operation
    }

    /// The envelope's non-zero `status`.
    pub fn status(&self) -> i64 {
        self.status
    }

    pub fn messages(&self) -> &[ApiMessage] {
        &self.messages
    }

    pub fn has(&self, code: ErrorCode) -> bool {
        self.messages.iter().any(|m| m.code == code)
    }

    /// Absent on some errors (`live/error_without_responsetime.json`).
    pub fn response_time(&self) -> Option<Timestamp> {
        self.response_time
    }
}

/// One `{message_code, message_string}` pair, from an error envelope or from
/// a `cancelOrders` failure entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiMessage {
    pub code: ErrorCode,
    /// Server-authored text, at most 512 bytes (longer is a `Protocol` error).
    pub text: Box<str>,
}

/// `ERR-<n>` as a number. Open by construction: an undocumented code is just
/// another value, and the documented ones are constants usable in `match`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ErrorCode(pub u16);

impl ErrorCode {
    pub const WRONG_POSITION_SIDE: ErrorCode = ErrorCode(70);
    pub const INSUFFICIENT_BALANCE: ErrorCode = ErrorCode(201);
    pub const ACTIVE_ORDER_LIMIT: ErrorCode = ErrorCode(635);
    pub const HISTORY_RANGE_TOO_WIDE: ErrorCode = ErrorCode(846);
    pub const CLIENT_ORDER_ID_USED: ErrorCode = ErrorCode(915);
    pub const RATE_LIMIT: ErrorCode = ErrorCode(5003);
    pub const TIMESTAMP_MISSING: ErrorCode = ErrorCode(5007);
    pub const TIMESTAMP_TOO_LATE: ErrorCode = ErrorCode(5008);
    pub const TIMESTAMP_TOO_EARLY: ErrorCode = ErrorCode(5009);
    pub const BAD_SIGNATURE: ErrorCode = ErrorCode(5010);
    pub const API_KEY_MISSING: ErrorCode = ErrorCode(5011);
    pub const API_KEY_REJECTED: ErrorCode = ErrorCode(5012);
    pub const HISTORY_TIMEOUT: ErrorCode = ErrorCode(5016);
    pub const INVALID_PARAMETER: ErrorCode = ErrorCode(5106);
    pub const ORDER_NOT_MODIFIABLE: ErrorCode = ErrorCode(5122);
    pub const ORDER_NOT_FOUND: ErrorCode = ErrorCode(5123);
    pub const SCHEDULED_MAINTENANCE: ErrorCode = ErrorCode(5201);
    pub const EMERGENCY_MAINTENANCE: ErrorCode = ErrorCode(5202);
    pub const PRE_OPEN: ErrorCode = ErrorCode(5203);
    pub const INVALID_SYMBOL_INTERVAL_DATE: ErrorCode = ErrorCode(5207);
    // TODO the remaining documented codes from the reference table.

    /// Parses `ERR-<1..=5 digits>`. Anything else is a `Protocol` error.
    pub(crate) fn parse(text: &str) -> Option<ErrorCode> {
        let digits = text.strip_prefix("ERR-")?;
        if digits.is_empty() || digits.len() > 5 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok().map(ErrorCode)
    }
}

/// Which of the four APIs an operation belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiGroup {
    PublicRest,
    PrivateRest,
    PublicWs,
    PrivateWs,
}

#[derive(Debug)]
pub struct ProtocolError {
    pub(crate) api: ApiGroup,
    pub(crate) operation: &'static str,
    pub(crate) kind: ProtocolErrorKind,
}

impl ProtocolError {
    pub fn api(&self) -> ApiGroup {
        self.api
    }
    pub fn operation(&self) -> &'static str {
        self.operation
    }
    pub fn kind(&self) -> &ProtocolErrorKind {
        &self.kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProtocolErrorKind {
    /// HTTP status outside 2xx and the body is not an envelope (404, 503).
    HttpStatus(u16),
    /// Body is not a JSON object with an integer `status`.
    NotAnEnvelope,
    /// `status != 0` but `messages` is missing, empty, or malformed.
    MalformedErrorEnvelope,
    /// `status == 0` but the endpoint's `data` is missing.
    MissingData,
    /// `data` did not match the documented shape. Position only: serde's
    /// message is dropped because it quotes values.
    Shape { line: usize, column: usize },
    /// A postcondition on a parsed answer failed.
    Postcondition(Postcondition),
    /// WebSocket handshake answered something other than `101` with the
    /// right `Sec-WebSocket-Accept`.
    WsHandshake { http_status: u16 },
    /// A WebSocket text message on a subscribed channel did not decode.
    WsMessage { line: usize, column: usize },
}

/// Named postconditions, so tests and callers can match the exact rule.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Postcondition {
    /// `cancelOrders` reported an id the caller did not send.
    CancelResultIdNotRequested(OrderId),
    /// `cancelOrders` reported one id as both cancelled and failed, or twice.
    CancelResultIdRepeated(OrderId),
    /// `ticker(symbol)` / `order_book(symbol)` answered for another symbol,
    /// or not exactly one entry.
    WrongSymbol,
    /// `orders(..)` returned an order the lookup did not name.
    OrderNotRequested(OrderId),
    /// A single-entry `data` array (`account/transfer`) had another length.
    ExpectedOneEntry { actual: usize },
}

#[derive(Debug)]
pub struct TransportError {
    pub(crate) api: ApiGroup,
    pub(crate) operation: &'static str,
    pub(crate) kind: TransportErrorKind,
    pub(crate) delivery: Delivery,
}

impl TransportError {
    pub fn api(&self) -> ApiGroup {
        self.api
    }
    pub fn operation(&self) -> &'static str {
        self.operation
    }
    pub fn kind(&self) -> &TransportErrorKind {
        &self.kind
    }
    pub fn delivery(&self) -> Delivery {
        self.delivery
    }
}

/// Whether the request could have reached the exchange.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Failed before the last request byte was handed to the socket.
    NotSent,
    /// The full request was written; the outcome is unknown.
    MaybeSent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportErrorKind {
    /// Kind only; an `io::Error` payload can carry arbitrary text.
    Io(io::ErrorKind),
    Tls,
    Timeout(Phase),
    /// A size limit from `Limits` was exceeded before the data was buffered.
    TooLarge { limit: u32 },
    /// The HTTP or WebSocket framing from the server was invalid.
    Framing(&'static str),
    /// The server closed the connection before a complete response.
    ClosedEarly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Connect,
    Request,
    WsHandshake,
    WsWrite,
    WsIdle,
}
