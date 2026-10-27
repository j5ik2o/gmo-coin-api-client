//! Domain primitives.
//!
//! Every value with domain meaning is its own immutable type. Each one
//! validates on construction, in this fixed order, so a huge input is
//! rejected before any parsing work happens:
//!
//! 1. size (byte length against the type's `MAX_LEN`),
//! 2. lexical content (allowed byte set),
//! 3. syntax (structure, for example `-?digits(.digits)?`),
//! 4. semantics (for example "not negative", "at most 30 minutes").
//!
//! [`InvalidValue`] reports which step failed and for which primitive. It
//! never carries the rejected input, because some inputs are secrets.
//!
//! The same constructors back `Deserialize`, so a value decoded from the
//! exchange passed exactly the checks a caller-built value passed. Inside the
//! crate these types are trusted and never re-validated.

mod enums;
mod id;
mod number;
mod symbol;
mod time;

pub use enums::{
    CancelType, CryptoShortfallStatus, ExchangeStatus, ExecutionMsgType, ExecutionType, FeeType,
    LimitTimeInForce, MarginCallStatus, OrderMsgType, OrderStatus, OrderType, PositionMsgType,
    SettleFilter, SettleType, Side, SummaryMsgType, TimeInForce, TransferStatus, TransferType,
    UnknownValue,
};
pub use id::{Batch, ClientOrderId, ExecutionId, OrderId, PositionId};
pub use number::{Amount, Decimal, Price, Size};
pub use symbol::{Market, Symbol};
pub use time::{
    BusinessDate, BusinessDateRange, HistoryWindow, IntradayInterval, KlineSpan, LongInterval,
    Timestamp,
};

/// Which domain primitive rejected a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Primitive {
    Decimal,
    Price,
    Size,
    Amount,
    Symbol,
    OrderId,
    ExecutionId,
    PositionId,
    ClientOrderId,
    Batch,
    Timestamp,
    BusinessDate,
    HistoryWindow,
    BusinessDateRange,
    EnumValue,
    ApiKey,
    ApiSecret,
    AccessToken,
    BaseUrl,
    Config,
    PageRequest,
}

/// The validation step that failed. Ordered the way the checks run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Violation {
    /// Step 1. Longer than `max` bytes, or shorter than `min`.
    Size {
        min: usize,
        max: usize,
        actual: usize,
    },
    /// Step 2. A byte outside the allowed set, at this byte offset.
    Lexical { offset: usize },
    /// Step 3. Bytes are allowed but the structure is wrong.
    Syntax(&'static str),
    /// Step 4. Well formed but outside the domain (for example negative).
    Semantics(&'static str),
}

/// A value failed a domain primitive's validation.
///
/// Invariant: never contains the rejected input (secrets pass through the
/// same path), only the primitive and the failed step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidValue {
    pub primitive: Primitive,
    pub violation: Violation,
}

impl core::fmt::Display for InvalidValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "invalid {:?}: {:?}", self.primitive, self.violation)
    }
}

impl std::error::Error for InvalidValue {}

/// Shared first two validation steps. Every text primitive calls this before
/// its own syntax and semantic checks, so the order cannot drift per type.
///
/// # Postconditions
/// On `Ok`, `input.len()` is in `min..=max` and every byte satisfies `allowed`.
pub(crate) fn check_size_and_lexical(
    primitive: Primitive,
    input: &str,
    min: usize,
    max: usize,
    allowed: impl Fn(u8) -> bool,
) -> Result<(), InvalidValue> {
    let actual = input.len();
    if actual < min || actual > max {
        return Err(InvalidValue {
            primitive,
            violation: Violation::Size { min, max, actual },
        });
    }
    match input.bytes().position(|b| !allowed(b)) {
        Some(offset) => Err(InvalidValue {
            primitive,
            violation: Violation::Lexical { offset },
        }),
        None => Ok(()),
    }
}

/// The one `Deserialize` path for text primitives: accept a JSON string
/// (borrowed or escaped) and run the primitive's own constructor on it.
/// Any other JSON type is rejected, never coerced (fail closed). The serde
/// error names the primitive and the failed step, never the value.
pub(crate) fn deserialize_text<'de, D, T>(
    d: D,
    parse: fn(&str) -> Result<T, InvalidValue>,
) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Text<T>(fn(&str) -> Result<T, InvalidValue>);
    impl<'de, T> serde::de::Visitor<'de> for Text<T> {
        type Value = T;
        fn expecting(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("a JSON string")
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<T, E> {
            (self.0)(v).map_err(E::custom)
        }
    }
    d.deserialize_str(Text(parse))
}
