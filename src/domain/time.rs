//! Time values, built on `std::time` only.

use core::fmt;
use core::str::FromStr;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Deserializer};

use super::{InvalidValue, Primitive, Violation, check_size_and_lexical, deserialize_text};

/// An instant on the exchange's clock, UTC.
///
/// Two wire forms decode into it: ISO-8601 `2019-03-19T02:15:06.001Z` (every
/// `timestamp` and `responsetime`) and a millisecond Unix-time string
/// `"1618588800000"` (kline `openTime`).
///
/// # Invariants
/// Nanoseconds since the Unix epoch, within `0..=i64::MAX` (years 1970 to
/// 2262). Up to nine fraction digits are kept, so nothing is truncated.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamp {
    unix_nanos: i64,
}

impl Timestamp {
    /// `YYYY-MM-DDTHH:MM:SS` + optional `.f{1,9}` + `Z`.
    pub const MAX_ISO_LEN: usize = 30;

    /// From ISO-8601 UTC text.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, the instant `text` denotes, with every fraction digit kept.
    /// # Errors
    /// `Size`, `Lexical` (outside `0-9 - : . T Z`), `Syntax` (field layout),
    /// `Semantics` (month 13, Feb 30, before 1970).
    pub fn parse_iso8601(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::Timestamp, text, 20, Self::MAX_ISO_LEN, |b| {
            b.is_ascii_digit() || matches!(b, b'-' | b':' | b'.' | b'T' | b'Z')
        })?;
        // TODO syntax: fixed offsets for '-', 'T', ':', optional '.' + 1..=9
        // digits, trailing 'Z' (no other offsets: the exchange always sends Z).
        // TODO semantics: validate the civil date, then days_from_civil
        // (Hinnant's algorithm, pure integer arithmetic) * 86_400 + seconds,
        // scaled to nanos with checked arithmetic.
        todo!()
    }

    /// From a millisecond Unix-time string.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `unix_millis()` equals the parsed number.
    /// # Errors
    /// `Size` (over 19 digits), `Lexical` (non-digit), `Semantics` (overflow).
    pub fn parse_unix_millis(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::Timestamp, text, 1, 19, |b| b.is_ascii_digit())?;
        // TODO u64 parse, checked_mul(1_000_000), fits i64.
        todo!()
    }

    /// From a `SystemTime`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `to_system_time() == t`.
    ///
    /// # Errors
    /// `Semantics` for an instant before 1970 or after 2262.
    pub fn from_system_time(t: SystemTime) -> Result<Self, InvalidValue> {
        let _ = t;
        todo!()
    }

    pub fn to_system_time(self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(self.unix_nanos as u64)
    }

    pub const fn unix_millis(self) -> i64 {
        self.unix_nanos / 1_000_000
    }

    /// `self - earlier`, or `None` when `earlier` is later.
    pub fn duration_since(self, earlier: Timestamp) -> Option<Duration> {
        let delta = self.unix_nanos.checked_sub(earlier.unix_nanos)?;
        u64::try_from(delta).ok().map(Duration::from_nanos)
    }

    /// For `#[serde(deserialize_with = "Timestamp::deserialize_millis")]`.
    pub(crate) fn deserialize_millis<'de, D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, Timestamp::parse_unix_millis)
    }
}

impl fmt::Display for Timestamp {
    /// ISO-8601 with millisecond precision when the sub-millisecond part is
    /// zero (the exchange's own form), otherwise nine fraction digits.
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({self})")
    }
}

impl FromStr for Timestamp {
    type Err = InvalidValue;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_iso8601(s)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, Timestamp::parse_iso8601)
    }
}

/// A business day in `YYYYMMDD` form (exchange business days run 06:00 to
/// 05:59 JST; the exchange decides which day a moment belongs to).
///
/// # Invariants
/// A valid Gregorian date from 2018-01-01 on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BusinessDate {
    year: u16,
    month: u8,
    day: u8,
}

impl BusinessDate {
    /// From calendar parts.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `to_string()` is the arguments as `YYYYMMDD`.
    ///
    /// # Errors
    /// `Semantics("no such day")` or `Semantics("before 2018")`.
    pub fn new(year: u16, month: u8, day: u8) -> Result<Self, InvalidValue> {
        let _ = (year, month, day);
        todo!()
    }

    /// From `YYYYMMDD` text.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `to_string() == text`.
    /// # Errors
    /// `Size` (not 8 bytes), `Lexical` (non-digit), `Semantics` (no such day).
    pub fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::BusinessDate, text, 8, 8, |b| b.is_ascii_digit())?;
        todo!()
    }

    /// Whole days from `earlier` to `self`; negative when reversed.
    pub fn days_since(self, earlier: BusinessDate) -> i64 {
        let _ = earlier;
        todo!()
    }
}

impl fmt::Display for BusinessDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}{:02}{:02}", self.year, self.month, self.day)
    }
}

impl<'de> Deserialize<'de> for BusinessDate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, BusinessDate::parse)
    }
}

/// `fromTimestamp`/`toTimestamp` for the deposit and withdrawal histories.
///
/// # Invariants
/// When `to` is present, `from < to` and `to - from <= 30 minutes` (the
/// documented limit; the exchange answers ERR-846 otherwise). When `to` is
/// absent the exchange uses `from + 30 minutes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryWindow {
    from: Timestamp,
    to: Option<Timestamp>,
}

impl HistoryWindow {
    pub const MAX_SPAN: Duration = Duration::from_secs(30 * 60);

    /// A window from `from`, closed by the exchange's 30-minute default.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// `to()` is `None`; the exchange applies `from + 30 minutes`.
    ///
    /// # Errors
    /// Infallible.
    pub fn starting_at(from: Timestamp) -> Self {
        Self { from, to: None }
    }

    /// A window with both ends.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `from() == from` and `to() == Some(to)`.
    /// # Errors
    /// `Semantics("to is not after from")`, `Semantics("span over 30 minutes")`.
    pub fn between(from: Timestamp, to: Timestamp) -> Result<Self, InvalidValue> {
        let invalid = |why| InvalidValue { primitive: Primitive::HistoryWindow, violation: Violation::Semantics(why) };
        match to.duration_since(from) {
            None | Some(Duration::ZERO) => Err(invalid("to is not after from")),
            Some(span) if span > Self::MAX_SPAN => Err(invalid("span over 30 minutes")),
            Some(_) => Ok(Self { from, to: Some(to) }),
        }
    }

    pub fn from(&self) -> Timestamp {
        self.from
    }

    pub fn to(&self) -> Option<Timestamp> {
        self.to
    }
}

/// `fromDate`/`toDate` for the exchange-fee history.
///
/// # Invariants
/// When `to` is present, `from <= to` and `to - from <= 7 days`. "Not in the
/// future" is left to the exchange: it depends on the exchange's clock and
/// business-day cutover, which this crate does not own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BusinessDateRange {
    from: BusinessDate,
    to: Option<BusinessDate>,
}

impl BusinessDateRange {
    pub const MAX_DAYS: i64 = 7;

    /// One business day.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// `to()` is `None`: one business day.
    ///
    /// # Errors
    /// Infallible.
    pub fn single(day: BusinessDate) -> Self {
        Self { from: day, to: None }
    }

    /// A range with both ends.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `from() == from` and `to() == Some(to)`.
    /// # Errors
    /// `Semantics("to before from")`, `Semantics("span over 7 days")`.
    pub fn between(from: BusinessDate, to: BusinessDate) -> Result<Self, InvalidValue> {
        let invalid = |why| InvalidValue { primitive: Primitive::BusinessDateRange, violation: Violation::Semantics(why) };
        match to.days_since(from) {
            d if d < 0 => Err(invalid("to before from")),
            d if d > Self::MAX_DAYS => Err(invalid("span over 7 days")),
            _ => Ok(Self { from, to: Some(to) }),
        }
    }

    pub fn from(&self) -> BusinessDate {
        self.from
    }

    pub fn to(&self) -> Option<BusinessDate> {
        self.to
    }
}

/// Which klines to fetch. The docs tie the `date` format to the interval:
/// minute and hour intervals take a day (`YYYYMMDD`, from 2021-04-15 on),
/// 4-hour and longer take a year (`YYYY`). The variants make the other
/// pairings unrepresentable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KlineSpan {
    /// `1min 5min 10min 15min 30min 1hour` for one business day.
    Day { interval: IntradayInterval, date: BusinessDate },
    /// `4hour 8hour 12hour 1day 1week 1month` for one year.
    Year { interval: LongInterval, year: u16 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntradayInterval {
    Min1,
    Min5,
    Min10,
    Min15,
    Min30,
    Hour1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LongInterval {
    Hour4,
    Hour8,
    Hour12,
    Day1,
    Week1,
    Month1,
}

impl KlineSpan {
    /// The `(interval, date)` query values, for example `("1min", "20210417")`.
    pub(crate) fn query(&self) -> (&'static str, String) {
        todo!()
    }
}
