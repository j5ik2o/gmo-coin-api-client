//! 時刻の値。`std::time` だけで構築する。

use core::fmt;
use core::str::FromStr;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Deserializer};

use super::{InvalidValue, Primitive, Violation, check_size_and_lexical, deserialize_text};

/// 取引所の時計上の瞬間 (UTC)。
///
/// 2 つのワイヤ形式がこの型にデコードされる。ISO-8601 の
/// `2019-03-19T02:15:06.001Z` (すべての `timestamp` と `responsetime`) と、
/// ミリ秒 Unix 時刻の文字列 `"1618588800000"` (kline の `openTime`) である。
///
/// # 不変条件
/// Unix エポックからのナノ秒で、`0..=i64::MAX` の範囲 (1970 年から 2262 年)。
/// 小数部は 9 桁まで保持するので、何も切り捨てない。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamp {
    unix_nanos: i64,
}

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

impl Timestamp {
    /// `YYYY-MM-DDTHH:MM:SS` + 省略可能な `.f{1,9}` + `Z`。
    pub const MAX_ISO_LEN: usize = 30;
    const MIN_ISO_LEN: usize = 20;

    /// ISO-8601 の UTC 文字列から構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `text` が表す瞬間で、小数部の桁はすべて保持される。
    ///
    /// # エラー
    /// `Size`、`Lexical` (`0-9 - : . T Z` 以外)、`Syntax` (フィールドの配置)、
    /// `Semantics` (13 月、2 月 30 日、1970 年より前)。
    pub fn parse_iso8601(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(
            Primitive::Timestamp,
            text,
            Self::MIN_ISO_LEN,
            Self::MAX_ISO_LEN,
            |b| b.is_ascii_digit() || matches!(b, b'-' | b':' | b'.' | b'T' | b'Z'),
        )?;
        let invalid = |violation| InvalidValue {
            primitive: Primitive::Timestamp,
            violation,
        };
        let syntax = |why| invalid(Violation::Syntax(why));
        let b = text.as_bytes();
        let at = |i: usize| b.get(i).copied();
        let field = |r: core::ops::Range<usize>| b.get(r).and_then(parse_digits);
        let layout_ok = at(4) == Some(b'-')
            && at(7) == Some(b'-')
            && at(10) == Some(b'T')
            && at(13) == Some(b':')
            && at(16) == Some(b':');
        let fraction = b.get(19..).and_then(|rest| rest.strip_suffix(b"Z"));
        let (true, Some(fraction), Some(year), Some(month), Some(day)) =
            (layout_ok, fraction, field(0..4), field(5..7), field(8..10))
        else {
            return Err(syntax("expected YYYY-MM-DDTHH:MM:SS[.f]Z"));
        };
        let (Some(hour), Some(minute), Some(second)) =
            (field(11..13), field(14..16), field(17..19))
        else {
            return Err(syntax("expected YYYY-MM-DDTHH:MM:SS[.f]Z"));
        };
        let fraction_nanos = match fraction {
            [] => 0,
            [b'.', digits @ ..] if !digits.is_empty() && digits.iter().all(u8::is_ascii_digit) => {
                digits
                    .iter()
                    .chain(core::iter::repeat(&b'0'))
                    .take(9)
                    .fold(0i64, |acc, d| acc * 10 + i64::from(d - b'0'))
            }
            _ => return Err(syntax("fraction must be '.' followed by 1 to 9 digits")),
        };
        if hour > 23 || minute > 59 || second > 59 {
            return Err(invalid(Violation::Semantics("no such time of day")));
        }
        if year < 1970 {
            return Err(invalid(Violation::Semantics("before 1970")));
        }
        let days = days_from_civil(year, month, day)
            .ok_or_else(|| invalid(Violation::Semantics("no such day")))?;
        let seconds =
            days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
        let unix_nanos = seconds
            .checked_mul(NANOS_PER_SEC)
            .and_then(|n| n.checked_add(fraction_nanos))
            .ok_or_else(|| invalid(Violation::Semantics("after 2262")))?;
        Ok(Self { unix_nanos })
    }

    /// ミリ秒 Unix 時刻の文字列から構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `unix_millis()` は解析した数値に等しい。
    ///
    /// # エラー
    /// `Size` (19 桁超)、`Lexical` (数字以外)、`Semantics` (オーバーフロー)。
    pub fn parse_unix_millis(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::Timestamp, text, 1, 19, |b| b.is_ascii_digit())?;
        let overflow = InvalidValue {
            primitive: Primitive::Timestamp,
            violation: Violation::Semantics("after 2262"),
        };
        let millis: i64 = text.parse().map_err(|_| overflow)?;
        millis
            .checked_mul(NANOS_PER_MILLI)
            .map(|unix_nanos| Self { unix_nanos })
            .ok_or(overflow)
    }

    /// `SystemTime` から構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `to_system_time() == t`。
    ///
    /// # エラー
    /// 1970 年より前、または 2262 年より後の瞬間に `Semantics`。
    pub fn from_system_time(t: SystemTime) -> Result<Self, InvalidValue> {
        let invalid = |why| InvalidValue {
            primitive: Primitive::Timestamp,
            violation: Violation::Semantics(why),
        };
        let since_epoch = t
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| invalid("before 1970"))?;
        let unix_nanos =
            i64::try_from(since_epoch.as_nanos()).map_err(|_| invalid("after 2262"))?;
        Ok(Self { unix_nanos })
    }

    pub fn to_system_time(self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(self.unix_nanos.cast_unsigned())
    }

    pub const fn unix_millis(self) -> i64 {
        self.unix_nanos / NANOS_PER_MILLI
    }

    /// `self - earlier`。`earlier` のほうが後なら `None`。
    pub fn duration_since(self, earlier: Timestamp) -> Option<Duration> {
        let delta = self.unix_nanos.checked_sub(earlier.unix_nanos)?;
        u64::try_from(delta).ok().map(Duration::from_nanos)
    }

    /// `#[serde(deserialize_with = "Timestamp::deserialize_millis")]` 用。
    pub(crate) fn deserialize_millis<'de, D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, Timestamp::parse_unix_millis)
    }
}

impl fmt::Display for Timestamp {
    /// ISO-8601。ミリ秒未満がゼロならミリ秒精度 (取引所自身の形式)、
    /// そうでなければ小数 9 桁。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.unix_nanos.div_euclid(NANOS_PER_SEC);
        let nanos = self.unix_nanos.rem_euclid(NANOS_PER_SEC);
        let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
        let of_day = seconds.rem_euclid(86_400);
        write!(
            f,
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
            of_day / 3_600,
            (of_day / 60) % 60,
            of_day % 60
        )?;
        if nanos % NANOS_PER_MILLI == 0 {
            write!(f, ".{:03}Z", nanos / NANOS_PER_MILLI)
        } else {
            write!(f, ".{nanos:09}Z")
        }
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

/// 1970-01-01 からの日数 (Howard Hinnant の `days_from_civil`、整数演算のみ)。
/// 暦として存在しない日付には `None`。
/// ASCII 数字だけの並びを数値にする。空、数字以外、桁あふれは `None`。
fn parse_digits(digits: &[u8]) -> Option<u32> {
    if digits.is_empty() {
        return None;
    }
    digits.iter().try_fold(0u32, |acc, d| {
        let digit = char::from(*d).to_digit(10)?;
        acc.checked_mul(10)?.checked_add(digit)
    })
}

fn days_from_civil(year: u32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    let y = i64::from(year) - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

/// `days_from_civil` の逆。
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (y + i64::from(m <= 2), m, d)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// `YYYYMMDD` 形式の営業日 (取引所の営業日は日本時間 06:00 から 05:59 まで。
/// ある瞬間がどの日に属するかは取引所が決める)。
///
/// # 不変条件
/// 2018-01-01 以降の、暦として存在するグレゴリオ暦の日付。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BusinessDate {
    year: u16,
    month: u8,
    day: u8,
}

impl BusinessDate {
    const FIRST_YEAR: u16 = 2018;

    /// 暦の要素から構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `to_string()` は引数を `YYYYMMDD` にしたもの。
    ///
    /// # エラー
    /// `Semantics("no such day")` または `Semantics("before 2018")`。
    pub fn new(year: u16, month: u8, day: u8) -> Result<Self, InvalidValue> {
        let invalid = |why| InvalidValue {
            primitive: Primitive::BusinessDate,
            violation: Violation::Semantics(why),
        };
        if days_from_civil(u32::from(year), u32::from(month), u32::from(day)).is_none() {
            return Err(invalid("no such day"));
        }
        if year < Self::FIRST_YEAR {
            return Err(invalid("before 2018"));
        }
        Ok(Self { year, month, day })
    }

    /// `YYYYMMDD` の文字列から構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `to_string() == text`。
    ///
    /// # エラー
    /// `Size` (8 バイトでない)、`Lexical` (数字以外)、`Semantics` (存在しない日)。
    pub fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::BusinessDate, text, 8, 8, |b| b.is_ascii_digit())?;
        let number = |r: core::ops::Range<usize>| {
            text.as_bytes()
                .get(r)
                .and_then(parse_digits)
                .and_then(|n| u16::try_from(n).ok())
                .unwrap_or(u16::MAX)
        };
        let (month, day) = (number(4..6), number(6..8));
        Self::new(
            number(0..4),
            u8::try_from(month).unwrap_or(u8::MAX),
            u8::try_from(day).unwrap_or(u8::MAX),
        )
    }

    /// `earlier` から `self` までの日数。逆順なら負。
    pub fn days_since(self, earlier: BusinessDate) -> i64 {
        self.days() - earlier.days()
    }

    #[expect(
        clippy::expect_used,
        reason = "BusinessDate::new が実在する日だけを通すので、ここで None になる値は作れない"
    )]
    fn days(self) -> i64 {
        days_from_civil(
            u32::from(self.year),
            u32::from(self.month),
            u32::from(self.day),
        )
        .expect("BusinessDate::new admits only real days")
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

/// 入出金履歴の `fromTimestamp`/`toTimestamp`。
///
/// # 不変条件
/// `to` があるとき `from < to` かつ `to - from <= 30 分` (ドキュメントの上限。
/// 超えると取引所は ERR-846 を返す)。`to` がないとき取引所は `from + 30 分` を使う。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryWindow {
    from: Timestamp,
    to: Option<Timestamp>,
}

impl HistoryWindow {
    pub const MAX_SPAN: Duration = Duration::from_mins(30);

    /// `from` から始まり、取引所の既定 (30 分) で閉じる窓。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `to()` は `None`。取引所が `from + 30 分` を適用する。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn starting_at(from: Timestamp) -> Self {
        Self { from, to: None }
    }

    /// 両端のある窓。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `from() == from` かつ `to() == Some(to)`。
    ///
    /// # エラー
    /// `Semantics("to is not after from")`、`Semantics("span over 30 minutes")`。
    pub fn between(from: Timestamp, to: Timestamp) -> Result<Self, InvalidValue> {
        let invalid = |why| InvalidValue {
            primitive: Primitive::HistoryWindow,
            violation: Violation::Semantics(why),
        };
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

/// 取引所手数料履歴の `fromDate`/`toDate`。
///
/// # 不変条件
/// `to` があるとき `from <= to` かつ `to - from <= 7 日`。「未来でない」は
/// 取引所に委ねる。取引所の時計と営業日の切り替えに依存し、このクレートは
/// それを持たないからである。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BusinessDateRange {
    from: BusinessDate,
    to: Option<BusinessDate>,
}

impl BusinessDateRange {
    pub const MAX_DAYS: i64 = 7;

    /// 営業日 1 日。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `to()` は `None`。営業日 1 日分。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn single(day: BusinessDate) -> Self {
        Self {
            from: day,
            to: None,
        }
    }

    /// 両端のある範囲。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `from() == from` かつ `to() == Some(to)`。
    ///
    /// # エラー
    /// `Semantics("to before from")`、`Semantics("span over 7 days")`。
    pub fn between(from: BusinessDate, to: BusinessDate) -> Result<Self, InvalidValue> {
        let invalid = |why| InvalidValue {
            primitive: Primitive::BusinessDateRange,
            violation: Violation::Semantics(why),
        };
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

/// 取得する kline の指定。ドキュメントは `date` の形式を interval に結び付けて
/// いる。分と時間の interval は日 (`YYYYMMDD`、2021-04-15 以降)、4 時間以上は
/// 年 (`YYYY`)。variant により他の組み合わせは表現できない。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KlineSpan {
    /// 営業日 1 日分の `1min 5min 10min 15min 30min 1hour`。
    Day {
        interval: IntradayInterval,
        date: BusinessDate,
    },
    /// 1 年分の `4hour 8hour 12hour 1day 1week 1month`。
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

impl IntradayInterval {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Min1 => "1min",
            Self::Min5 => "5min",
            Self::Min10 => "10min",
            Self::Min15 => "15min",
            Self::Min30 => "30min",
            Self::Hour1 => "1hour",
        }
    }
}

impl LongInterval {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hour4 => "4hour",
            Self::Hour8 => "8hour",
            Self::Hour12 => "12hour",
            Self::Day1 => "1day",
            Self::Week1 => "1week",
            Self::Month1 => "1month",
        }
    }
}

impl KlineSpan {
    /// クエリの `(interval, date)` の値。たとえば `("1min", "20210417")`。
    pub(crate) fn query(self) -> (&'static str, String) {
        match self {
            Self::Day { interval, date } => (interval.as_str(), date.to_string()),
            Self::Year { interval, year } => (interval.as_str(), format!("{year:04}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(text: &str) -> Timestamp {
        Timestamp::parse_iso8601(text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    #[test]
    fn iso8601_round_trips_through_display() {
        for text in [
            "2019-03-19T02:15:06.001Z",
            "2018-03-30T12:34:56.789Z",
            "1970-01-01T00:00:00.000Z",
            "2024-02-29T23:59:59.000Z",
        ] {
            assert_eq!(ts(text).to_string(), text);
        }
        assert_eq!(
            ts("2019-03-19T02:15:06Z").to_string(),
            "2019-03-19T02:15:06.000Z"
        );
        assert_eq!(
            ts("2019-03-19T02:15:06.123456789Z").to_string(),
            "2019-03-19T02:15:06.123456789Z"
        );
        assert_eq!(
            ts("2019-03-19T02:15:06.5Z").unix_millis(),
            ts("2019-03-19T02:15:06.500Z").unix_millis()
        );
    }

    #[test]
    fn both_wire_forms_denote_the_same_instant() {
        let iso = ts("2021-04-16T17:00:00.000Z");
        let millis = Timestamp::parse_unix_millis("1618592400000").unwrap();
        assert_eq!(iso, millis);
        assert_eq!(millis.to_string(), "2021-04-16T17:00:00.000Z");
        assert_eq!(
            Timestamp::parse_unix_millis("0").unwrap().to_string(),
            "1970-01-01T00:00:00.000Z"
        );
    }

    #[test]
    fn system_time_round_trip() {
        let now = SystemTime::now();
        let t = Timestamp::from_system_time(now).unwrap();
        assert_eq!(t.to_system_time(), now);
        assert!(
            Timestamp::from_system_time(SystemTime::UNIX_EPOCH - Duration::from_secs(1)).is_err()
        );
    }

    #[test]
    fn iso8601_validation_order() {
        let v = |t: &str| Timestamp::parse_iso8601(t).unwrap_err().violation;
        assert!(matches!(v("2019-03-19"), Violation::Size { .. }));
        assert!(matches!(
            v("2019-03-19T02:15:06.0123456789Z"),
            Violation::Size { .. }
        ));
        assert_eq!(
            v("2019-03-19T02:15:06.001+"),
            Violation::Lexical { offset: 23 }
        );
        assert!(matches!(
            v("2019/03/19T02:15:06.001Z"),
            Violation::Lexical { .. }
        ));
        assert!(matches!(
            v("2019-03-19T02:15:06.001."),
            Violation::Syntax(_)
        ));
        assert!(matches!(
            v("2019-03-19 02:15:06.001Z"),
            Violation::Lexical { .. }
        ));
        assert!(matches!(v("2019-03-19T02:15:06.Z"), Violation::Syntax(_)));
        assert!(matches!(
            v("2019-13-19T02:15:06.001Z"),
            Violation::Semantics(_)
        ));
        assert!(matches!(
            v("2019-02-30T02:15:06.001Z"),
            Violation::Semantics(_)
        ));
        assert!(matches!(
            v("2019-03-19T24:15:06.001Z"),
            Violation::Semantics(_)
        ));
        assert!(matches!(
            v("1969-12-31T23:59:59.000Z"),
            Violation::Semantics(_)
        ));
        assert!(matches!(
            Timestamp::parse_unix_millis("9999999999999999999")
                .unwrap_err()
                .violation,
            Violation::Semantics(_)
        ));
        assert!(matches!(
            Timestamp::parse_unix_millis("12a").unwrap_err().violation,
            Violation::Lexical { .. }
        ));
    }

    #[test]
    fn ordering_and_duration() {
        let a = ts("2019-03-19T02:15:06.001Z");
        let b = ts("2019-03-19T02:45:06.001Z");
        assert!(a < b);
        assert_eq!(b.duration_since(a), Some(Duration::from_mins(30)));
        assert_eq!(a.duration_since(b), None);
    }

    #[test]
    fn history_window_rules() {
        let a = ts("2019-03-19T02:15:06.001Z");
        let b = ts("2019-03-19T02:45:06.001Z");
        let c = ts("2019-03-19T02:45:06.002Z");
        assert!(HistoryWindow::between(a, b).is_ok());
        assert_eq!(
            HistoryWindow::between(a, c).unwrap_err().violation,
            Violation::Semantics("span over 30 minutes")
        );
        assert_eq!(
            HistoryWindow::between(a, a).unwrap_err().violation,
            Violation::Semantics("to is not after from")
        );
        assert_eq!(
            HistoryWindow::between(b, a).unwrap_err().violation,
            Violation::Semantics("to is not after from")
        );
        assert_eq!(HistoryWindow::starting_at(a).to(), None);
    }

    #[test]
    fn business_dates() {
        let d = BusinessDate::parse("20210417").unwrap();
        assert_eq!(d.to_string(), "20210417");
        assert_eq!(d, BusinessDate::new(2021, 4, 17).unwrap());
        assert!(matches!(
            BusinessDate::parse("2021041").unwrap_err().violation,
            Violation::Size { .. }
        ));
        assert!(matches!(
            BusinessDate::parse("2021-4-1").unwrap_err().violation,
            Violation::Lexical { .. }
        ));
        assert_eq!(
            BusinessDate::parse("20210230").unwrap_err().violation,
            Violation::Semantics("no such day")
        );
        assert_eq!(
            BusinessDate::parse("20171231").unwrap_err().violation,
            Violation::Semantics("before 2018")
        );
        assert_eq!(
            BusinessDate::new(2024, 2, 29)
                .unwrap()
                .days_since(BusinessDate::new(2024, 2, 22).unwrap()),
            7
        );
        assert_eq!(
            BusinessDate::new(2024, 1, 1)
                .unwrap()
                .days_since(BusinessDate::new(2024, 1, 2).unwrap()),
            -1
        );
        let bd: BusinessDate = serde_json::from_str("\"20210417\"").unwrap();
        assert_eq!(bd, d);
    }

    #[test]
    fn business_date_range_rules() {
        let from = BusinessDate::new(2024, 3, 1).unwrap();
        let ok = BusinessDate::new(2024, 3, 8).unwrap();
        let too_far = BusinessDate::new(2024, 3, 9).unwrap();
        assert!(BusinessDateRange::between(from, from).is_ok());
        assert!(BusinessDateRange::between(from, ok).is_ok());
        assert_eq!(
            BusinessDateRange::between(from, too_far)
                .unwrap_err()
                .violation,
            Violation::Semantics("span over 7 days")
        );
        assert_eq!(
            BusinessDateRange::between(ok, from).unwrap_err().violation,
            Violation::Semantics("to before from")
        );
        assert_eq!(BusinessDateRange::single(from).to(), None);
    }

    #[test]
    fn kline_span_query_values() {
        let day = KlineSpan::Day {
            interval: IntradayInterval::Min1,
            date: BusinessDate::new(2021, 4, 17).unwrap(),
        };
        assert_eq!(day.query(), ("1min", "20210417".to_owned()));
        let year = KlineSpan::Year {
            interval: LongInterval::Month1,
            year: 2025,
        };
        assert_eq!(year.query(), ("1month", "2025".to_owned()));
    }
}
