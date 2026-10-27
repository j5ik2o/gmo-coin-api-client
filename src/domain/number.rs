//! 正確な十進数。
//!
//! 取引所は価格・数量・金額をすべて `"750760"` や `"-0.0001"` のような
//! JSON 文字列で送る。ここで定義する型は、値を整数の仮数と十進スケールで
//! 正確に保持する。丸めは一切起きず、比較は数値として行われ (`"1.5" == "1.50"`)、
//! 値は `Copy` であり、何千段もある板情報でも段ごとの割り当ては起きない。
//!
//! 呼び出し側が自分の十進型へ変換するときは [`Decimal::mantissa`] と
//! [`Decimal::scale`] を使う。たとえば
//! `rust_decimal::Decimal::from_i128_with_scale(d.mantissa(), d.scale().into())`。
//! `Display` は取引所が送った文字列を末尾のゼロまで含めてそのまま再現する。

use core::fmt;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, Violation, check_size_and_lexical, deserialize_text};

/// 符号付きの正確な十進数: `mantissa × 10^-scale`。
///
/// # 不変条件
/// - `scale <= Decimal::MAX_SCALE`。
/// - `|mantissa| < 10^38`。受け入れた値はすべて正確である (丸め経路は存在せず、
///   収まらない入力は切り捨てではなく拒否される)。
/// - 末尾のゼロはスケールに残るので、`Display` は入力を再現する。
///
/// 負になり得る値に使う: 手数料 (`makerFee: "-0.0001"`)、損益、受渡金額、
/// 証拠金維持率、レバレッジ。
#[derive(Clone, Copy)]
pub struct Decimal {
    mantissa: i128,
    scale: u8,
}

impl Decimal {
    /// 段階 1 の上限。38 桁と符号と小数点が余裕を持って収まる。
    pub const MAX_LEN: usize = 48;
    pub const MAX_SCALE: u8 = 30;
    /// 有効桁数の上限。`10^38 < i128::MAX` なので仮数は常に収まる。
    pub const MAX_DIGITS: u32 = 38;

    /// `-?(0|[1-9][0-9]*)(\.[0-9]+)?` を解析する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `to_string() == text`。ただし `-0` 形式は符号なしで出力される。
    ///
    /// # エラー
    /// [`InvalidValue`] の `Size` (空、または `MAX_LEN` 超)、`Lexical`
    /// (`-.0-9` 以外のバイト)、`Syntax` (符号や小数点の位置、先頭ゼロ)、
    /// `Semantics` (38 桁超、またはスケールが `MAX_SCALE` 超)。
    pub fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::Decimal, text, 1, Self::MAX_LEN, |b| {
            b.is_ascii_digit() || b == b'-' || b == b'.'
        })?;
        let invalid = |violation| InvalidValue {
            primitive: Primitive::Decimal,
            violation,
        };
        let syntax = |why| invalid(Violation::Syntax(why));

        let (negative, unsigned) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        let (integer, fraction) = match unsigned.split_once('.') {
            Some((i, f)) => (i, Some(f)),
            None => (unsigned, None),
        };
        if integer.is_empty() {
            return Err(syntax("missing integer part"));
        }
        if !integer.bytes().all(|b| b.is_ascii_digit()) {
            return Err(syntax("sign or point out of place"));
        }
        if integer.len() > 1 && integer.starts_with('0') {
            return Err(syntax("leading zero"));
        }
        if let Some(f) = fraction {
            if f.is_empty() {
                return Err(syntax("missing fraction digits"));
            }
            if !f.bytes().all(|b| b.is_ascii_digit()) {
                return Err(syntax("sign or point out of place"));
            }
        }

        let fraction = fraction.unwrap_or("");
        let scale = u8::try_from(fraction.len())
            .ok()
            .filter(|&s| s <= Self::MAX_SCALE)
            .ok_or_else(|| invalid(Violation::Semantics("scale over 30")))?;
        let significant = integer.len() + fraction.len() - usize::from(integer == "0");
        if significant > Self::MAX_DIGITS as usize {
            return Err(invalid(Violation::Semantics(
                "more than 38 significant digits",
            )));
        }
        let mut mantissa: i128 = 0;
        for b in integer.bytes().chain(fraction.bytes()) {
            mantissa = mantissa * 10 + i128::from(b - b'0');
        }
        if negative {
            mantissa = -mantissa;
        }
        Ok(Self { mantissa, scale })
    }

    pub const fn mantissa(self) -> i128 {
        self.mantissa
    }

    pub const fn scale(self) -> u8 {
        self.scale
    }

    pub const fn is_negative(self) -> bool {
        self.mantissa < 0
    }

    pub const fn is_zero(self) -> bool {
        self.mantissa == 0
    }

    /// 末尾のゼロを落とした `(mantissa, scale)`。`Eq` と `Hash` が一致する基準。
    fn normalized(self) -> (i128, u8) {
        let (mut mantissa, mut scale) = (self.mantissa, self.scale);
        while scale > 0 && mantissa % 10 == 0 {
            mantissa /= 10;
            scale -= 1;
        }
        (mantissa, scale)
    }

    /// 整数部と、`MAX_SCALE` 桁に揃えた小数部。どちらも `i128` に収まり、
    /// 仮数同士を掛け合わせる必要がない (`10^38 × 10^30` は `i128` を超える)。
    fn parts(self) -> (i128, i128) {
        let unit = 10i128.pow(u32::from(self.scale));
        let integer = self.mantissa / unit;
        let fraction = (self.mantissa % unit) * 10i128.pow(u32::from(Self::MAX_SCALE - self.scale));
        (integer, fraction)
    }
}

impl PartialEq for Decimal {
    /// 数値としての等価性: `1.5 == 1.50`。
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.parts().cmp(&other.parts())
    }
}

impl core::hash::Hash for Decimal {
    /// 末尾のゼロを落とした値をハッシュするので `Eq` と一致する。
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.normalized().hash(state);
    }
}

impl fmt::Display for Decimal {
    /// 正確な文字列: 仮数の右から `scale` 桁の位置に小数点を置いたもの。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.mantissa);
        }
        let digits = self.mantissa.unsigned_abs().to_string();
        let scale = usize::from(self.scale);
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let (integer, fraction) = padded.split_at(padded.len() - scale);
        let sign = if self.mantissa < 0 { "-" } else { "" };
        write!(f, "{sign}{integer}.{fraction}")
    }
}

impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Decimal {
    type Err = InvalidValue;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for Decimal {
    /// 常に JSON 文字列。取引所のワイヤ形式である。
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    /// JSON 文字列だけを受け付ける。JSON 数値は未知の形として拒否する
    /// (フェイルクローズド)。浮動小数点として解析すると精度が失われるため。
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, Decimal::parse)
    }
}

/// 負でないブランド付き十進数を生成する。ブランドがあるので、数量を
/// 期待する場所に価格を渡すとコンパイルが通らない。注文呼び出しで両者を
/// 取り違えるのは容易で、その誤りは実際のお金を動かす。
macro_rules! non_negative_decimal {
    ($(#[$doc:meta])* $name:ident, $primitive:expr) => {
        $(#[$doc])*
        ///
        /// # 不変条件
        /// 負でない [`Decimal`] を包む。
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Decimal);

        impl $name {
            /// 取引所の十進文字列から構築する。
            ///
            /// # 事前条件
            /// なし。
            ///
            /// # 事後条件
            /// `Ok` なら値は負でなく、`to_string() == text`。
            ///
            /// # エラー
            /// [`Decimal::parse`] が拒否するものすべて (このプリミティブ名に
            /// 付け替えられる) と、`Semantics("negative")`。
            pub fn parse(text: &str) -> Result<Self, InvalidValue> {
                let retag = |e: InvalidValue| InvalidValue { primitive: $primitive, ..e };
                Self::new(Decimal::parse(text).map_err(retag)?)
            }

            /// 解析済みの `Decimal` から構築する。
            ///
            /// # 事前条件
            /// なし。
            ///
            /// # 事後条件
            /// `Ok` なら `get() == value`。
            ///
            /// # エラー
            /// `value < 0` のとき `Semantics("negative")`。
            pub fn new(value: Decimal) -> Result<Self, InvalidValue> {
                if value.is_negative() {
                    return Err(InvalidValue {
                        primitive: $primitive,
                        violation: Violation::Semantics("negative"),
                    });
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> Decimal {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = InvalidValue;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(s)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                deserialize_text(d, $name::parse)
            }
        }
    };
}

non_negative_decimal!(
    /// 決済通貨 (JPY) 建ての価格: 注文価格、レート、最良気配。
    Price,
    Primitive::Price
);
non_negative_decimal!(
    /// 基軸資産の数量: 注文数量、約定数量、取引量。
    Size,
    Primitive::Size
);
non_negative_decimal!(
    /// 負でないお金または資産の量: 残高、振替額、入金額。符号付きの
    /// 金額 (損益、手数料) は [`Decimal`] を使う。
    Amount,
    Primitive::Amount
);

/// 注文引数向けの正 (ゼロでない) のブランド付き十進数を生成する。
/// 数量ゼロの注文や価格ゼロの指値を型の上で表現できなくする。
macro_rules! positive_decimal {
    ($(#[$doc:meta])* $name:ident, $inner:ident, $primitive:expr) => {
        $(#[$doc])*
        ///
        /// # 不変条件
        #[doc = concat!("ゼロより大きい [`", stringify!($inner), "`] を包む。")]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($inner);

        impl $name {
            /// 十進文字列から構築する。
            ///
            /// # 事前条件
            /// なし。
            ///
            /// # 事後条件
            /// `Ok` なら値はゼロより大きく、`to_string() == text`。
            ///
            /// # エラー
            #[doc = concat!("[`", stringify!($inner), "::parse`] が拒否するものすべて (このプリミティブ名に付け替えられる) と、`Semantics(\"zero\")`。")]
            pub fn parse(text: &str) -> Result<Self, InvalidValue> {
                let retag = |e: InvalidValue| InvalidValue { primitive: $primitive, ..e };
                Self::new($inner::parse(text).map_err(retag)?)
            }

            /// 負でない値から構築する。
            ///
            /// # 事前条件
            /// なし。
            ///
            /// # 事後条件
            /// `Ok` なら `get() == value`。
            ///
            /// # エラー
            /// `value` がゼロのとき `Semantics("zero")`。
            pub fn new(value: $inner) -> Result<Self, InvalidValue> {
                if value.get().is_zero() {
                    return Err(InvalidValue {
                        primitive: $primitive,
                        violation: Violation::Semantics("zero"),
                    });
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = InvalidValue;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(s)
            }
        }
    };
}

positive_decimal!(
    /// 注文に載せる価格。ゼロは表現できない。応答側の価格 (`"0"` を含む) は
    /// [`Price`] のままなので、`Deserialize` は実装しない。
    OrderPrice,
    Price,
    Primitive::OrderPrice
);
positive_decimal!(
    /// 注文に載せる数量。数量ゼロの注文は表現できない。
    OrderSize,
    Size,
    Primitive::OrderSize
);

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;

    fn d(text: &str) -> Decimal {
        Decimal::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    #[test]
    fn display_reproduces_the_input_text() {
        for text in [
            "0",
            "750760",
            "-0.0001",
            "194785.8484",
            "1.50",
            "0.000000001",
            "-12345.678900",
        ] {
            assert_eq!(d(text).to_string(), text);
        }
        assert_eq!(d("-0").to_string(), "0");
        assert_eq!(
            d("-0.00").to_string(),
            "0.00",
            "negative zero keeps its scale, drops its sign"
        );
    }

    #[test]
    fn comparison_is_numeric() {
        assert_eq!(d("1.5"), d("1.50"));
        assert!(d("1.5") < d("1.51"));
        assert!(d("-1.5") < d("-1.2"));
        assert!(d("-2.0") < d("-1.9"));
        assert!(d("-0.5") < d("0.3"));
        assert!(d("-1.5") > d("-2"));
        assert!(
            d("99999999999999999999999999999999999999") > d("0.000000000000000000000000000001")
        );
        assert_eq!(d("0"), d("-0.000"));
    }

    #[test]
    fn equal_values_hash_equally() {
        fn hash(v: Decimal) -> u64 {
            let mut h = DefaultHasher::new();
            v.hash(&mut h);
            h.finish()
        }
        assert_eq!(hash(d("1.5")), hash(d("1.500")));
        assert_eq!(hash(d("0")), hash(d("0.00")));
        assert_eq!(hash(d("100")), hash(d("100.0")));
    }

    #[test]
    fn validation_runs_size_then_lexical_then_syntax_then_semantics() {
        let violation = |t: &str| Decimal::parse(t).unwrap_err().violation;
        let too_long = "1".repeat(Decimal::MAX_LEN + 1);
        assert!(matches!(violation(&too_long), Violation::Size { .. }));
        assert!(matches!(violation(""), Violation::Size { .. }));
        assert_eq!(violation("1,5"), Violation::Lexical { offset: 1 });
        assert_eq!(violation("1e5"), Violation::Lexical { offset: 1 });
        assert!(matches!(violation("01"), Violation::Syntax(_)));
        assert!(matches!(violation("1."), Violation::Syntax(_)));
        assert!(matches!(violation(".5"), Violation::Syntax(_)));
        assert!(matches!(violation("--1"), Violation::Syntax(_)));
        assert!(matches!(violation("1-"), Violation::Syntax(_)));
        assert!(matches!(violation("1.2.3"), Violation::Syntax(_)));
        assert!(matches!(violation("-"), Violation::Syntax(_)));
        let too_many_digits = "1".repeat(39);
        assert!(matches!(
            violation(&too_many_digits),
            Violation::Semantics(_)
        ));
        let too_fine = format!("0.{}", "1".repeat(31));
        assert!(matches!(violation(&too_fine), Violation::Semantics(_)));
        assert!(Decimal::parse(&"9".repeat(38)).is_ok());
        assert!(Decimal::parse(&format!("0.{}", "1".repeat(30))).is_ok());
    }

    #[test]
    fn mantissa_and_scale_are_exact() {
        let v = d("-0.0001");
        assert_eq!((v.mantissa(), v.scale()), (-1, 4));
        let v = d("194785.8484");
        assert_eq!((v.mantissa(), v.scale()), (1_947_858_484, 4));
    }

    #[test]
    fn branded_types_reject_negative_and_keep_text() {
        assert_eq!(Price::parse("12500000").unwrap().to_string(), "12500000");
        let err = Price::parse("-1").unwrap_err();
        assert_eq!(
            (err.primitive, err.violation),
            (Primitive::Price, Violation::Semantics("negative"))
        );
        let err = Size::parse("abc").unwrap_err();
        assert_eq!(
            err.primitive,
            Primitive::Size,
            "errors are retagged with the branded type"
        );
        assert!(Amount::parse("0").is_ok());
    }

    #[test]
    fn order_arguments_cannot_be_zero() {
        assert!(OrderSize::parse("0.0001").is_ok());
        let err = OrderSize::parse("0.000").unwrap_err();
        assert_eq!(
            (err.primitive, err.violation),
            (Primitive::OrderSize, Violation::Semantics("zero"))
        );
        let err = OrderPrice::parse("-5").unwrap_err();
        assert_eq!(
            (err.primitive, err.violation),
            (Primitive::OrderPrice, Violation::Semantics("negative"))
        );
        assert_eq!(
            serde_json::to_string(&OrderPrice::parse("100").unwrap()).unwrap(),
            "\"100\""
        );
    }

    #[test]
    fn json_round_trip_is_a_string_and_a_number_is_rejected() {
        let v: Decimal = serde_json::from_str("\"-0.0001\"").unwrap();
        assert_eq!(serde_json::to_string(&v).unwrap(), "\"-0.0001\"");
        assert!(serde_json::from_str::<Decimal>("1.5").is_err());
        assert!(serde_json::from_str::<Price>("1").is_err());
    }
}
