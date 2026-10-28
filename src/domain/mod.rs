//! ドメインプリミティブ。
//!
//! ドメイン上の意味を持つ値はすべて、それ自身の不変 (immutable) な型として
//! 定義する。各型は構築時に次の固定順序で検証するため、巨大な入力は
//! 構文解析の前に弾かれる。
//!
//! 1. サイズ (バイト長を型の `MAX_LEN` と比べる)
//! 2. 字句 (許されるバイト集合)
//! 3. 構文 (構造。たとえば `-?digits(.digits)?`)
//! 4. 意味 (たとえば「負でない」「30 分以内」)
//!
//! [`InvalidValue`] は、どのプリミティブがどの段階で失敗したかだけを報告する。
//! 秘密情報も同じ経路を通るため、拒否した入力そのものは決して保持しない。
//!
//! `Deserialize` も同じコンストラクタを呼ぶので、取引所から受け取った値は
//! 呼び出し側が組み立てた値と同じ検査を通過している。クレート内部では
//! これらの型を信頼し、再検証しない。

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
pub use number::{Amount, Decimal, OrderPrice, OrderSize, Price, Size};
pub use symbol::{Market, Symbol};
pub use time::{
    BusinessDate, BusinessDateRange, HistoryWindow, IntradayInterval, KlineSpan, LongInterval,
    Timestamp,
};

/// 値を拒否したドメインプリミティブ。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Primitive {
    Decimal,
    Price,
    Size,
    Amount,
    OrderPrice,
    OrderSize,
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
    /// `exchangeFee/history` の継続トークン。取引所から受け取った文字列を
    /// 呼び出し側が渡し直すもので、長さだけを検査する。
    Cursor,
}

/// 失敗した検証段階。検査が走る順に並んでいる。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Violation {
    /// 段階 1。`max` バイトより長いか、`min` バイトより短い。
    Size {
        min: usize,
        max: usize,
        actual: usize,
    },
    /// 段階 2。許されない バイトがこのバイトオフセットにある。
    Lexical { offset: usize },
    /// 段階 3。バイトは許されるが構造が違う。
    Syntax(&'static str),
    /// 段階 4。形は正しいがドメインの外にある (たとえば負の値)。
    Semantics(&'static str),
}

/// ドメインプリミティブの検証に失敗した。
///
/// # 不変条件
/// 拒否した入力を決して保持しない (秘密情報も同じ経路を通る)。保持するのは
/// プリミティブ名と失敗した段階だけである。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidValue {
    pub primitive: Primitive,
    pub violation: Violation,
}

impl core::fmt::Display for InvalidValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?} の値が不正です: ", self.primitive)?;
        match self.violation {
            Violation::Size { min, max, actual } => {
                write!(f, "長さ {actual} バイトは {min}..={max} の範囲外です")
            }
            Violation::Lexical { offset } => {
                write!(f, "オフセット {offset} に許されない文字があります")
            }
            Violation::Syntax(why) => write!(f, "構文が不正です ({why})"),
            Violation::Semantics(why) => write!(f, "意味上の制約に反しています ({why})"),
        }
    }
}

impl std::error::Error for InvalidValue {}

/// 共通の最初の 2 段階。テキストを受け取る各プリミティブは、自身の構文・
/// 意味検査の前に必ずこれを呼ぶ。順序が型ごとにずれることはない。
///
/// # 事後条件
/// `Ok` なら `input.len()` は `min..=max` にあり、すべてのバイトが `allowed` を満たす。
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

/// テキスト系プリミティブに共通の `Deserialize` 経路。JSON 文字列 (借用でも
/// エスケープ済みでも) を受け取り、そのプリミティブ自身のコンストラクタを
/// 走らせる。他の JSON 型は強制変換せず拒否する (フェイルクローズド)。
/// serde のエラーにはプリミティブ名と失敗段階だけが入り、値は入らない。
/// `data` を小文字の 16 進数にして `out` の先頭から書く。`out` が `2 * data.len()`
/// バイトより短ければ入る分だけ書き、長ければ残りは触らない。
pub(crate) fn write_hex(out: &mut [u8], data: &[u8]) {
    fn digit(nibble: u8) -> u8 {
        if nibble < 10 {
            b'0' + nibble
        } else {
            b'a' + (nibble - 10)
        }
    }
    let (pairs, _) = out.as_chunks_mut::<2>();
    for (pair, byte) in pairs.iter_mut().zip(data) {
        *pair = [digit(byte >> 4), digit(byte & 0x0f)];
    }
}

pub(crate) fn deserialize_text<'de, D, T>(
    d: D,
    parse: fn(&str) -> Result<T, InvalidValue>,
) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Text<T>(fn(&str) -> Result<T, InvalidValue>);
    impl<T> serde::de::Visitor<'_> for Text<T> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_checked_before_lexical_content() {
        let err = check_size_and_lexical(Primitive::Symbol, "", 1, 3, |b| b.is_ascii_uppercase())
            .unwrap_err();
        assert_eq!(
            err.violation,
            Violation::Size {
                min: 1,
                max: 3,
                actual: 0
            }
        );

        let err =
            check_size_and_lexical(Primitive::Symbol, "abcd", 1, 3, |b| b.is_ascii_uppercase())
                .unwrap_err();
        assert_eq!(
            err.violation,
            Violation::Size {
                min: 1,
                max: 3,
                actual: 4
            },
            "too long wins over bad bytes"
        );

        let err =
            check_size_and_lexical(Primitive::Symbol, "ABc", 1, 3, |b| b.is_ascii_uppercase())
                .unwrap_err();
        assert_eq!(err.violation, Violation::Lexical { offset: 2 });
        assert_eq!(err.primitive, Primitive::Symbol);
    }

    #[test]
    #[expect(
        clippy::assert_is_empty,
        reason = "空かどうかを見る意図を is_empty で直接書く。assert_eq! に直すと空配列の型注釈が要り、読みにくくなる"
    )]
    fn a_json_number_is_not_coerced_into_a_text_primitive() {
        let err = serde_json::from_str::<Symbol>("123").unwrap_err();
        assert!(!err.to_string().is_empty());
        assert!(serde_json::from_str::<Symbol>("\"BTC\"").is_ok());
    }

    #[test]
    fn display_is_japanese_and_names_no_input() {
        let err = InvalidValue {
            primitive: Primitive::ApiSecret,
            violation: Violation::Lexical { offset: 3 },
        };
        let text = err.to_string();
        assert!(text.contains("ApiSecret"));
        assert!(text.contains("オフセット 3"));
    }
}
