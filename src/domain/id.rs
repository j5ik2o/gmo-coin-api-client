//! 識別子。

use core::fmt;
use core::num::NonZeroU64;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, Violation, check_size_and_lexical, deserialize_text};

/// 正の数値 id を生成する。取引所はこれらを JSON 数値で送るが、
/// `POST /order`、`/closeOrder`、`/closeBulkOrder` だけは新しい注文 id を
/// 文字列で返す (`"data": "637000"`)。どちらの形もデコードできる。
macro_rules! numeric_id {
    ($(#[$doc:meta])* $name:ident, $primitive:expr) => {
        $(#[$doc])*
        ///
        /// # 不変条件
        /// 正 (ゼロでない) で `u64` に収まる。
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(NonZeroU64);

        impl $name {
            /// JSON 数値の値から構築する。
            ///
            /// # 事前条件
            /// なし。
            ///
            /// # 事後条件
            /// `Ok` なら `get() == value`。
            ///
            /// # エラー
            /// `Semantics("zero")`。
            pub fn new(value: u64) -> Result<Self, InvalidValue> {
                NonZeroU64::new(value).map(Self).ok_or(InvalidValue {
                    primitive: $primitive,
                    violation: Violation::Semantics("zero"),
                })
            }

            /// 十進数字の文字列 (`POST /order` が返す形) から構築する。
            ///
            /// # 事前条件
            /// なし。
            ///
            /// # 事後条件
            /// `Ok` なら `get()` は `text` の十進値。
            ///
            /// # エラー
            /// `Size` (20 バイト超)、`Lexical` (数字以外)、`Semantics`
            /// (ゼロ、または `u64::MAX` 超)。
            pub fn parse(text: &str) -> Result<Self, InvalidValue> {
                check_size_and_lexical($primitive, text, 1, 20, |b| b.is_ascii_digit())?;
                let value: u64 = text.parse().map_err(|_| InvalidValue {
                    primitive: $primitive,
                    violation: Violation::Semantics("over u64::MAX"),
                })?;
                Self::new(value)
            }

            pub const fn get(self) -> u64 {
                self.0.get()
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
            /// JSON 数値。ドキュメントのすべてのリクエストボディがこの形を使う。
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_u64(self.get())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            /// JSON の非負整数、または十進数字の文字列を受け付ける。
            /// 浮動小数点と負の数は拒否する。
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct IdVisitor;
                impl<'de> serde::de::Visitor<'de> for IdVisitor {
                    type Value = $name;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("a positive integer or a decimal-digit string")
                    }
                    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<$name, E> {
                        $name::new(v).map_err(E::custom)
                    }
                    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<$name, E> {
                        $name::parse(v).map_err(E::custom)
                    }
                }
                d.deserialize_any(IdVisitor)
            }
        }
    };
}

numeric_id!(
    /// `orderId` と `rootOrderId`。
    OrderId,
    Primitive::OrderId
);
numeric_id!(
    /// `executionId`。
    ExecutionId,
    Primitive::ExecutionId
);
numeric_id!(
    /// `positionId`。
    PositionId,
    Primitive::PositionId
);

/// 呼び出し側自身の注文 id (`clientOrderId`)。
///
/// このクレートの注文リクエストはすべてこれを持つ。取引所では任意だが、
/// ここでは必須である。取引所はアカウント内で一意性を保ち、再利用には
/// ERR-915 で答え、その後 `GET /orders?clientOrderId=` で元の結果が
/// 取れる。したがって HTTP の結果が不明な注文 (リクエストが出た後に接続が
/// 落ちた) は同じ id で安全に再送できる。一度だけ発注されるか、発注済みと
/// 報告されるかのどちらかになる。`Error::outcome_unknown` を参照。
///
/// # 不変条件
/// 1 から 36 バイトの `A-Z a-z 0-9`。大文字小文字を区別する。
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ClientOrderId {
    len: u8,
    bytes: [u8; ClientOrderId::MAX_LEN],
}

impl ClientOrderId {
    pub const MAX_LEN: usize = 36;

    /// 呼び出し側自身の id から構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `as_str() == text`。
    ///
    /// # エラー
    /// `Size` (空、または 36 バイト超) または `Lexical` (英数字以外)。
    pub fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::ClientOrderId, text, 1, Self::MAX_LEN, |b| {
            b.is_ascii_alphanumeric()
        })?;
        let mut bytes = [0u8; Self::MAX_LEN];
        bytes[..text.len()].copy_from_slice(text.as_bytes());
        Ok(Self {
            len: text.len() as u8,
            bytes,
        })
    }

    /// OS の乱数源 (TLS のために既にツリーにある ring 経由) から得た
    /// 32 文字の小文字 16 進数。ドキュメントが UUID に推奨する形である。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// OS の乱数源から得た 32 文字の小文字 16 進数。
    ///
    /// # エラー
    /// なし。`# パニック` を参照。
    ///
    /// # パニック
    /// OS の乱数源が失敗したとき。`std` の `RandomState` と同じ扱いである。
    pub fn generate() -> Self {
        use ring::rand::SecureRandom;
        let mut random = [0u8; 16];
        ring::rand::SystemRandom::new()
            .fill(&mut random)
            .expect("the OS random source is available");
        let mut bytes = [0u8; Self::MAX_LEN];
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (i, b) in random.iter().enumerate() {
            bytes[2 * i] = HEX[usize::from(b >> 4)];
            bytes[2 * i + 1] = HEX[usize::from(b & 0x0f)];
        }
        Self { len: 32, bytes }
    }

    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or_default()
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientOrderId({})", self.as_str())
    }
}

impl FromStr for ClientOrderId {
    type Err = InvalidValue;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ClientOrderId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ClientOrderId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, ClientOrderId::parse)
    }
}

/// 最大 `MAX` 件の相異なる要素を持つ、空でないリスト。
///
/// ドキュメントに書かれた件数上限を型に載せるので、検査はリクエストが
/// 存在する前、呼び出し側がリストを組み立てる時点で起きる。
/// `cancelOrders` は `Batch<OrderId, 10>`、`GET /orders` は
/// `Batch<OrderId, 10>` または `Batch<ClientOrderId, 10>`、
/// `GET /executions` は `Batch<ExecutionId, 10>` を取る。
///
/// # 不変条件
/// `1 <= len <= MAX` で、等しい要素が 2 つ以上ない。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch<T, const MAX: usize> {
    items: Vec<T>,
}

impl<T: Eq, const MAX: usize> Batch<T, MAX> {
    /// 最大 `MAX` 件の相異なる要素から構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `as_slice()` は `items` と同じ順序である。
    ///
    /// # エラー
    /// 空または過大なリストに `Size { min: 1, max: MAX, .. }` (段階 1。重複の
    /// 走査より先に検査する)、重複に `Semantics("duplicate")`。
    pub fn new(items: Vec<T>) -> Result<Self, InvalidValue> {
        const { assert!(MAX >= 1, "Batch<T, 0> can hold nothing") };
        let actual = items.len();
        if actual == 0 || actual > MAX {
            return Err(InvalidValue {
                primitive: Primitive::Batch,
                violation: Violation::Size {
                    min: 1,
                    max: MAX,
                    actual,
                },
            });
        }
        // MAX はどの用途でも高々 10 なので、二次の走査のほうがハッシュより安い。
        for (i, a) in items.iter().enumerate() {
            if items[i + 1..].contains(a) {
                return Err(InvalidValue {
                    primitive: Primitive::Batch,
                    violation: Violation::Semantics("duplicate"),
                });
            }
        }
        Ok(Self { items })
    }

    /// 要素 1 件のバッチ。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// ちょうど `item` だけを持つバッチ。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn one(item: T) -> Self {
        const { assert!(MAX >= 1, "Batch<T, 0> can hold nothing") };
        Self { items: vec![item] }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.items
    }

    pub fn contains(&self, item: &T) -> bool {
        self.items.contains(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_decode_from_a_number_and_from_a_digit_string() {
        assert_eq!(
            serde_json::from_str::<OrderId>("637000").unwrap().get(),
            637000
        );
        assert_eq!(
            serde_json::from_str::<OrderId>("\"637000\"").unwrap().get(),
            637000
        );
        assert!(serde_json::from_str::<OrderId>("0").is_err());
        assert!(serde_json::from_str::<OrderId>("-5").is_err());
        assert!(serde_json::from_str::<OrderId>("1.5").is_err());
        assert!(serde_json::from_str::<OrderId>("\"12a\"").is_err());
        assert_eq!(
            serde_json::to_string(&ExecutionId::new(72123911).unwrap()).unwrap(),
            "72123911"
        );
    }

    #[test]
    fn id_text_validation_order() {
        let v = |t: &str| OrderId::parse(t).unwrap_err().violation;
        assert!(matches!(v(&"1".repeat(21)), Violation::Size { .. }));
        assert_eq!(v("12x"), Violation::Lexical { offset: 2 });
        assert_eq!(v("0"), Violation::Semantics("zero"));
        assert_eq!(
            v("99999999999999999999"),
            Violation::Semantics("over u64::MAX")
        );
        assert_eq!(
            PositionId::parse("18446744073709551615").unwrap().get(),
            u64::MAX
        );
    }

    #[test]
    fn generated_client_order_ids_are_hex_and_distinct() {
        let a = ClientOrderId::generate();
        let b = ClientOrderId::generate();
        assert_eq!(a.as_str().len(), 32);
        assert!(
            a.as_str()
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        );
        assert_ne!(a, b);
        assert_eq!(
            ClientOrderId::parse(a.as_str()).unwrap(),
            a,
            "a generated id passes its own validation"
        );
    }

    #[test]
    fn client_order_id_rules() {
        assert_eq!(
            ClientOrderId::parse("abc123").unwrap().to_string(),
            "abc123"
        );
        assert!(matches!(
            ClientOrderId::parse("").unwrap_err().violation,
            Violation::Size { .. }
        ));
        assert!(matches!(
            ClientOrderId::parse(&"a".repeat(37)).unwrap_err().violation,
            Violation::Size { .. }
        ));
        assert_eq!(
            ClientOrderId::parse("abc-1").unwrap_err().violation,
            Violation::Lexical { offset: 3 }
        );
        assert_eq!(
            serde_json::to_string(&ClientOrderId::parse("x1").unwrap()).unwrap(),
            "\"x1\""
        );
    }

    #[test]
    fn batch_size_is_checked_before_duplicates() {
        let ids = |ns: &[u64]| {
            ns.iter()
                .map(|&n| OrderId::new(n).unwrap())
                .collect::<Vec<_>>()
        };
        let err = Batch::<OrderId, 10>::new(ids(&[1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])).unwrap_err();
        assert_eq!(
            err.violation,
            Violation::Size {
                min: 1,
                max: 10,
                actual: 11
            }
        );
        let err = Batch::<OrderId, 10>::new(ids(&[1, 2, 1])).unwrap_err();
        assert_eq!(err.violation, Violation::Semantics("duplicate"));
        let err = Batch::<OrderId, 10>::new(vec![]).unwrap_err();
        assert_eq!(
            err.violation,
            Violation::Size {
                min: 1,
                max: 10,
                actual: 0
            }
        );
        let ok = Batch::<OrderId, 10>::new(ids(&[3, 1, 2])).unwrap();
        assert_eq!(ok.as_slice(), ids(&[3, 1, 2]).as_slice());
        assert!(ok.contains(&OrderId::new(1).unwrap()));
        assert_eq!(
            Batch::<OrderId, 1>::one(OrderId::new(7).unwrap())
                .as_slice()
                .len(),
            1
        );
    }
}
