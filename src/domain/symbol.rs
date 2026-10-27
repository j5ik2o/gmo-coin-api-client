//! 取引所の銘柄。

use core::fmt;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, check_size_and_lexical, deserialize_text};

/// 取引所の綴りそのままの銘柄: `BTC` (現物)、`BTC_JPY` (レバレッジ)、
/// `JPY` や `ZPG` (資産残高)、`BTC/JPY` (取引高上限)。
///
/// 集合は開いている。取引所は時間とともに銘柄を追加する (2026-01-17 に SUI、
/// 2026-03-21 に WILD) ので、形式が正しい銘柄はすべてデシリアライズできる。
/// 関連定数は利便性のためのものであり、ホワイトリストではない。
///
/// インラインに格納される (`Copy`、割り当てなし)。すべてのティッカー、注文、
/// WebSocket イベントが 1 つ持つ。
///
/// # 不変条件
/// - 1 から [`Symbol::MAX_LEN`] バイト。
/// - `A-Z`、`0-9`、`_`、`/` のみ。これにより、銘柄はエスケープなしで
///   クエリ文字列や JSON ボディに置ける。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Symbol {
    len: u8,
    bytes: [u8; Symbol::MAX_LEN],
}

/// 銘柄が属する市場。綴りから導かれる。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Market {
    /// 区切りなし: `BTC`、`ETH`。現物取引と資産残高。
    Spot,
    /// `_` または `/` を含む: `BTC_JPY`。レバレッジ取引。
    Leverage,
}

impl Symbol {
    pub const MAX_LEN: usize = 31;

    pub const BTC: Symbol = Symbol::from_static("BTC");
    pub const ETH: Symbol = Symbol::from_static("ETH");
    pub const BCH: Symbol = Symbol::from_static("BCH");
    pub const LTC: Symbol = Symbol::from_static("LTC");
    pub const XRP: Symbol = Symbol::from_static("XRP");
    pub const XLM: Symbol = Symbol::from_static("XLM");
    pub const DOT: Symbol = Symbol::from_static("DOT");
    pub const ATOM: Symbol = Symbol::from_static("ATOM");
    pub const FCR: Symbol = Symbol::from_static("FCR");
    pub const ADA: Symbol = Symbol::from_static("ADA");
    pub const LINK: Symbol = Symbol::from_static("LINK");
    pub const DOGE: Symbol = Symbol::from_static("DOGE");
    pub const SOL: Symbol = Symbol::from_static("SOL");
    pub const ASTR: Symbol = Symbol::from_static("ASTR");
    pub const NAC: Symbol = Symbol::from_static("NAC");
    pub const SUI: Symbol = Symbol::from_static("SUI");
    pub const WILD: Symbol = Symbol::from_static("WILD");
    pub const JPY: Symbol = Symbol::from_static("JPY");
    pub const BTC_JPY: Symbol = Symbol::from_static("BTC_JPY");
    pub const ETH_JPY: Symbol = Symbol::from_static("ETH_JPY");
    pub const BCH_JPY: Symbol = Symbol::from_static("BCH_JPY");
    pub const LTC_JPY: Symbol = Symbol::from_static("LTC_JPY");
    pub const XRP_JPY: Symbol = Symbol::from_static("XRP_JPY");
    pub const DOT_JPY: Symbol = Symbol::from_static("DOT_JPY");
    pub const ATOM_JPY: Symbol = Symbol::from_static("ATOM_JPY");
    pub const ADA_JPY: Symbol = Symbol::from_static("ADA_JPY");
    pub const LINK_JPY: Symbol = Symbol::from_static("LINK_JPY");
    pub const DOGE_JPY: Symbol = Symbol::from_static("DOGE_JPY");
    pub const SOL_JPY: Symbol = Symbol::from_static("SOL_JPY");
    pub const SUI_JPY: Symbol = Symbol::from_static("SUI_JPY");

    /// 定数用のコンパイル時コンストラクタ。不正なリテラルは実行時検査ではなく
    /// ビルドを失敗させる。
    const fn from_static(text: &str) -> Symbol {
        let src = text.as_bytes();
        assert!(
            !src.is_empty() && src.len() <= Self::MAX_LEN,
            "symbol length"
        );
        let mut bytes = [0u8; Self::MAX_LEN];
        let mut i = 0;
        while i < src.len() {
            let b = src[i];
            assert!(Self::allowed(b), "symbol byte");
            bytes[i] = b;
            i += 1;
        }
        Symbol {
            len: src.len() as u8,
            bytes,
        }
    }

    const fn allowed(b: u8) -> bool {
        b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_' || b == b'/'
    }

    /// 取引所の綴りから構築する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `as_str() == text`。
    ///
    /// # エラー
    /// `Size` (空、または [`Symbol::MAX_LEN`] 超) または `Lexical`。
    pub fn parse(text: &str) -> Result<Symbol, InvalidValue> {
        check_size_and_lexical(Primitive::Symbol, text, 1, Self::MAX_LEN, Self::allowed)?;
        let mut bytes = [0u8; Self::MAX_LEN];
        bytes[..text.len()].copy_from_slice(text.as_bytes());
        Ok(Symbol {
            len: text.len() as u8,
            bytes,
        })
    }

    pub fn as_str(&self) -> &str {
        debug_assert!(
            self.bytes[..self.len as usize].is_ascii(),
            "Symbol::parse admits ASCII only"
        );
        core::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or_default()
    }

    /// 綴りから導かれ、決して保存されないので、銘柄と食い違うことがない。
    /// 注文の事前条件で使う (`losscutPrice` はレバレッジ銘柄が必要、
    /// `/order` の `cancelBefore` は現物銘柄が必要)。
    pub fn market(&self) -> Market {
        if self.as_str().contains(['_', '/']) {
            Market::Leverage
        } else {
            Market::Spot
        }
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol({})", self.as_str())
    }
}

impl FromStr for Symbol {
    type Err = InvalidValue;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for Symbol {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, Symbol::parse)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Violation;

    #[test]
    fn constants_and_parsed_values_agree() {
        assert_eq!(Symbol::parse("BTC_JPY").unwrap(), Symbol::BTC_JPY);
        assert_eq!(Symbol::BTC.as_str(), "BTC");
        assert_eq!(Symbol::SUI_JPY.to_string(), "SUI_JPY");
    }

    #[test]
    fn unknown_symbols_decode_when_well_formed() {
        let s: Symbol = serde_json::from_str("\"NEWCOIN2030\"").unwrap();
        assert_eq!(s.as_str(), "NEWCOIN2030");
        assert_eq!(Symbol::parse("BTC/JPY").unwrap().market(), Market::Leverage);
    }

    #[test]
    fn validation_order_is_size_then_lexical() {
        let long = "A".repeat(Symbol::MAX_LEN + 1);
        assert!(matches!(
            Symbol::parse(&long).unwrap_err().violation,
            Violation::Size { .. }
        ));
        assert_eq!(
            Symbol::parse("btc").unwrap_err().violation,
            Violation::Lexical { offset: 0 }
        );
        assert_eq!(
            Symbol::parse("BTC JPY").unwrap_err().violation,
            Violation::Lexical { offset: 3 }
        );
        assert!(matches!(
            Symbol::parse("").unwrap_err().violation,
            Violation::Size { .. }
        ));
    }

    #[test]
    fn market_is_derived_from_spelling() {
        assert_eq!(Symbol::BTC.market(), Market::Spot);
        assert_eq!(Symbol::JPY.market(), Market::Spot);
        assert_eq!(Symbol::BTC_JPY.market(), Market::Leverage);
    }
}
