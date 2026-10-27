//! Exchange symbols.

use core::fmt;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, check_size_and_lexical, deserialize_text};

/// A symbol as the exchange spells it: `BTC` (spot), `BTC_JPY` (leverage),
/// `JPY` or `ZPG` (asset balances), `BTC/JPY` (trading-volume limits).
///
/// The set is open. The exchange adds symbols over time (SUI on 2026-01-17,
/// WILD on 2026-03-21), so any well-formed symbol deserializes. The
/// associated constants are a convenience, not a whitelist.
///
/// Stored inline (`Copy`, no allocation): every ticker, order, and WebSocket
/// event carries one.
///
/// # Invariants
/// - 1 to [`Symbol::MAX_LEN`] bytes.
/// - Only `A-Z`, `0-9`, `_`, `/`. This is also what makes a symbol safe to
///   place in a query string or JSON body without escaping.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Symbol {
    len: u8,
    bytes: [u8; Symbol::MAX_LEN],
}

/// Which market a symbol trades in, derived from its spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Market {
    /// No separator: `BTC`, `ETH`. Spot trading and asset balances.
    Spot,
    /// Contains `_` or `/`: `BTC_JPY`. Leverage trading.
    Leverage,
}

impl Symbol {
    pub const MAX_LEN: usize = 31;

    pub const BTC: Symbol = Symbol::from_static("BTC");
    pub const ETH: Symbol = Symbol::from_static("ETH");
    pub const XRP: Symbol = Symbol::from_static("XRP");
    pub const JPY: Symbol = Symbol::from_static("JPY");
    pub const BTC_JPY: Symbol = Symbol::from_static("BTC_JPY");
    pub const ETH_JPY: Symbol = Symbol::from_static("ETH_JPY");
    pub const XRP_JPY: Symbol = Symbol::from_static("XRP_JPY");
    // TODO the remaining documented symbols (BCH, LTC, XLM, DOT, ATOM, FCR,
    // ADA, LINK, DOGE, SOL, ASTR, NAC, SUI, WILD and their _JPY pairs).

    /// Compile-time constructor for the constants. An invalid literal fails
    /// the build, not a runtime check.
    const fn from_static(text: &str) -> Symbol {
        let src = text.as_bytes();
        assert!(!src.is_empty() && src.len() <= Self::MAX_LEN, "symbol length");
        let mut bytes = [0u8; Self::MAX_LEN];
        let mut i = 0;
        while i < src.len() {
            let b = src[i];
            assert!(Self::allowed(b), "symbol byte");
            bytes[i] = b;
            i += 1;
        }
        Symbol { len: src.len() as u8, bytes }
    }

    const fn allowed(b: u8) -> bool {
        b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_' || b == b'/'
    }

    /// From the exchange's spelling.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `as_str() == text`.
    /// # Errors
    /// `Size` (empty or over [`Symbol::MAX_LEN`]) or `Lexical`.
    pub fn parse(text: &str) -> Result<Symbol, InvalidValue> {
        check_size_and_lexical(Primitive::Symbol, text, 1, Self::MAX_LEN, Self::allowed)?;
        let mut bytes = [0u8; Self::MAX_LEN];
        bytes[..text.len()].copy_from_slice(text.as_bytes());
        Ok(Symbol { len: text.len() as u8, bytes })
    }

    pub fn as_str(&self) -> &str {
        debug_assert!(self.bytes[..self.len as usize].is_ascii(), "Symbol::parse admits ASCII only");
        core::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or_default()
    }

    /// Derived from the spelling, never stored, so it cannot disagree with
    /// the symbol. Used by the order preconditions (`losscutPrice` needs a
    /// leverage symbol; `cancelBefore` on `/order` needs a spot symbol).
    pub fn market(&self) -> Market {
        if self.as_str().contains(['_', '/']) { Market::Leverage } else { Market::Spot }
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
