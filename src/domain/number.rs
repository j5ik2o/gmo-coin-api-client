//! Exact decimal numbers.
//!
//! The exchange sends every price, size, and amount as a JSON string such as
//! `"750760"` or `"-0.0001"`. These types keep the exact value as an integer
//! mantissa and a decimal scale, so nothing is rounded, comparisons are
//! numeric (`"1.5" == "1.50"`), values are `Copy`, and an order book with
//! thousands of levels allocates nothing per level.
//!
//! Conversion to the caller's own decimal type goes through
//! [`Decimal::mantissa`] and [`Decimal::scale`], for example
//! `rust_decimal::Decimal::from_i128_with_scale(d.mantissa(), d.scale().into())`.
//! `Display` prints the exact text the exchange sent, trailing zeros included.

use core::fmt;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, Violation, check_size_and_lexical, deserialize_text};

/// A signed exact decimal: `mantissa × 10^-scale`.
///
/// # Invariants
/// - `scale <= Decimal::MAX_SCALE`.
/// - `|mantissa| < 10^38`, so every value accepted is exact (no rounding path
///   exists; an input that does not fit is rejected, never truncated).
/// - Trailing zeros are kept in the scale so `Display` reproduces the input.
///
/// Used for values that may be negative: fees (`makerFee: "-0.0001"`), profit
/// and loss, delivery amounts, margin ratios, leverage.
#[derive(Clone, Copy)]
pub struct Decimal {
    mantissa: i128,
    scale: u8,
}

impl Decimal {
    /// Step 1 bound. 38 digits, a sign, and a point fit comfortably.
    pub const MAX_LEN: usize = 48;
    pub const MAX_SCALE: u8 = 30;

    /// Parses `-?(0|[1-9][0-9]*)(\.[0-9]+)?`.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `to_string() == text`, except that `-0` forms print without the sign.
    ///
    /// # Errors
    /// [`InvalidValue`] with `Size` (empty or over `MAX_LEN`), `Lexical`
    /// (a byte outside `-.0-9`), `Syntax` (misplaced sign or point, leading
    /// zero), or `Semantics` (more than 38 digits or scale over `MAX_SCALE`).
    pub fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::Decimal, text, 1, Self::MAX_LEN, |b| {
            b.is_ascii_digit() || b == b'-' || b == b'.'
        })?;
        // TODO syntax: optional leading '-', then integer part (no leading
        // zero unless it is exactly "0"), then optional '.' + at least one digit.
        // TODO semantics: accumulate digits into i128 with checked_mul/checked_add;
        // overflow => Violation::Semantics("more than 38 significant digits").
        // scale = number of fraction digits; > MAX_SCALE => Semantics.
        // "-0" and "-0.00" normalize to a non-negative zero with the same scale.
        todo!()
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
}

impl PartialEq for Decimal {
    /// Numeric equality: `1.5 == 1.50`.
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
    fn cmp(&self, _other: &Self) -> core::cmp::Ordering {
        // TODO compare signs first; then strip trailing zeros from both
        // mantissas (reducing scale) and compare integer parts via
        // mantissa / 10^scale, then the remainders aligned to the larger scale.
        // Never multiply mantissas up: 10^38 * 10^30 overflows i128.
        todo!()
    }
}

impl core::hash::Hash for Decimal {
    /// Hashes the value with trailing zeros stripped, so it agrees with `Eq`.
    fn hash<H: core::hash::Hasher>(&self, _state: &mut H) {
        todo!()
    }
}

impl fmt::Display for Decimal {
    /// Exact text: the mantissa with the point `scale` digits from the right.
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
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
    /// Always a JSON string, the exchange's wire form.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    /// Accepts only a JSON string. A JSON number is an unknown shape and is
    /// rejected (fail closed), because float parsing would lose precision.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_text(d, Decimal::parse)
    }
}

/// Generates a non-negative branded decimal. Branding stops a price from
/// being passed where a size is expected; the two are swapped easily in an
/// order call and the mistake moves real money.
macro_rules! non_negative_decimal {
    ($(#[$doc:meta])* $name:ident, $primitive:expr) => {
        $(#[$doc])*
        ///
        /// # Invariants
        /// Wraps a [`Decimal`] that is not negative.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Decimal);

        impl $name {
            /// From the exchange's decimal text.
            ///
            /// # Preconditions
            /// None.
            ///
            /// # Postconditions
            /// On `Ok`, the value is not negative and `to_string() == text`.
            /// # Errors
            /// Everything [`Decimal::parse`] rejects, re-tagged with this
            /// primitive, plus `Semantics("negative")`.
            pub fn parse(text: &str) -> Result<Self, InvalidValue> {
                let retag = |e: InvalidValue| InvalidValue { primitive: $primitive, ..e };
                Self::new(Decimal::parse(text).map_err(retag)?)
            }

            /// From an already-parsed `Decimal`.
            ///
            /// # Preconditions
            /// None.
            ///
            /// # Postconditions
            /// On `Ok`, `get() == value`.
            /// # Errors
            /// `Semantics("negative")` when `value < 0`.
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
    /// A price in the quote currency (JPY): order prices, rates, best bid and ask.
    Price,
    Primitive::Price
);
non_negative_decimal!(
    /// A quantity of the base asset: order size, executed size, volume.
    Size,
    Primitive::Size
);
non_negative_decimal!(
    /// A non-negative quantity of money or an asset: balances, transfer and
    /// deposit amounts. Signed money values (profit and loss, fees) use
    /// [`Decimal`].
    Amount,
    Primitive::Amount
);
