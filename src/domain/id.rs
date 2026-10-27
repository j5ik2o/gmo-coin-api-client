//! Identifiers.

use core::fmt;
use core::num::NonZeroU64;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, Violation, check_size_and_lexical, deserialize_text};

/// Generates a positive numeric id. The exchange sends these as JSON numbers,
/// except `POST /order`, `/closeOrder`, and `/closeBulkOrder`, which return
/// the new order id as a string (`"data": "637000"`). Both forms decode.
macro_rules! numeric_id {
    ($(#[$doc:meta])* $name:ident, $primitive:expr) => {
        $(#[$doc])*
        ///
        /// # Invariants
        /// Positive (non-zero) and fits in `u64`.
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(NonZeroU64);

        impl $name {
            /// From a JSON-number value.
            ///
            /// # Preconditions
            /// None.
            ///
            /// # Postconditions
            /// On `Ok`, `get() == value`.
            ///
            /// # Errors
            /// `Semantics("zero")`.
            pub fn new(value: u64) -> Result<Self, InvalidValue> {
                NonZeroU64::new(value).map(Self).ok_or(InvalidValue {
                    primitive: $primitive,
                    violation: Violation::Semantics("zero"),
                })
            }

            /// From a decimal-digit string (the form `POST /order` answers with).
            ///
            /// # Preconditions
            /// None.
            ///
            /// # Postconditions
            /// On `Ok`, `get()` is the decimal value of `text`.
            /// # Errors
            /// `Size` (over 20 bytes), `Lexical` (non-digit), `Semantics`
            /// (zero or over `u64::MAX`).
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
            /// JSON number, as every request body in the docs uses.
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_u64(self.get())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            /// Accepts a JSON unsigned integer or a decimal-digit string.
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                // TODO visitor with visit_u64 => Self::new, visit_str => Self::parse,
                // and every other JSON type rejected (no float, no negative).
                let _ = d;
                todo!()
            }
        }
    };
}

numeric_id!(
    /// `orderId` and `rootOrderId`.
    OrderId,
    Primitive::OrderId
);
numeric_id!(
    /// `executionId`.
    ExecutionId,
    Primitive::ExecutionId
);
numeric_id!(
    /// `positionId`.
    PositionId,
    Primitive::PositionId
);

/// The caller's own order id (`clientOrderId`).
///
/// Every order request in this crate carries one; it is not optional here
/// even though the exchange treats it as optional. The exchange keeps it
/// unique per account and answers a reuse with ERR-915, after which
/// `GET /orders?clientOrderId=` returns the original result. That makes an
/// order whose HTTP outcome is unknown (connection dropped after the request
/// left) safe to resend with the same id: it is either placed once or
/// reported as already placed. See `Error::outcome_unknown`.
///
/// # Invariants
/// 1 to 36 bytes of `A-Z a-z 0-9`. Case-sensitive.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ClientOrderId {
    len: u8,
    bytes: [u8; ClientOrderId::MAX_LEN],
}

impl ClientOrderId {
    pub const MAX_LEN: usize = 36;

    /// From the caller's own id.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `as_str() == text`.
    /// # Errors
    /// `Size` (empty or over 36 bytes) or `Lexical` (non-alphanumeric).
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

    /// 32 lowercase hex characters from the OS random source (via ring,
    /// already in the tree for TLS), the form the docs recommend for UUIDs.
    ///
    /// # Panics
    /// If the OS random source fails, as `std`'s `RandomState` does.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// 32 lowercase hex characters from the OS random source.
    ///
    /// # Errors
    /// None; see `# Panics`.
    pub fn generate() -> Self {
        // TODO ring::rand::SystemRandom::new().fill(&mut [0u8; 16]),
        // hex-encode lowercase into bytes[..32], len = 32.
        todo!()
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

/// A non-empty list of at most `MAX` distinct items.
///
/// Carries the documented count limits in the type, so the check happens
/// when the caller builds the list, before any request exists:
/// `cancelOrders` takes `Batch<OrderId, 10>`, `GET /orders` takes
/// `Batch<OrderId, 10>` or `Batch<ClientOrderId, 10>`, `GET /executions`
/// takes `Batch<ExecutionId, 10>`.
///
/// # Invariants
/// `1 <= len <= MAX` and no two items are equal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch<T, const MAX: usize> {
    items: Vec<T>,
}

impl<T: Eq, const MAX: usize> Batch<T, MAX> {
    /// From up to `MAX` distinct items.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// On `Ok`, `as_slice()` is `items` in the same order.
    /// # Errors
    /// `Size { min: 1, max: MAX, .. }` for an empty or oversized list (step 1,
    /// checked before the duplicate scan), `Semantics("duplicate")`.
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
        // MAX is at most 10 in every use, so the quadratic scan is cheaper than hashing.
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

    /// A batch of one item.
    ///
    /// # Preconditions
    /// None.
    ///
    /// # Postconditions
    /// A batch holding exactly `item`.
    ///
    /// # Errors
    /// Infallible.
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
