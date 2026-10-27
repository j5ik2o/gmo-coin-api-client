//! Enumerated wire values.
//!
//! Two kinds, chosen by one rule:
//!
//! - **Open** (`open_enum!`): values the exchange sends and has grown before
//!   (`cancelType` gained `CLOSED_ORDER`, `SOK_TAKER`, `PRICE_LIMIT`;
//!   `settleType` gained `LOSS_CUT` in order events). An unrecognized value
//!   decodes to `Unknown(UnknownValue)`, which keeps the exact text. That is
//!   a faithful representation of what arrived, not a default: the caller
//!   sees it and decides. It still passes size and lexical checks, so garbage
//!   fails closed.
//! - **Closed** (`closed_enum!`): values this crate sends, and `Side`, which
//!   is binary by definition. An unrecognized value is a decode error.
//!
//! The wire string for each variant is written once, in the macro call, and
//! both directions derive from it.

use core::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, check_size_and_lexical, deserialize_text};

/// The exact text of an enum value this crate version does not know.
///
/// # Invariants
/// 1 to 64 bytes of `A-Z 0-9 _`, and never equal to a known variant of the
/// enum that holds it. The constructor is private, so a caller cannot build
/// `OrderStatus::Unknown("ORDERED")` and alias a known variant.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct UnknownValue(Box<str>);

impl UnknownValue {
    pub const MAX_LEN: usize = 64;

    fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::EnumValue, text, 1, Self::MAX_LEN, |b| {
            b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'
        })?;
        Ok(Self(text.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for UnknownValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unknown({})", self.0)
    }
}

macro_rules! open_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $wire:literal),+ $(,)? }) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $($(#[$vdoc])* $variant,)+
            /// A value added by the exchange after this crate version.
            Unknown(UnknownValue),
        }

        impl $name {
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $wire,)+
                    Self::Unknown(v) => v.as_str(),
                }
            }

            fn parse(text: &str) -> Result<Self, InvalidValue> {
                match text {
                    $($wire => Ok(Self::$variant),)+
                    other => UnknownValue::parse(other).map(Self::Unknown),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                deserialize_text(d, $name::parse)
            }
        }
    };
}

macro_rules! closed_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $wire:literal),+ $(,)? }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$vdoc])* $variant,)+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)+
                }
            }

            fn parse(text: &str) -> Result<Self, InvalidValue> {
                match text {
                    $($wire => Ok(Self::$variant),)+
                    _ => Err(InvalidValue {
                        primitive: Primitive::EnumValue,
                        violation: super::Violation::Semantics(concat!("not a ", stringify!($name))),
                    }),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                deserialize_text(d, $name::parse)
            }
        }
    };
}

closed_enum!(
    /// `side`.
    Side { Buy = "BUY", Sell = "SELL" }
);

closed_enum!(
    /// `timeInForce` for a LIMIT order. The docs allow `timeInForce` only on
    /// LIMIT; MARKET and STOP always execute FAK, so they carry no field.
    LimitTimeInForce {
        /// Fill and store: the unfilled rest stays on the book. Exchange default.
        Fas = "FAS",
        /// Fill and kill: the unfilled rest expires.
        Fak = "FAK",
        /// Fill or kill: all at once or nothing.
        Fok = "FOK",
        /// Post-only (`SOK`): expires instead of taking liquidity.
        PostOnly = "SOK",
    }
);

closed_enum!(
    /// `settleType` filter for `cancelBulkOrder`.
    SettleFilter { Open = "OPEN", Close = "CLOSE" }
);

closed_enum!(
    /// `transferType` for `POST /account/transfer`.
    TransferType {
        /// From the crypto account to the FX account.
        Withdrawal = "WITHDRAWAL",
        /// From the FX account to the crypto account.
        Deposit = "DEPOSIT",
    }
);

open_enum!(
    /// `GET /status`.
    ExchangeStatus { Maintenance = "MAINTENANCE", PreOpen = "PREOPEN", Open = "OPEN" }
);

open_enum!(
    /// `status` of an order, `orderStatus` of an order event.
    OrderStatus {
        Waiting = "WAITING",
        Ordered = "ORDERED",
        Modifying = "MODIFYING",
        Cancelling = "CANCELLING",
        Canceled = "CANCELED",
        Executed = "EXECUTED",
        Expired = "EXPIRED",
    }
);

open_enum!(
    /// `cancelType`.
    CancelType {
        User = "USER",
        PositionLosscut = "POSITION_LOSSCUT",
        InsufficientBalance = "INSUFFICIENT_BALANCE",
        InsufficientMargin = "INSUFFICIENT_MARGIN",
        AccountLosscut = "ACCOUNT_LOSSCUT",
        MarginCall = "MARGIN_CALL",
        MarginCallLosscut = "MARGIN_CALL_LOSSCUT",
        ExpiredFak = "EXPIRED_FAK",
        ExpiredFok = "EXPIRED_FOK",
        ExpiredSok = "EXPIRED_SOK",
        ExpiredSelfTrade = "EXPIRED_SELFTRADE",
        CollateralLiquidation = "COLLATERAL_LIQUIDATION",
        ClosedOrder = "CLOSED_ORDER",
        SokTaker = "SOK_TAKER",
        PriceLimit = "PRICE_LIMIT",
    }
);

open_enum!(
    /// `orderType`.
    OrderType { Normal = "NORMAL", Losscut = "LOSSCUT" }
);

open_enum!(
    /// `executionType` as reported back.
    ExecutionType { Market = "MARKET", Limit = "LIMIT", Stop = "STOP" }
);

open_enum!(
    /// `settleType` as reported back.
    SettleType { Open = "OPEN", Close = "CLOSE", LossCut = "LOSS_CUT" }
);

open_enum!(
    /// `timeInForce` as reported back.
    TimeInForce { Fak = "FAK", Fas = "FAS", Fok = "FOK", Sok = "SOK" }
);

open_enum!(
    /// `marginCallStatus`.
    MarginCallStatus { Normal = "NORMAL", MarginCall = "MARGIN_CALL", Losscut = "LOSSCUT" }
);

open_enum!(
    /// `cryptoShortfallStatus`.
    CryptoShortfallStatus { Normal = "NORMAL", Shortfall = "SHORTFALL", Liquidation = "LIQUIDATION" }
);

open_enum!(
    /// `status` of a deposit, withdrawal, or transfer record.
    TransferStatus { Executed = "EXECUTED" }
);

open_enum!(
    /// `feeType` in the exchange-fee history.
    FeeType {
        LeverageFee = "LEVERAGE_FEE",
        SpotFeeRefund = "SPOT_FEE_REFUND",
        LeverageFeeRefund = "LEVERAGE_FEE_REFUND",
    }
);

open_enum!(
    /// `msgType` of `orderEvents`.
    OrderMsgType { New = "NOR", Replaced = "ROR", Canceled = "COR", Executed = "ER" }
);

open_enum!(
    /// `msgType` of `executionEvents`.
    ExecutionMsgType { Executed = "ER" }
);

open_enum!(
    /// `msgType` of `positionEvents`.
    PositionMsgType { Opened = "OPR", Updated = "UPR", LosscutUpdated = "ULR", Closed = "CPR" }
);

open_enum!(
    /// `msgType` of `positionSummaryEvents`.
    SummaryMsgType { Init = "INIT", Update = "UPDATE", Periodic = "PERIODIC" }
);
