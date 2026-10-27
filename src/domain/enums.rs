//! 列挙型のワイヤ値。
//!
//! 2 種類あり、1 つの規則で選ぶ。
//!
//! - **開いた列挙** (`open_enum!`): 取引所が送る値で、過去に増えたことがある
//!   もの (`cancelType` に `CLOSED_ORDER`、`SOK_TAKER`、`PRICE_LIMIT` が増え、
//!   注文イベントの `settleType` に `LOSS_CUT` が増えた)。未知の値は
//!   `Unknown(UnknownValue)` にデコードされ、元の文字列をそのまま保持する。
//!   これは届いた値の忠実な表現であり、既定値ではない。呼び出し側がそれを
//!   見て判断する。サイズと字句の検査は通るので、ごみはフェイルクローズドで
//!   弾かれる。
//! - **閉じた列挙** (`closed_enum!`): このクレートが送る値と、定義上二値の
//!   `Side`。未知の値はデコードエラーになる。
//!
//! 各 variant のワイヤ文字列はマクロ呼び出しに一度だけ書き、双方向とも
//! そこから導かれる。

use core::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{InvalidValue, Primitive, check_size_and_lexical, deserialize_text};

/// このクレートのバージョンが知らない列挙値の、元のままの文字列。
///
/// # 不変条件
/// 1 から 64 バイトの `A-Z 0-9 _` で、それを保持する列挙の既知の variant と
/// 決して等しくない。コンストラクタが非公開なので、呼び出し側は
/// `OrderStatus::Unknown("ORDERED")` を作って既知の variant を偽装できない。
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
            /// このクレートのバージョンより後に取引所が追加した値。
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
    /// `side`。
    Side { Buy = "BUY", Sell = "SELL" }
);

closed_enum!(
    /// LIMIT 注文の `timeInForce`。ドキュメントは `timeInForce` を LIMIT に
    /// のみ許す。MARKET と STOP は常に FAK で執行されるので、フィールドを持たない。
    LimitTimeInForce {
        /// Fill and Store: 未約定分は板に残る。取引所の既定。
        Fas = "FAS",
        /// Fill and Kill: 未約定分は失効する。
        Fak = "FAK",
        /// Fill or Kill: 全量約定か、さもなければ失効。
        Fok = "FOK",
        /// Post-only (`SOK`): 流動性を取る代わりに失効する。
        PostOnly = "SOK",
    }
);

closed_enum!(
    /// `cancelBulkOrder` の `settleType` フィルタ。
    SettleFilter { Open = "OPEN", Close = "CLOSE" }
);

closed_enum!(
    /// `POST /account/transfer` の `transferType`。
    TransferType {
        /// 暗号資産口座から FX 口座へ。
        Withdrawal = "WITHDRAWAL",
        /// FX 口座から暗号資産口座へ。
        Deposit = "DEPOSIT",
    }
);

open_enum!(
    /// `GET /status`。
    ExchangeStatus { Maintenance = "MAINTENANCE", PreOpen = "PREOPEN", Open = "OPEN" }
);

open_enum!(
    /// 注文の `status`、注文イベントの `orderStatus`。
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
    /// `cancelType`。
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
    /// `orderType`。
    OrderType { Normal = "NORMAL", Losscut = "LOSSCUT" }
);

open_enum!(
    /// 応答側の `executionType`。
    ExecutionType { Market = "MARKET", Limit = "LIMIT", Stop = "STOP" }
);

open_enum!(
    /// 応答側の `settleType`。
    SettleType { Open = "OPEN", Close = "CLOSE", LossCut = "LOSS_CUT" }
);

open_enum!(
    /// 応答側の `timeInForce`。
    TimeInForce { Fak = "FAK", Fas = "FAS", Fok = "FOK", Sok = "SOK" }
);

open_enum!(
    /// `marginCallStatus`。
    MarginCallStatus { Normal = "NORMAL", MarginCall = "MARGIN_CALL", Losscut = "LOSSCUT" }
);

open_enum!(
    /// `cryptoShortfallStatus`。
    CryptoShortfallStatus { Normal = "NORMAL", Shortfall = "SHORTFALL", Liquidation = "LIQUIDATION" }
);

open_enum!(
    /// 入金・出金・振替レコードの `status`。
    TransferStatus { Executed = "EXECUTED" }
);

open_enum!(
    /// 取引所手数料履歴の `feeType`。
    FeeType {
        LeverageFee = "LEVERAGE_FEE",
        SpotFeeRefund = "SPOT_FEE_REFUND",
        LeverageFeeRefund = "LEVERAGE_FEE_REFUND",
    }
);

open_enum!(
    /// `orderEvents` の `msgType`。
    OrderMsgType { New = "NOR", Replaced = "ROR", Canceled = "COR", Executed = "ER" }
);

open_enum!(
    /// `executionEvents` の `msgType`。
    ExecutionMsgType { Executed = "ER" }
);

open_enum!(
    /// `positionEvents` の `msgType`。
    PositionMsgType { Opened = "OPR", Updated = "UPR", LosscutUpdated = "ULR", Closed = "CPR" }
);

open_enum!(
    /// `positionSummaryEvents` の `msgType`。
    SummaryMsgType { Init = "INIT", Update = "UPDATE", Periodic = "PERIODIC" }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Violation;

    #[test]
    fn open_enums_keep_unknown_values_and_reject_garbage() {
        let known: CancelType = serde_json::from_str("\"PRICE_LIMIT\"").unwrap();
        assert_eq!(known, CancelType::PriceLimit);
        let unknown: CancelType = serde_json::from_str("\"NEW_REASON_7\"").unwrap();
        assert!(matches!(&unknown, CancelType::Unknown(v) if v.as_str() == "NEW_REASON_7"));
        assert_eq!(unknown.to_string(), "NEW_REASON_7");
        assert!(serde_json::from_str::<CancelType>("\"lower case\"").is_err());
        assert!(serde_json::from_str::<CancelType>("\"\"").is_err());
        assert!(serde_json::from_str::<CancelType>(&format!("\"{}\"", "A".repeat(65))).is_err());
        assert!(serde_json::from_str::<CancelType>("5").is_err());
    }

    #[test]
    fn closed_enums_reject_unknown_values() {
        assert_eq!(serde_json::from_str::<Side>("\"BUY\"").unwrap(), Side::Buy);
        assert!(serde_json::from_str::<Side>("\"HOLD\"").is_err());
        assert_eq!(
            serde_json::to_string(&LimitTimeInForce::PostOnly).unwrap(),
            "\"SOK\""
        );
        assert_eq!(TransferType::Deposit.as_str(), "DEPOSIT");
    }

    #[test]
    fn unknown_value_validation_order() {
        let v = |t: &str| UnknownValue::parse(t).unwrap_err().violation;
        assert!(matches!(v(&"A".repeat(65)), Violation::Size { .. }));
        assert_eq!(v("A-B"), Violation::Lexical { offset: 1 });
    }
}
