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
    /// 保持できる文字列の最大バイト数。
    pub const MAX_LEN: usize = 64;

    fn parse(text: &str) -> Result<Self, InvalidValue> {
        check_size_and_lexical(Primitive::EnumValue, text, 1, Self::MAX_LEN, |b| {
            b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'
        })?;
        Ok(Self(text.into()))
    }

    /// 届いた文字列をそのまま返す。
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
            /// ワイヤ上の文字列。`Unknown` は届いた文字列をそのまま返す。
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
            /// ワイヤ上の文字列。
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
    Side {
        /// 買い。
        Buy = "BUY",
        /// 売り。
        Sell = "SELL",
    }
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
    SettleFilter {
        /// 現物取引の注文と、新規のレバレッジ取引の注文を取り消す。
        Open = "OPEN",
        /// 現物取引の注文と、決済のレバレッジ取引の注文を取り消す。
        Close = "CLOSE",
    }
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
    ExchangeStatus {
        /// メンテナンス中。
        Maintenance = "MAINTENANCE",
        /// プレオープン中。この間の注文と注文変更は ERR-5203 で拒否される。
        PreOpen = "PREOPEN",
        /// 稼動中。
        Open = "OPEN",
    }
);

open_enum!(
    /// 注文の `status`、注文イベントの `orderStatus`。
    OrderStatus {
        /// 待機中。取引所のドキュメントには「逆指値注文の場合は WAITING が有効」とある。
        Waiting = "WAITING",
        /// 注文中。
        Ordered = "ORDERED",
        /// 変更中。
        Modifying = "MODIFYING",
        /// 取消中。
        Cancelling = "CANCELLING",
        /// 取消済。理由は [`CancelType`] に入る。
        Canceled = "CANCELED",
        /// 全量約定。
        Executed = "EXECUTED",
        /// 失効。
        Expired = "EXPIRED",
    }
);

open_enum!(
    /// `cancelType`。
    CancelType {
        /// ユーザーによる取消。
        User = "USER",
        /// 建玉のロスカットによる取消。
        PositionLosscut = "POSITION_LOSSCUT",
        /// 残高不足による取消。
        InsufficientBalance = "INSUFFICIENT_BALANCE",
        /// 証拠金不足による取消。
        InsufficientMargin = "INSUFFICIENT_MARGIN",
        /// 口座のロスカットによる取消。
        AccountLosscut = "ACCOUNT_LOSSCUT",
        /// 追加証拠金 (追証) の発生による取消。
        MarginCall = "MARGIN_CALL",
        /// 追加証拠金 (追証) に伴うロスカットによる取消。
        MarginCallLosscut = "MARGIN_CALL_LOSSCUT",
        /// `FAK` の条件により、未約定の残数量が失効した。
        ExpiredFak = "EXPIRED_FAK",
        /// `FOK` の条件により、全数量が失効した。
        ExpiredFok = "EXPIRED_FOK",
        /// `SOK` (Post-only) の条件により、Maker にならない全数量が失効した。
        ExpiredSok = "EXPIRED_SOK",
        /// 自己約定 (セルフトレード) による失効。
        ExpiredSelfTrade = "EXPIRED_SELFTRADE",
        /// 代用暗号資産の清算による取消。
        CollateralLiquidation = "COLLATERAL_LIQUIDATION",
        /// 注文イベントで `orderStatus` が `CANCELED` または `EXPIRED` のときだけ返る取消区分。
        ClosedOrder = "CLOSED_ORDER",
        /// 注文イベントで `orderStatus` が `ORDERED` のときだけ返る取消区分。
        SokTaker = "SOK_TAKER",
        /// 注文イベントで `orderStatus` が `ORDERED` のときだけ返る取消区分。
        PriceLimit = "PRICE_LIMIT",
    }
);

open_enum!(
    /// `orderType`。
    OrderType {
        /// 通常の注文。
        Normal = "NORMAL",
        /// ロスカットによる注文。
        Losscut = "LOSSCUT",
    }
);

open_enum!(
    /// 応答側の `executionType`。
    ExecutionType {
        /// 成行。
        Market = "MARKET",
        /// 指値。
        Limit = "LIMIT",
        /// 逆指値。
        Stop = "STOP",
    }
);

open_enum!(
    /// 応答側の `settleType`。
    SettleType {
        /// 新規。レバレッジ取引の新規注文とその約定に加え、現物取引の注文と約定もこの値を持つ。
        Open = "OPEN",
        /// 決済。
        Close = "CLOSE",
        /// ロスカットによる決済。取引所のドキュメントでは、注文イベントの `settleType` にだけ挙がっている。
        LossCut = "LOSS_CUT",
    }
);

open_enum!(
    /// 応答側の `timeInForce`。
    TimeInForce {
        /// 注文が一部約定したあと、未約定の残数量を失効とする条件。
        Fak = "FAK",
        /// 注文が一部約定したあと、未約定の残数量を有効とする条件。
        Fas = "FAS",
        /// 全数量がすぐに約定しなければ、全数量を失効とする条件。
        Fok = "FOK",
        /// 指値注文が Maker にならなければ、全数量を失効とする条件 (Post-only)。
        Sok = "SOK",
    }
);

open_enum!(
    /// `marginCallStatus`。
    MarginCallStatus {
        /// 追加証拠金は発生していない。
        Normal = "NORMAL",
        /// 追加証拠金 (追証) が発生している。
        MarginCall = "MARGIN_CALL",
        /// ロスカットの状態。
        Losscut = "LOSSCUT",
    }
);

open_enum!(
    /// `cryptoShortfallStatus`。
    CryptoShortfallStatus {
        /// 代用暗号資産不足金は発生していない。
        Normal = "NORMAL",
        /// 代用暗号資産不足金が発生している。
        Shortfall = "SHORTFALL",
        /// 清算の状態。
        Liquidation = "LIQUIDATION",
    }
);

open_enum!(
    /// 入金・出金・振替レコードの `status`。
    TransferStatus {
        /// 実行済み。
        Executed = "EXECUTED",
    }
);

open_enum!(
    /// 取引所手数料履歴の `feeType`。
    FeeType {
        /// 取引所のレバレッジ手数料。
        LeverageFee = "LEVERAGE_FEE",
        /// 現物取引の取引手数料の端数の返金。
        SpotFeeRefund = "SPOT_FEE_REFUND",
        /// レバレッジ取引の取引手数料の端数の返金。`LEVERAGE_FEE` の返金ではない。
        LeverageFeeRefund = "LEVERAGE_FEE_REFUND",
    }
);

open_enum!(
    /// `orderEvents` の `msgType`。
    ///
    /// 取引所のドキュメントは値を挙げるだけで、各値の意味を説明していない。
    OrderMsgType {
        /// `NOR`。
        New = "NOR",
        /// `ROR`。
        Replaced = "ROR",
        /// `COR`。
        Canceled = "COR",
        /// `ER`。約定情報通知 (`executionEvents`) が送る `msgType` と同じ値。
        Executed = "ER",
    }
);

open_enum!(
    /// `executionEvents` の `msgType`。
    ExecutionMsgType {
        /// `ER`。約定情報通知が送る唯一の値。
        Executed = "ER",
    }
);

open_enum!(
    /// `positionEvents` の `msgType`。
    ///
    /// 取引所のドキュメントは値を挙げるだけで、各値の意味を説明していない。
    PositionMsgType {
        /// `OPR`。
        Opened = "OPR",
        /// `UPR`。
        Updated = "UPR",
        /// `ULR`。
        LosscutUpdated = "ULR",
        /// `CPR`。
        Closed = "CPR",
    }
);

open_enum!(
    /// `positionSummaryEvents` の `msgType`。
    ///
    /// 取引所のドキュメントは `INIT` と `UPDATE` の意味を説明していない。
    SummaryMsgType {
        /// `INIT`。
        Init = "INIT",
        /// `UPDATE`。
        Update = "UPDATE",
        /// `PERIODIC`。購読の `option` に `PERIODIC` を指定したときの定期送信 (5 秒ごと)。
        Periodic = "PERIODIC",
    }
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
