//! チャネルごとの知識: コマンドの本文と、受信メッセージのデコード。
//!
//! [`Codec`] は封印されている (このモジュールは非公開) ので、そのメソッドは
//! 公開 API の一部ではない。`FeedChannel` のスーパートレイトとして
//! `PublicChannel` と `PrivateChannel` だけが実装する。

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{Disconnect, PrivateChannel, PrivateEvent, PublicChannel, PublicEvent};
use crate::error::{ApiGroup, ProtocolErrorKind};

pub trait Codec: Sized + PartialEq {
    type Event;
    /// 置き換えのキー: 同じキーへの後の subscribe が先のものを置き換える。
    type Key: Ord + Clone + Send;
    const API: ApiGroup;
    fn key(&self) -> Self::Key;
    /// `{"command":"subscribe","channel":"ticker","symbol":"BTC"}` の類。
    /// チャネルに `option` があればそれも含む。
    fn command(&self, subscribe: bool) -> String;
    /// テキストメッセージ 1 つをデコードする。
    ///
    /// # エラー
    /// `channel` が既知のチャネルを指すのにペイロードが一致しないとき
    /// `WsMessage` (フェイルクローズド)。`error` フィールドを持つ
    /// メッセージは `Decoded::Rejected` になる。それ以外の形も `WsMessage`。
    fn decode(text: &str) -> Result<Decoded<Self::Event>, ProtocolErrorKind>;
    /// データイベントのキー。`Disconnected`、`Reconnected`、`Rejected` は `None`。
    fn event_key(event: &Self::Event) -> Option<Self::Key>;
    fn disconnected(d: Disconnect) -> Self::Event;
    fn reconnected() -> Self::Event;
    fn rejected(channel: Option<Self>, text: Box<str>) -> Self::Event;
}

/// デコードの結果。拒否はチャネルを持たない: どのコマンドへの返答かは
/// セッションがこの接続に最後に書いたコマンドから決める。
#[derive(Debug)]
pub enum Decoded<E> {
    Event(E),
    Rejected(Box<str>),
}

/// subscribe / unsubscribe コマンドの本文。
#[derive(Serialize)]
struct Command<'a> {
    #[serde(rename = "command")]
    verb: &'static str,
    channel: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    option: Option<&'static str>,
}

impl Command<'_> {
    #[expect(
        clippy::expect_used,
        reason = "文字列だけの平らなオブジェクトの直列化は失敗しない"
    )]
    fn render(&self) -> String {
        serde_json::to_string(self).expect("a command is a plain object of strings")
    }
}

fn command_word(subscribe: bool) -> &'static str {
    if subscribe {
        "subscribe"
    } else {
        "unsubscribe"
    }
}

/// 受信メッセージの先頭だけ: どのチャネルか、または拒否か。
#[derive(serde::Deserialize)]
struct Head {
    channel: Option<String>,
    error: Option<serde_json::Value>,
}

fn ws_message(e: &serde_json::Error) -> ProtocolErrorKind {
    ProtocolErrorKind::WsMessage {
        line: e.line(),
        column: e.column(),
    }
}

fn head(text: &str) -> Result<Head, ProtocolErrorKind> {
    serde_json::from_str(text).map_err(|e| ws_message(&e))
}

fn payload<T: DeserializeOwned>(text: &str) -> Result<T, ProtocolErrorKind> {
    serde_json::from_str(text).map_err(|e| ws_message(&e))
}

/// 拒否文の上限。REST の `ApiMessage::text` と同じ 512 バイト。
const MAX_REJECTION_BYTES: usize = 512;

/// サーバの拒否文。文字列ならそのまま、それ以外の JSON 値ならその表記。
/// `MAX_REJECTION_BYTES` を超える分は文字境界で切り捨てる。
fn rejection(error: serde_json::Value) -> Box<str> {
    let mut text = match error {
        serde_json::Value::String(text) => text,
        other => other.to_string(),
    };
    text.truncate(text.floor_char_boundary(MAX_REJECTION_BYTES));
    text.into_boxed_str()
}

/// 既知のチャネル名を持たないメッセージ。位置は先頭を指す。
const UNKNOWN_SHAPE: ProtocolErrorKind = ProtocolErrorKind::WsMessage { line: 1, column: 1 };

impl Codec for PublicChannel {
    type Event = PublicEvent;
    type Key = (u8, crate::domain::Symbol);
    const API: ApiGroup = ApiGroup::PublicWs;
    fn key(&self) -> Self::Key {
        match self {
            PublicChannel::Ticker(s) => (0, *s),
            PublicChannel::OrderBooks(s) => (1, *s),
            PublicChannel::Trades { symbol, .. } => (2, *symbol),
        }
    }
    fn command(&self, subscribe: bool) -> String {
        let (channel, symbol, option) = match self {
            PublicChannel::Ticker(s) => ("ticker", s, None),
            PublicChannel::OrderBooks(s) => ("orderbooks", s, None),
            PublicChannel::Trades { symbol, taker_only } => {
                ("trades", symbol, taker_only.then_some("TAKER_ONLY"))
            }
        };
        Command {
            verb: command_word(subscribe),
            channel,
            symbol: Some(symbol.as_str()),
            option,
        }
        .render()
    }
    fn decode(text: &str) -> Result<Decoded<PublicEvent>, ProtocolErrorKind> {
        let head = head(text)?;
        if let Some(error) = head.error {
            return Ok(Decoded::Rejected(rejection(error)));
        }
        match head.channel.as_deref() {
            Some("ticker") => payload(text).map(PublicEvent::Ticker),
            Some("orderbooks") => payload(text).map(PublicEvent::OrderBook),
            Some("trades") => payload(text).map(PublicEvent::Trade),
            _ => Err(UNKNOWN_SHAPE),
        }
        .map(Decoded::Event)
    }
    fn event_key(event: &PublicEvent) -> Option<Self::Key> {
        match event {
            PublicEvent::Ticker(t) => Some((0, t.symbol)),
            PublicEvent::OrderBook(b) => Some((1, b.symbol)),
            PublicEvent::Trade(t) => Some((2, t.symbol)),
            _ => None,
        }
    }
    fn disconnected(d: Disconnect) -> PublicEvent {
        PublicEvent::Disconnected(d)
    }
    fn reconnected() -> PublicEvent {
        PublicEvent::Reconnected
    }
    fn rejected(channel: Option<Self>, text: Box<str>) -> PublicEvent {
        PublicEvent::Rejected { channel, text }
    }
}

impl Codec for PrivateChannel {
    type Event = PrivateEvent;
    type Key = u8;
    const API: ApiGroup = ApiGroup::PrivateWs;
    fn key(&self) -> u8 {
        match self {
            PrivateChannel::ExecutionEvents => 0,
            PrivateChannel::OrderEvents => 1,
            PrivateChannel::PositionEvents => 2,
            PrivateChannel::PositionSummaryEvents { .. } => 3,
        }
    }
    fn command(&self, subscribe: bool) -> String {
        let (channel, option) = match self {
            PrivateChannel::ExecutionEvents => ("executionEvents", None),
            PrivateChannel::OrderEvents => ("orderEvents", None),
            PrivateChannel::PositionEvents => ("positionEvents", None),
            PrivateChannel::PositionSummaryEvents { periodic } => {
                ("positionSummaryEvents", periodic.then_some("PERIODIC"))
            }
        };
        Command {
            verb: command_word(subscribe),
            channel,
            symbol: None,
            option,
        }
        .render()
    }
    fn decode(text: &str) -> Result<Decoded<PrivateEvent>, ProtocolErrorKind> {
        let head = head(text)?;
        if let Some(error) = head.error {
            return Ok(Decoded::Rejected(rejection(error)));
        }
        match head.channel.as_deref() {
            Some("executionEvents") => payload(text).map(PrivateEvent::Execution),
            Some("orderEvents") => payload(text).map(PrivateEvent::Order),
            Some("positionEvents") => payload(text).map(PrivateEvent::Position),
            Some("positionSummaryEvents") => payload(text).map(PrivateEvent::PositionSummary),
            _ => Err(UNKNOWN_SHAPE),
        }
        .map(Decoded::Event)
    }
    fn event_key(event: &PrivateEvent) -> Option<u8> {
        match event {
            PrivateEvent::Execution(_) => Some(0),
            PrivateEvent::Order(_) => Some(1),
            PrivateEvent::Position(_) => Some(2),
            PrivateEvent::PositionSummary(_) => Some(3),
            _ => None,
        }
    }
    fn disconnected(d: Disconnect) -> PrivateEvent {
        PrivateEvent::Disconnected(d)
    }
    fn reconnected() -> PrivateEvent {
        PrivateEvent::Reconnected
    }
    fn rejected(channel: Option<Self>, text: Box<str>) -> PrivateEvent {
        PrivateEvent::Rejected { channel, text }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::domain::Symbol;

    /// `tests/fixtures/<name>` をコンパイル時に埋め込む。
    macro_rules! fixture {
        ($name:literal) => {
            include_str!(concat!("../../tests/fixtures/", $name))
        };
    }

    fn parsed(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn public_commands_carry_channel_symbol_and_option() {
        assert_eq!(
            parsed(&PublicChannel::Ticker(Symbol::BTC).command(true)),
            json!({"command": "subscribe", "channel": "ticker", "symbol": "BTC"})
        );
        assert_eq!(
            parsed(&PublicChannel::OrderBooks(Symbol::ETH).command(false)),
            json!({"command": "unsubscribe", "channel": "orderbooks", "symbol": "ETH"})
        );
        assert_eq!(
            parsed(
                &PublicChannel::Trades {
                    symbol: Symbol::XRP,
                    taker_only: true
                }
                .command(true)
            ),
            json!({"command": "subscribe", "channel": "trades", "symbol": "XRP", "option": "TAKER_ONLY"})
        );
        assert_eq!(
            parsed(
                &PublicChannel::Trades {
                    symbol: Symbol::XRP,
                    taker_only: false
                }
                .command(true)
            ),
            json!({"command": "subscribe", "channel": "trades", "symbol": "XRP"})
        );
    }

    #[test]
    fn private_commands_have_no_symbol() {
        assert_eq!(
            parsed(&PrivateChannel::ExecutionEvents.command(true)),
            json!({"command": "subscribe", "channel": "executionEvents"})
        );
        assert_eq!(
            parsed(&PrivateChannel::PositionSummaryEvents { periodic: true }.command(true)),
            json!({"command": "subscribe", "channel": "positionSummaryEvents", "option": "PERIODIC"})
        );
        assert_eq!(
            parsed(&PrivateChannel::OrderEvents.command(false)),
            json!({"command": "unsubscribe", "channel": "orderEvents"})
        );
    }

    #[test]
    fn a_known_channel_with_a_wrong_payload_fails_closed_with_a_position() {
        let err = PublicChannel::decode(r#"{"channel":"ticker","ask":"1"}"#).unwrap_err();
        assert!(
            matches!(err, ProtocolErrorKind::WsMessage { .. }),
            "{err:?}"
        );
        let err = PrivateChannel::decode(r#"{"channel":"orderEvents","orderId":"x"}"#).unwrap_err();
        assert!(
            matches!(err, ProtocolErrorKind::WsMessage { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn an_unknown_channel_or_a_non_object_fails_closed() {
        for text in [r#"{"channel":"klines"}"#, "[1,2]", "", r#"{"hello":1}"#] {
            assert!(
                matches!(
                    PublicChannel::decode(text),
                    Err(ProtocolErrorKind::WsMessage { .. })
                ),
                "{text:?}"
            );
            assert!(
                matches!(
                    PrivateChannel::decode(text),
                    Err(ProtocolErrorKind::WsMessage { .. })
                ),
                "{text:?}"
            );
        }
    }

    fn rejected_text<C: Codec>(text: &str) -> Box<str> {
        match C::decode(text).unwrap() {
            Decoded::Rejected(text) => text,
            Decoded::Event(_) => panic!("{text} is a rejection"),
        }
    }

    #[test]
    fn an_error_field_becomes_rejected_with_its_text() {
        assert_eq!(
            &*rejected_text::<PublicChannel>(r#"{"error":"Requests are too many."}"#),
            "Requests are too many."
        );
        assert_eq!(
            &*rejected_text::<PrivateChannel>(r#"{"error":{"code":1}}"#),
            r#"{"code":1}"#
        );
    }

    #[test]
    fn a_rejection_text_is_cut_at_512_bytes_on_a_char_boundary() {
        let long = "あ".repeat(200);
        let text = rejected_text::<PublicChannel>(&format!(r#"{{"error":"{long}"}}"#));
        assert_eq!(text.len(), 510, "170 three-byte chars fit under 512 bytes");
        assert!(text.chars().all(|c| c == 'あ'));
    }

    #[test]
    fn every_ws_fixture_decodes_to_its_event() {
        let public = |text: &str| match PublicChannel::decode(text).unwrap() {
            Decoded::Event(event) => event,
            Decoded::Rejected(text) => panic!("{text}"),
        };
        assert!(
            matches!(public(fixture!("ws_public_ticker.json")), PublicEvent::Ticker(t) if t.symbol == Symbol::BTC)
        );
        assert!(
            matches!(public(fixture!("ws_public_orderbooks.json")), PublicEvent::OrderBook(b) if b.asks.len() == 2)
        );
        assert!(
            matches!(public(fixture!("ws_public_trades.json")), PublicEvent::Trade(t) if t.symbol == Symbol::BTC)
        );
        let private = |text: &str| match PrivateChannel::decode(text).unwrap() {
            Decoded::Event(event) => event,
            Decoded::Rejected(text) => panic!("{text}"),
        };
        assert!(
            matches!(private(fixture!("ws_private_executionEvents.json")), PrivateEvent::Execution(e) if e.symbol == Symbol::BTC_JPY)
        );
        assert!(matches!(
            private(fixture!("ws_private_orderEvents.json")),
            PrivateEvent::Order(_)
        ));
        assert!(matches!(
            private(fixture!("ws_private_positionEvents.json")),
            PrivateEvent::Position(_)
        ));
        assert!(matches!(
            private(fixture!("ws_private_positionSummaryEvents.json")),
            PrivateEvent::PositionSummary(_)
        ));
    }
}
