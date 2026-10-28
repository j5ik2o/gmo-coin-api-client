//! 応答エンベロープ。純粋関数: バイト列を受け取り、型付きの結果を返す。

use std::borrow::Cow;

use serde::Deserialize;
use serde_json::value::RawValue;
use zeroize::Zeroizing;

use super::Answer;
use crate::domain::Timestamp;
use crate::error::{
    ApiError, ApiGroup, ApiMessage, Error, ErrorCode, ProtocolError, ProtocolErrorKind, Result,
};

/// 送られてきたままのエンベロープ。`status` は必須で、他はそれ次第である。
#[derive(Deserialize)]
struct Wire<'a> {
    status: i64,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
    #[serde(borrow)]
    messages: Option<Vec<WireMessage<'a>>>,
    responsetime: Option<Timestamp>,
}

/// `Cow` なのでエスケープされた文字列もデコードでき、可能なら借用する。
#[derive(Deserialize)]
pub(crate) struct WireMessage<'a> {
    #[serde(borrow)]
    pub(crate) message_code: Cow<'a, str>,
    #[serde(borrow)]
    pub(crate) message_string: Cow<'a, str>,
}

/// HTTP 応答 1 つを分類する。
///
/// 検査の順序。これがフェイルクローズドの順序でもある。
/// 1. 本文がエンベロープ (整数 `status` を持つ JSON オブジェクト) として
///    解析できる。できなければ、HTTP 状態が 2xx の外なら `Protocol(HttpStatus)`、
///    そうでなければ `Protocol(NotAnEnvelope)`。取引所は多くのエラーを 200 で
///    答えるので、HTTP 状態だけを信用することはない。
/// 2. `status != 0`: `messages` は形式の正しいメッセージ (コードは `ERR-<n>`、
///    文章は 512 バイト以下) の空でない配列でなければならず、`Error::Api` に
///    なる。そうでなければ `Protocol(MalformedErrorEnvelope)`。
/// 3. `status == 0` かつ HTTP 状態が 2xx: `data` を保持する [`Answer`] (`data` は
///    欠けていてもよい。欠けていてよいかはエンドポイントメソッドが決める)。
///    `status == 0` でも HTTP 状態が 2xx の外なら `Protocol(HttpStatus)`。
///    成功の形をした本文を中継機が別の状態で返したとき、それを受付と
///    みなさない。
pub(crate) fn parse(
    api: ApiGroup,
    operation: &'static str,
    http_status: u16,
    body: &[u8],
) -> Result<Answer> {
    let protocol = |kind| {
        Error::Protocol(ProtocolError {
            api,
            operation,
            kind,
        })
    };
    let success_status = (200..300).contains(&http_status);
    let wire: Wire<'_> = match serde_json::from_slice(body) {
        Ok(w) => w,
        Err(_) if !success_status => {
            return Err(protocol(ProtocolErrorKind::HttpStatus(http_status)));
        }
        Err(_) => return Err(protocol(ProtocolErrorKind::NotAnEnvelope)),
    };
    if wire.status == 0 && !success_status {
        return Err(protocol(ProtocolErrorKind::HttpStatus(http_status)));
    }
    if wire.status != 0 {
        let messages = wire
            .messages
            .filter(|m| !m.is_empty())
            .and_then(|m| m.iter().map(message).collect::<Option<Vec<_>>>())
            .ok_or_else(|| protocol(ProtocolErrorKind::MalformedErrorEnvelope))?;
        return Err(Error::Api(ApiError {
            operation,
            status: wire.status,
            messages,
            response_time: wire.responsetime,
        }));
    }
    Ok(Answer {
        api,
        operation,
        data: wire.data.map(|raw| Zeroizing::new(raw.get().to_owned())),
    })
}

/// コードが `ERR-<n>` でない、または文章が 512 バイトを超えるとき `None`。
pub(crate) fn message(m: &WireMessage<'_>) -> Option<ApiMessage> {
    let code = ErrorCode::parse(&m.message_code)?;
    (m.message_string.len() <= 512).then(|| ApiMessage {
        code,
        text: m.message_string.as_ref().into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const OP: &str = "GET /v1/status";

    fn classify(http_status: u16, body: &str) -> Result<Answer> {
        parse(ApiGroup::PublicRest, OP, http_status, body.as_bytes())
    }

    fn protocol_kind(r: Result<Answer>) -> ProtocolErrorKind {
        match r {
            Err(Error::Protocol(p)) => p.kind().clone(),
            Err(other) => panic!("expected a protocol error, got {other:?}"),
            Ok(_) => panic!("expected an error"),
        }
    }

    #[test]
    fn both_live_error_shapes_classify_as_api_errors() {
        let with = include_str!("../../tests/fixtures/error_with_responsetime.json");
        let without = include_str!("../../tests/fixtures/error_without_responsetime.json");
        match classify(200, with) {
            Err(Error::Api(e)) => {
                assert_eq!(e.status(), 1);
                assert!(e.has(ErrorCode::API_KEY_MISSING));
                assert_eq!(e.messages()[0].text.as_ref(), "API-key format invalid.");
                assert!(e.response_time().is_some());
                assert_eq!(e.operation(), OP);
            }
            other => panic!("{other:?}"),
        }
        match classify(200, without) {
            Err(Error::Api(e)) => {
                assert_eq!(e.status(), 2);
                assert!(e.has(ErrorCode::INVALID_SYMBOL_INTERVAL_DATE));
                assert_eq!(e.response_time(), None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_success_envelope_yields_its_data_and_missing_data_is_typed() {
        #[derive(Debug, Deserialize)]
        struct Data {
            status: String,
        }
        let answer = classify(
            200,
            r#"{"status":0,"data":{"status":"OPEN"},"responsetime":"2019-03-19T02:15:06.001Z"}"#,
        )
        .unwrap();
        assert_eq!(answer.raw(), Some(r#"{"status":"OPEN"}"#));
        assert_eq!(answer.data::<Data>().unwrap().status, "OPEN");

        let answer = classify(
            200,
            r#"{"status":0,"responsetime":"2019-03-19T02:15:06.001Z"}"#,
        )
        .unwrap();
        assert!(answer.raw().is_none());
        let err = answer.data::<Data>().unwrap_err();
        assert!(matches!(err, Error::Protocol(p) if *p.kind() == ProtocolErrorKind::MissingData));

        let answer = classify(200, r#"{"status":0,"data":[1,2]}"#).unwrap();
        let err = answer.data::<Data>().unwrap_err();
        assert!(
            matches!(err, Error::Protocol(p) if matches!(p.kind(), ProtocolErrorKind::Shape { .. }))
        );
    }

    #[test]
    fn fail_closed_order_of_checks() {
        assert_eq!(
            protocol_kind(classify(404, "<html>not found</html>")),
            ProtocolErrorKind::HttpStatus(404)
        );
        assert_eq!(
            protocol_kind(classify(503, "")),
            ProtocolErrorKind::HttpStatus(503)
        );
        assert_eq!(
            protocol_kind(classify(200, "<html>")),
            ProtocolErrorKind::NotAnEnvelope
        );
        assert_eq!(
            protocol_kind(classify(200, r#"{"data":1}"#)),
            ProtocolErrorKind::NotAnEnvelope
        );
        assert_eq!(
            protocol_kind(classify(200, r#"{"status":"0"}"#)),
            ProtocolErrorKind::NotAnEnvelope
        );
        assert_eq!(
            protocol_kind(classify(200, r#"{"status":1}"#)),
            ProtocolErrorKind::MalformedErrorEnvelope
        );
        assert_eq!(
            protocol_kind(classify(200, r#"{"status":1,"messages":[]}"#)),
            ProtocolErrorKind::MalformedErrorEnvelope
        );
        assert_eq!(
            protocol_kind(classify(
                200,
                r#"{"status":1,"messages":[{"message_code":"5003","message_string":"x"}]}"#
            )),
            ProtocolErrorKind::MalformedErrorEnvelope
        );
        let long = format!(
            r#"{{"status":1,"messages":[{{"message_code":"ERR-5003","message_string":"{}"}}]}}"#,
            "x".repeat(513)
        );
        assert_eq!(
            protocol_kind(classify(200, &long)),
            ProtocolErrorKind::MalformedErrorEnvelope
        );
        assert!(matches!(
            classify(
                500,
                r#"{"status":1,"messages":[{"message_code":"ERR-554","message_string":"x"}]}"#
            ),
            Err(Error::Api(_))
        ));
        assert_eq!(
            protocol_kind(classify(502, r#"{"status":0,"data":"637000"}"#)),
            ProtocolErrorKind::HttpStatus(502),
            "a success envelope behind a non-2xx status is not a success"
        );
        assert!(classify(201, r#"{"status":0,"data":"637000"}"#).is_ok());
        match classify(
            200,
            r#"{"status":1,"messages":[{"message_code":"ERR-5106","message_string":"bad \"param\""}]}"#,
        ) {
            Err(Error::Api(e)) => assert_eq!(e.messages()[0].text.as_ref(), "bad \"param\""),
            other => panic!("{other:?}"),
        }
    }
}
