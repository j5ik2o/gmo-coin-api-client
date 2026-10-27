//! The response envelope. Pure: bytes in, typed outcome out.

use std::borrow::Cow;

use serde::Deserialize;
use serde_json::value::RawValue;

use super::Answer;
use crate::domain::Timestamp;
use crate::error::{ApiError, ApiGroup, ApiMessage, Error, ErrorCode, ProtocolError, ProtocolErrorKind, Result};

/// The envelope as sent. `status` is required; everything else depends on it.
#[derive(Deserialize)]
struct Wire<'a> {
    status: i64,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
    #[serde(borrow)]
    messages: Option<Vec<WireMessage<'a>>>,
    responsetime: Option<Timestamp>,
}

/// `Cow` so an escaped string still decodes; borrowed when it can be.
#[derive(Deserialize)]
pub(crate) struct WireMessage<'a> {
    #[serde(borrow)]
    pub(crate) message_code: Cow<'a, str>,
    #[serde(borrow)]
    pub(crate) message_string: Cow<'a, str>,
}

/// Classifies one HTTP response.
///
/// Order of checks, which is also the fail-closed order:
/// 1. The body parses as an envelope (a JSON object with an integer
///    `status`). If it does not: HTTP status outside 2xx gives
///    `Protocol(HttpStatus)`, otherwise `Protocol(NotAnEnvelope)`. The HTTP
///    status is not trusted on its own because the exchange answers many
///    errors with 200.
/// 2. `status != 0`: `messages` must be a non-empty array of well-formed
///    messages (code `ERR-<n>`, text at most 512 bytes), giving `Error::Api`.
///    Otherwise `Protocol(MalformedErrorEnvelope)`.
/// 3. `status == 0`: an [`Answer`] holding `data` (possibly absent; the
///    endpoint method decides whether absence is allowed).
pub(crate) fn parse(api: ApiGroup, operation: &'static str, http_status: u16, body: &[u8]) -> Result<Answer> {
    let protocol = |kind| Error::Protocol(ProtocolError { api, operation, kind });
    let wire: Wire<'_> = match serde_json::from_slice(body) {
        Ok(w) => w,
        Err(_) if !(200..300).contains(&http_status) => return Err(protocol(ProtocolErrorKind::HttpStatus(http_status))),
        Err(_) => return Err(protocol(ProtocolErrorKind::NotAnEnvelope)),
    };
    if wire.status != 0 {
        let messages = wire
            .messages
            .filter(|m| !m.is_empty())
            .and_then(|m| m.iter().map(message).collect::<Option<Vec<_>>>())
            .ok_or_else(|| protocol(ProtocolErrorKind::MalformedErrorEnvelope))?;
        return Err(Error::Api(ApiError { operation, status: wire.status, messages, response_time: wire.responsetime }));
    }
    Ok(Answer { api, operation, data: wire.data.map(RawValue::to_owned) })
}

/// `None` when the code is not `ERR-<n>` or the text is over 512 bytes.
pub(crate) fn message(m: &WireMessage<'_>) -> Option<ApiMessage> {
    let code = ErrorCode::parse(&m.message_code)?;
    (m.message_string.len() <= 512).then(|| ApiMessage { code, text: m.message_string.as_ref().into() })
}
