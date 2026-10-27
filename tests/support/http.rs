//! The mock's HTTP/1.1 server side. Written independently of the client's
//! `wire::http`.

use tokio::io::{AsyncRead, AsyncWrite};

use super::Reply;

pub struct Request {
    pub method: String,
    /// Path and query as received.
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }

    pub fn is_websocket_upgrade(&self) -> bool {
        self.header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
    }
}

/// Reads one request: head via `httparse::Request`, then exactly
/// `Content-Length` body bytes. `Ok(None)` on a clean EOF between requests
/// (the client closed an idle pooled connection).
pub async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Option<Request>> {
    // TODO read until "\r\n\r\n" (cap 16 KiB), httparse::Request::parse,
    // then read Content-Length bytes. A client that sends chunked or a body
    // without Content-Length is a test failure (the client never should).
    let _ = stream;
    todo!()
}

/// Writes `reply`, keeping the connection open unless the reply closes it.
pub async fn write_reply<S: AsyncWrite + Unpin>(stream: &mut S, reply: &Reply) -> std::io::Result<()> {
    // TODO Fixture/Json => 200 + Content-Length; Status => that code;
    // Chunked => Transfer-Encoding: chunked in `chunk`-sized pieces;
    // CloseAfterRequest => shutdown; Delay => sleep then recurse.
    let _ = (stream, reply);
    todo!()
}
