//! HTTP/1.1 クライアント側。hyper の `client::conn::http1` を使い、接続ごとに
//! 同時 1 リクエスト。
//!
//! 範囲は取引所が必要とするものに限る: JSON ボディを伴う `GET`、`POST`、
//! `PUT`、`DELETE`、identity エンコーディング (`Accept-Encoding` は送らない)。
//! パイプライン、`Expect: 100-continue`、HTTP/2 はない。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use zeroize::Zeroizing;

use super::{Connector, io_kind};
use crate::client::Config;
use crate::endpoint::Base;
use crate::error::{ApiGroup, Delivery, Phase, TransportError, TransportErrorKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl Method {
    /// 大文字。署名でもこの形を使う (`timestamp + METHOD + path + body`)。
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
        }
    }

    pub(crate) fn to_http(self) -> http::Method {
        match self {
            Method::Get => http::Method::GET,
            Method::Post => http::Method::POST,
            Method::Put => http::Method::PUT,
            Method::Delete => http::Method::DELETE,
        }
    }
}

/// 送信準備のできたリクエスト。
pub(crate) type Request = http::Request<Full<Bytes>>;

/// 完全に読み終えた応答。ボディは `ws-auth` のトークンを含み得るのでゼロ埋めする。
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body: Zeroizing<Vec<u8>>,
}

/// hyper の接続ハンドル。接続を駆動するタスクは `tokio::spawn` されていて、
/// このハンドルが drop されると接続を閉じる。
type Sender = SendRequest<Full<Bytes>>;

/// 1 つの REST ベースのアイドル接続。
///
/// 共有ではなく所有である。リクエストは接続を取り出し、やり取りの間ずっと
/// 排他的に所有し、応答を完全に読み終えて接続がまだ使えるときだけ戻す。
/// 2 つのリクエストが同時に 1 本の接続を使うことはない。`Mutex` が守るのは
/// アイドルスタックの push と pop だけで、`.await` をまたいで保持されない
/// `std` のミューテックスである。
///
/// # 不変条件
/// `idle.len() <= max_idle`。`IDLE_TTL` より古い項目は取り出し時に捨てられ、
/// 決して再利用されない。
pub(crate) struct Pool {
    idle: Mutex<Vec<(Sender, Instant)>>,
    max_idle: u8,
}

impl Pool {
    /// 一般的なロードバランサのアイドルタイムアウトより短いので、プール
    /// された接続が再利用時に既にサーバ側で閉じられていることは稀になる。
    pub(crate) const IDLE_TTL: Duration = Duration::from_secs(15);

    pub(crate) fn new(max_idle: u8) -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            max_idle,
        }
    }

    fn checkout(&self) -> Option<Sender> {
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        idle.retain(|(sender, since)| {
            !sender.is_closed() && now.duration_since(*since) < Self::IDLE_TTL
        });
        idle.pop().map(|(sender, _)| sender)
    }

    fn checkin(&self, sender: Sender) {
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        if idle.len() >= usize::from(self.max_idle) {
            if self.max_idle == 0 {
                return;
            }
            idle.remove(0);
        }
        idle.push((sender, Instant::now()));
    }
}

/// リクエストが使うプールとベース、およびエラーがそれをどう名指すか。
pub(crate) struct Route<'a> {
    pub(crate) api: ApiGroup,
    pub(crate) operation: &'static str,
    pub(crate) base: &'a Base,
    pub(crate) pool: &'a Pool,
}

/// 1 つの試行が失敗した場所。
enum Attempt {
    /// リクエストのバイトは 1 つも書かれていない (`ready` の失敗、または
    /// hyper がリクエストを返してきた)。
    NotWritten,
    /// 書き込みが始まった後の失敗。
    Failed(TransportErrorKind),
}

/// 1 リクエストとその完全な応答。接続は `config.timeouts.connect`、
/// やり取り全体は `config.timeouts.request` に収める。
///
/// # 事後条件
/// - `Delivery::NotSent` の `Err` は、リクエストのバイトが 1 つも書かれて
///   いないときだけ。hyper がリクエストを書き始めた後の失敗はすべて `MaybeSent`。
/// - 再利用した接続で、リクエストが書かれる前に失敗したら、メソッドを問わず
///   新しい接続で一度だけ再試行する。hyper は 1 バイトも書いていないときに
///   限ってリクエストを返すので、取引所に二度届くことはない。`encode` が
///   再び呼ばれるので、タイムスタンプと署名は新しくなる。書き始めた後の
///   失敗はここでは再試行しない。それは `Error::outcome_unknown` の役目である。
pub(crate) async fn round_trip(
    connector: &Connector,
    route: &Route<'_>,
    config: &Config,
    encode: &mut (dyn FnMut() -> Request + Send),
) -> Result<Response, TransportError> {
    let fail = |kind, delivery| TransportError {
        api: route.api,
        operation: route.operation,
        kind,
        delivery,
    };
    let max_body = config.limits.max_response_bytes.get();
    let mut retried = false;
    loop {
        let pooled = if retried { None } else { route.pool.checkout() };
        let (mut sender, reused) = match pooled {
            Some(sender) => (sender, true),
            None => {
                let stream = connector
                    .connect(route.base, &config.timeouts)
                    .await
                    .map_err(|k| fail(k, Delivery::NotSent))?;
                let (sender, connection) =
                    hyper::client::conn::http1::handshake(TokioIo::new(stream))
                        .await
                        .map_err(|e| fail(hyper_kind(&e), Delivery::NotSent))?;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                (sender, false)
            }
        };
        let request = encode();
        let mut delivery = Delivery::NotSent;
        let outcome = tokio::time::timeout(
            config.timeouts.request.get(),
            exchange(&mut sender, request, &mut delivery, max_body),
        )
        .await;
        match outcome {
            Ok(Ok(response)) => {
                if sender.is_ready() && !sender.is_closed() {
                    route.pool.checkin(sender);
                }
                return Ok(response);
            }
            Ok(Err(Attempt::NotWritten)) if reused && !retried => {
                retried = true;
            }
            Ok(Err(Attempt::NotWritten)) => {
                return Err(fail(TransportErrorKind::ClosedEarly, Delivery::NotSent));
            }
            Ok(Err(Attempt::Failed(kind))) => return Err(fail(kind, delivery)),
            Err(_elapsed) => {
                return Err(fail(TransportErrorKind::Timeout(Phase::Request), delivery));
            }
        }
    }
}

/// 1 本の接続で 1 リクエストを送り、応答ボディを上限つきで集める。
async fn exchange(
    sender: &mut Sender,
    request: Request,
    delivery: &mut Delivery,
    max_body: u32,
) -> Result<Response, Attempt> {
    sender.ready().await.map_err(|_| Attempt::NotWritten)?;
    *delivery = Delivery::MaybeSent;
    let response = match sender.try_send_request(request).await {
        Ok(response) => response,
        Err(mut e) => {
            // hyper がリクエストを返してきたなら、1 バイトも書かれていない。
            if e.take_message().is_some() {
                return Err(Attempt::NotWritten);
            }
            return Err(Attempt::Failed(hyper_kind(&e.into_error())));
        }
    };
    let too_large = Attempt::Failed(TransportErrorKind::TooLarge { limit: max_body });
    let declared = response
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|n| n > u64::from(max_body)) {
        return Err(too_large);
    }
    let status = response.status().as_u16();
    let mut body = response.into_body();
    // `Content-Length` の分を先に確保する。伸長のたびに古いバッファが
    // ゼロ埋めされないまま残るのを避けるため (chunked では残る)。
    let reserve = declared.map_or(0, |n| usize::try_from(n).unwrap_or(usize::MAX));
    let mut buf = Zeroizing::new(Vec::with_capacity(reserve));
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| Attempt::Failed(hyper_kind(&e)))?;
        if let Ok(data) = frame.into_data() {
            if buf.len() + data.len() > max_body as usize {
                return Err(Attempt::Failed(TransportErrorKind::TooLarge {
                    limit: max_body,
                }));
            }
            buf.extend_from_slice(&data);
        }
    }
    Ok(Response { status, body: buf })
}

/// hyper のエラーを種類だけに写す。メッセージも原因も保持しない。
fn hyper_kind(e: &hyper::Error) -> TransportErrorKind {
    if e.is_timeout() {
        return TransportErrorKind::Timeout(Phase::Request);
    }
    if e.is_incomplete_message() || e.is_closed() || e.is_canceled() || e.is_body_write_aborted() {
        return TransportErrorKind::ClosedEarly;
    }
    if e.is_parse() {
        return TransportErrorKind::Framing("http");
    }
    match std::error::Error::source(e).and_then(|s| s.downcast_ref::<std::io::Error>()) {
        Some(io) => io_kind(io),
        None => TransportErrorKind::Framing("http"),
    }
}
