//! HTTP/1.1 クライアント側。hyper の `client::conn::http1` を使い、接続ごとに
//! 同時 1 リクエスト。
//!
//! 範囲は取引所が必要とするものに限る: JSON ボディを伴う `GET`、`POST`、
//! `PUT`、`DELETE`、identity エンコーディング (`Accept-Encoding` は送らない)。
//! パイプライン、`Expect: 100-continue`、HTTP/2 はない。

use core::num::NonZeroU8;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::sync::{Semaphore, SemaphorePermit};
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

/// 1 つの REST ベースのアイドル接続と、送信中リクエスト数の上限。
///
/// 接続は共有ではなく所有である。リクエストは接続を取り出し、やり取りの間
/// ずっと排他的に所有し、応答を完全に読み終えて接続が閉じていないときだけ
/// 戻す。2 つのリクエストが同時に 1 本の接続を使うことはない。`Mutex` が
/// 守るのはアイドルスタックの push と pop だけで、`.await` をまたいで保持
/// されない `std` のミューテックスである。
///
/// 新しく接続を開くのは、同時に処理しているリクエストの中だけである。
/// 送信中リクエストを `max_inflight` 件に絞るので、同時に開く接続、TLS
/// ハンドシェイク、hyper の接続タスクもその本数を超えない。
///
/// # 不変条件
/// - `idle.len() <= max_idle`。`IDLE_TTL` より古い項目は取り出し時に捨てられ、
///   決して再利用されない。
/// - 同時に処理しているリクエスト (接続の取り出しから応答の読み終えまで、
///   再試行を含む) は `max_inflight` 件以下。
/// - 生きている接続は、アイドルスタックにあるか 1 件のリクエストが所有する
///   かのどちらか。したがってその本数は `max(max_idle, max_inflight)` 以下。
pub(crate) struct Pool {
    idle: Mutex<Vec<(Sender, Instant)>>,
    max_idle: u8,
    inflight: Semaphore,
}

impl Pool {
    /// 一般的なロードバランサのアイドルタイムアウトより短いので、プール
    /// された接続が再利用時に既にサーバ側で閉じられていることは稀になる。
    pub(crate) const IDLE_TTL: Duration = Duration::from_secs(15);

    pub(crate) fn new(max_idle: u8, max_inflight: NonZeroU8) -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            max_idle,
            inflight: Semaphore::new(usize::from(max_inflight.get())),
        }
    }

    /// 送信中リクエスト 1 件分の許可を取る。上限に達していれば、どれかが
    /// 終わるまで待つ。
    ///
    /// # パニック
    /// このクレートはセマフォを閉じないので起きない。
    #[expect(
        clippy::expect_used,
        reason = "このセマフォを閉じる経路がないので、acquire の失敗は起きない"
    )]
    async fn enter(&self) -> SemaphorePermit<'_> {
        self.inflight
            .acquire()
            .await
            .expect("the inflight semaphore is never closed")
    }

    fn checkout(&self) -> Option<Sender> {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        idle.retain(|(sender, since)| {
            !sender.is_closed() && now.duration_since(*since) < Self::IDLE_TTL
        });
        idle.pop().map(|(sender, _)| sender)
    }

    fn checkin(&self, sender: Sender) {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
/// - 同時に処理しているリクエストは `Limits::max_inflight_requests` 件以下。
///   上限に達している呼び出しは許可を待つ。この待ちは `config.timeouts` の
///   どの値にも収まらないが、既に並んでいる呼び出しはそれぞれ最大
///   `timeouts.request` で終わるので、その件数で有界になる。
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
    // 許可は、接続の取り出しから応答の読み終えまで、再試行を含む往復の
    // 間ずっと保持し、戻るときに返す。
    let _inflight = route.pool.enter().await;
    let mut retried = false;
    loop {
        let pooled = if retried { None } else { route.pool.checkout() };
        let (mut sender, reused) = if let Some(sender) = pooled {
            (sender, true)
        } else {
            let stream = connector
                .connect(route.base, &config.timeouts)
                .await
                .map_err(|k| fail(k, Delivery::NotSent))?;
            let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .map_err(|e| fail(hyper_kind(&e), Delivery::NotSent))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            (sender, false)
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
                // `is_ready()` は hyper の接続タスクが次に動くまで偽のことが
                // あり、再利用できる接続を捨ててしまう。準備は次の取り出しが
                // `ready().await` で待つ。
                if !sender.is_closed() {
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
