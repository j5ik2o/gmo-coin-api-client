//! クライアントのハンドルとその設定。

use core::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crate::domain::{InvalidValue, Primitive, Violation};
use crate::endpoint::Endpoints;
use crate::secret::Credentials;
use crate::transport::Connector;
use crate::transport::http::Pool;
use crate::ws::Pacer;

/// 4 つの API すべてに対する 1 つのハンドル。
///
/// 型パラメータが、何を呼べるかをコンパイル時に決める。
///
/// - `Client<Anonymous>` は Public REST メソッドと `public_feed` を持つ。
/// - `Client<Authenticated>` はそれに加えて、すべての Private REST メソッドと
///   `private_feed` を持つ。
///
/// 実行時の「認証情報がない」エラーは存在しない。匿名クライアントでの
/// Private 呼び出しはコンパイルが通らないからである。
///
/// `Clone` は安価である (`Arc` 1 つ)。複製は接続プールと WebSocket コマンドの
/// ペーサを共有する。この共有は意図的なもので、subscribe の上限は接続ごとでは
/// なく IP ごとだからである。
#[derive(Clone)]
pub struct Client<A: Access = Anonymous> {
    pub(crate) inner: Arc<Inner>,
    pub(crate) access: A,
}

/// 封印されている: 以下の 2 つの状態だけが存在する。
pub trait Access: sealed::Sealed + Clone + Send + Sync + 'static {}

mod sealed {
    pub trait Sealed {}
}

/// 認証情報なし。Public API のみ。
#[derive(Clone, Debug)]
pub struct Anonymous(());

/// 認証情報を保持する。`Clone` は 1 つの `Credentials` を `Arc` で共有し、
/// シークレット自体は決して複製されない。
#[derive(Clone, Debug)]
pub struct Authenticated(pub(crate) Arc<Credentials>);

impl sealed::Sealed for Anonymous {}
impl sealed::Sealed for Authenticated {}
impl Access for Anonymous {}
impl Access for Authenticated {}

pub(crate) struct Inner {
    pub(crate) endpoints: Endpoints,
    pub(crate) config: Config,
    pub(crate) connector: Connector,
    pub(crate) public_pool: Pool,
    pub(crate) private_pool: Pool,
    pub(crate) public_ws_pacer: Pacer,
    pub(crate) private_ws_pacer: Pacer,
}

impl Client<Anonymous> {
    /// # 事前条件
    /// なし。`Endpoints` と `Config` は構築時点で妥当である。
    ///
    /// # 事後条件
    /// ここでは I/O は起きない。接続は最初の呼び出しで遅延して開かれる。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn new(endpoints: Endpoints, config: Config) -> Self {
        let max_idle = config.limits.max_idle_connections;
        let inner = Inner {
            endpoints,
            config,
            connector: Connector::new(),
            public_pool: Pool::new(max_idle),
            private_pool: Pool::new(max_idle),
            public_ws_pacer: Pacer::new(),
            private_ws_pacer: Pacer::new(),
        };
        Self {
            inner: Arc::new(inner),
            access: Anonymous(()),
        }
    }

    /// 本番のエンドポイントと既定の設定。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Client::new(Endpoints::production(), Config::default())` と同じ。I/O なし。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn production() -> Self {
        Self::new(Endpoints::production(), Config::default())
    }

    /// 認証情報を加える。`self` を消費するので、Private API への経路は
    /// 返されたクライアントの 1 つだけになる。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// 返されたクライアントはこのクライアントのプールとペーサを共有する。
    /// `self` は消費されるので、認証情報には結果を通してしか到達できない。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn authenticate(self, credentials: Credentials) -> Client<Authenticated> {
        Client {
            inner: self.inner,
            access: Authenticated(Arc::new(credentials)),
        }
    }
}

/// 調整できるものすべて。すべて有界である。「タイムアウトなし」や
/// 「上限なし」を表す方法はない。
#[derive(Clone, Debug)]
pub struct Config {
    pub timeouts: Timeouts,
    pub limits: Limits,
    pub reconnect: Backoff,
}

impl Default for Config {
    /// [`Timeouts`]、[`Limits`]、[`Backoff`] それぞれの既定値。
    fn default() -> Self {
        let secs = |n| Timeout::new(Duration::from_secs(n)).expect("within 1 ms..=10 min");
        Self {
            timeouts: Timeouts {
                connect: secs(5),
                request: secs(10),
                ws_handshake: secs(10),
                ws_write: secs(5),
                ws_idle: secs(150),
            },
            limits: Limits {
                max_response_bytes: ByteLimit::new(8 * 1024 * 1024).expect("within range"),
                max_ws_message_bytes: ByteLimit::new(1024 * 1024).expect("within range"),
                max_idle_connections: 8,
                max_subscriptions: 64,
            },
            reconnect: Backoff::new(secs(1), secs(60)).expect("initial <= max"),
        }
    }
}

/// 既定値: 接続 5 秒、リクエスト 10 秒、WebSocket ハンドシェイク 10 秒、
/// WebSocket 書き込み 5 秒、WebSocket アイドル 150 秒 (サーバは 60 秒ごとに
/// ping を送るので、150 秒の沈黙は ping が 2 回失われたことを意味する)。
#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    pub connect: Timeout,
    /// REST のやり取り全体: 書き込み、待機、本文の読み切り。
    pub request: Timeout,
    pub ws_handshake: Timeout,
    pub ws_write: Timeout,
    pub ws_idle: Timeout,
}

/// # 不変条件
/// `1 ms <= duration <= 10 min`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeout(Duration);

impl Timeout {
    pub const MAX: Duration = Duration::from_secs(600);

    /// タイムアウト。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `1 ms <= get() <= 10 min`。
    ///
    /// # エラー
    /// `Semantics("timeout out of range")`。
    pub fn new(d: Duration) -> Result<Self, InvalidValue> {
        if d < Duration::from_millis(1) || d > Self::MAX {
            return Err(InvalidValue {
                primitive: Primitive::Config,
                violation: Violation::Semantics("timeout out of range"),
            });
        }
        Ok(Self(d))
    }

    pub fn get(self) -> Duration {
        self.0
    }
}

/// 既定値: 応答本文 8 MiB、WebSocket メッセージ 1 MiB、REST ベースごとに
/// アイドル接続 8 本、フィードごとに購読 64 件。
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// `Content-Length`、またはチャンクごとの累計に対して、バイトをバッファ
    /// する前に検査される。
    pub max_response_bytes: ByteLimit,
    /// tungstenite の `max_message_size` と `max_frame_size` に渡され、
    /// ペイロードを読む前にフレームヘッダの長さに対して検査される。
    pub max_ws_message_bytes: ByteLimit,
    pub max_idle_connections: u8,
    pub max_subscriptions: u8,
}

/// # 不変条件
/// `1 <= bytes <= 64 MiB`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteLimit(NonZeroU32);

impl ByteLimit {
    pub const MAX: u32 = 64 * 1024 * 1024;

    /// バイト数の上限。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `1 <= get() <= 64 MiB`。
    ///
    /// # エラー
    /// `Semantics("byte limit out of range")`。
    pub fn new(bytes: u32) -> Result<Self, InvalidValue> {
        match NonZeroU32::new(bytes) {
            Some(n) if bytes <= Self::MAX => Ok(Self(n)),
            _ => Err(InvalidValue {
                primitive: Primitive::Config,
                violation: Violation::Semantics("byte limit out of range"),
            }),
        }
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// フィードの再接続間隔: `initial` から始めて失敗ごとに倍にし、`max` で
/// 頭打ちにする。接続が `max` の間維持されたらリセットする。既定は 1 秒、60 秒。
///
/// # 不変条件
/// `initial <= max`。
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    initial: Timeout,
    max: Timeout,
}

impl Backoff {
    /// バックオフのスケジュール。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// `Ok` なら `delay(n)` は `n` が増えても減らず、`max` を超えない。
    ///
    /// # エラー
    /// `Semantics("initial > max")`。
    pub fn new(initial: Timeout, max: Timeout) -> Result<Self, InvalidValue> {
        if initial.get() > max.get() {
            return Err(InvalidValue {
                primitive: Primitive::Config,
                violation: Violation::Semantics("initial > max"),
            });
        }
        Ok(Self { initial, max })
    }

    /// `attempt` 回目 (0 始まり) の試行の前に置く遅延。
    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        debug_assert!(
            self.initial.get() <= self.max.get(),
            "Backoff::new keeps initial <= max"
        );
        let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
        self.initial
            .get()
            .saturating_mul(factor)
            .min(self.max.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_documented_values() {
        let c = Config::default();
        assert_eq!(c.timeouts.connect.get(), Duration::from_secs(5));
        assert_eq!(c.timeouts.request.get(), Duration::from_secs(10));
        assert_eq!(c.timeouts.ws_idle.get(), Duration::from_secs(150));
        assert_eq!(c.limits.max_response_bytes.get(), 8 * 1024 * 1024);
        assert_eq!(c.limits.max_ws_message_bytes.get(), 1024 * 1024);
        assert_eq!(c.limits.max_idle_connections, 8);
        assert_eq!(c.limits.max_subscriptions, 64);
        assert_eq!(c.reconnect.delay(0), Duration::from_secs(1));
        assert_eq!(c.reconnect.delay(3), Duration::from_secs(8));
        assert_eq!(c.reconnect.delay(40), Duration::from_secs(60));
    }

    #[test]
    fn bounds_are_enforced() {
        assert!(Timeout::new(Duration::ZERO).is_err());
        assert!(Timeout::new(Duration::from_secs(601)).is_err());
        assert!(Timeout::new(Duration::from_millis(1)).is_ok());
        assert!(ByteLimit::new(0).is_err());
        assert!(ByteLimit::new(ByteLimit::MAX + 1).is_err());
        let a = Timeout::new(Duration::from_secs(2)).unwrap();
        let b = Timeout::new(Duration::from_secs(1)).unwrap();
        assert_eq!(
            Backoff::new(a, b).unwrap_err().violation,
            Violation::Semantics("initial > max")
        );
    }

    #[test]
    fn construction_does_no_io_and_authenticate_shares_the_inner() {
        let client = Client::production();
        let inner = Arc::as_ptr(&client.inner);
        let signed = client.authenticate(crate::secret::Credentials::new(
            crate::secret::ApiKey::new("k".to_owned()).unwrap(),
            crate::secret::ApiSecret::new("s".to_owned()).unwrap(),
        ));
        assert_eq!(Arc::as_ptr(&signed.inner), inner);
        assert_eq!(
            format!("{:?}", signed.access),
            "Authenticated(Credentials(<redacted>))"
        );
    }
}
