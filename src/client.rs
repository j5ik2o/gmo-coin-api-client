//! クライアントのハンドルとその設定。

use core::fmt;
use core::num::{NonZeroU8, NonZeroU32};
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

impl<A: Access> fmt::Debug for Client<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
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
        Self::with_connector(endpoints, config, Connector::new())
    }

    /// 指定したコネクタで組み立てる。[`Client::new`] は webpki のルートを
    /// 使うコネクタを渡し、テストはフィクスチャの証明書を信頼するコネクタを
    /// 渡す。
    pub(crate) fn with_connector(
        endpoints: Endpoints,
        config: Config,
        connector: Connector,
    ) -> Self {
        let limits = config.limits;
        let inner = Inner {
            endpoints,
            config,
            connector,
            public_pool: Pool::new(limits.max_idle_connections, limits.max_inflight_requests),
            private_pool: Pool::new(limits.max_idle_connections, limits.max_inflight_requests),
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
    /// 各通信段階のタイムアウト。
    pub timeouts: Timeouts,
    /// サイズと本数の上限。
    pub limits: Limits,
    /// フィードの再接続間隔。
    pub reconnect: Backoff,
}

impl Default for Config {
    /// [`Timeouts`]、[`Limits`]、[`Backoff`] それぞれの既定値。
    #[expect(
        clippy::expect_used,
        reason = "定数の既定値が検証範囲内であることは単体テストで確かめており、実行時には失敗しない"
    )]
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
                max_inflight_requests: NonZeroU8::new(8).expect("8 is not zero"),
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
    /// 接続の確立のタイムアウト。
    pub connect: Timeout,
    /// REST のやり取り全体: 書き込み、待機、本文の読み切り。
    pub request: Timeout,
    /// WebSocket ハンドシェイクのタイムアウト。
    pub ws_handshake: Timeout,
    /// WebSocket への書き込みのタイムアウト。
    pub ws_write: Timeout,
    /// WebSocket のアイドル (受信がない状態) のタイムアウト。
    pub ws_idle: Timeout,
}

/// # 不変条件
/// `1 ms <= duration <= 10 min`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeout(Duration);

impl Timeout {
    /// `Timeout` に許される最大の長さ (10 分)。
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

    /// 設定された長さを返す。
    pub fn get(self) -> Duration {
        self.0
    }
}

/// 既定値: 応答本文 8 MiB、WebSocket メッセージ 1 MiB、REST ベースごとに
/// アイドル接続 8 本と同時実行 8 件、フィードごとに購読 64 件。
///
/// 同時実行の既定はアイドル接続の既定と同じ 8 なので、1 つの REST ベースで
/// 生きている接続は、送信中とアイドルを合わせても 8 本を超えない (接続は
/// 常にどちらか一方が所有する)。
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// `Content-Length`、またはチャンクごとの累計に対して、バイトをバッファ
    /// する前に検査される。
    pub max_response_bytes: ByteLimit,
    /// tungstenite の `max_message_size` と `max_frame_size` に渡され、
    /// ペイロードを読む前にフレームヘッダの長さに対して検査される。
    pub max_ws_message_bytes: ByteLimit,
    /// REST ベースごとに保持するアイドル接続の最大本数。
    pub max_idle_connections: u8,
    /// REST ベースごとに同時に処理する (送信中の) リクエストの最大数。
    ///
    /// 接続の取り出しから応答の読み終えまでを 1 件と数え、上限に達している
    /// 呼び出しは空くまで順番に待つ。接続、TLS ハンドシェイク、hyper の接続
    /// タスクの数を有界にするためのもので、取引所への呼び出し回数は間引かない。
    /// 0 は取れない (`NonZeroU8`)。0 ではどの REST 呼び出しも永久に待つ。
    pub max_inflight_requests: NonZeroU8,
    /// フィードごとの購読数の上限。
    pub max_subscriptions: u8,
}

/// # 不変条件
/// `1 <= bytes <= 64 MiB`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteLimit(NonZeroU32);

impl ByteLimit {
    /// `ByteLimit` に許される最大のバイト数 (64 MiB)。
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

    /// 設定されたバイト数を返す。常に 1 以上である。
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// フィードの再接続間隔: `initial` から始めて失敗ごとに倍にし、`max` で
/// 頭打ちにする。接続が `max` の間維持されたらリセットする。既定は 1 秒、60 秒。
///
/// 待ち時間にはばらつき (equal jitter) を加える。上限 `cap` に対して
/// `cap/2` 以上 `cap` 以下の一様乱数を返すので、同じ設定でも試行のたびに
/// 値が変わり、`cap` を超えない。複数のフィードが同じ瞬間に再接続し、
/// 購読コマンドが同じ 1 秒の窓に集中して IP ごとの上限に当たるのを避ける
/// ためである。`cap/2` を下回らないので、呼び出し回数の上限 (ERR-5003) で
/// 梯子を 2 段進める判断が、「少なくとも `cap/2` は待つ」という意味を保つ。
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
    /// `Ok` なら `cap(n)` は `n` が増えても減らず、`max` を超えない。
    /// `delay(n)` は `[cap(n)/2, cap(n)]` の乱数なので、これも `max` を超えない。
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

    /// `attempt` 回目 (0 始まり) の試行の待ち時間の上限。ばらつきを含まない。
    ///
    /// 16 段で倍々が飽和し、`initial` が `max / 65536` より小さい設定では
    /// `max` を下回ることがある。
    pub(crate) fn cap(&self, attempt: u32) -> Duration {
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

    /// `attempt` 回目 (0 始まり) の試行の前に置く遅延。`cap(attempt)` に対する
    /// equal jitter で、`cap/2` 以上 `cap` 以下の一様乱数である。同じ設定でも
    /// 呼ぶたびに値が変わる。
    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        self.delay_with(attempt, random_below)
    }

    /// 乱数源を差し替えられる [`Backoff::delay`]。`draw` は `[0, bound)` の値を
    /// 返す関数で、`None` は乱数源が失敗したことを表す。失敗と最小値を
    /// 差し込むためのテスト用の口であり、クレートの外には出ない。
    ///
    /// # 事後条件
    /// 返り値は `cap(attempt)/2` 以上 `cap(attempt)` 以下。`draw` が `None` を
    /// 返したときは `cap(attempt)` ちょうどになる。
    pub(crate) fn delay_with(
        &self,
        attempt: u32,
        draw: impl FnOnce(u64) -> Option<u64>,
    ) -> Duration {
        let cap = self.cap(attempt);
        let cap_nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
        let half = cap_nanos / 2;
        let delay_nanos = match draw(half) {
            // 乱数源が失敗したときは上限そのものに倒す。待ち時間の上限という
            // 不変条件は乱数源の成否に依存しない。
            None => cap_nanos,
            // 差し込み口の契約を破る値が来ても、上限は超えない。
            Some(extra) => half.saturating_add(extra.min(half)),
        };
        Duration::from_nanos(delay_nanos)
    }
}

/// `[0, bound)` の一様乱数。乱数源は、TLS のために既に依存している ring の
/// `SystemRandom` を使う。失敗したときは `None`。
fn random_below(bound: u64) -> Option<u64> {
    use ring::rand::SecureRandom;
    if bound == 0 {
        return Some(0);
    }
    let mut bytes = [0u8; 8];
    ring::rand::SystemRandom::new().fill(&mut bytes).ok()?;
    // `2^64` は `bound` (最大 600 秒 = 6e11 ナノ秒) より桁違いに大きいので、
    // 剰余による偏りは無視できる。
    Some(u64::from_le_bytes(bytes) % bound)
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
        assert_eq!(c.limits.max_inflight_requests.get(), 8);
        assert_eq!(c.limits.max_subscriptions, 64);
        assert_eq!(c.reconnect.cap(0), Duration::from_secs(1));
        assert_eq!(c.reconnect.cap(3), Duration::from_secs(8));
        assert_eq!(c.reconnect.cap(40), Duration::from_secs(60));
    }

    /// ばらつきは呼ぶたびに変わり、どの段でも梯子の上限の半分以上、上限以下
    /// に収まる。段ごとに 1000 回引く。
    #[test]
    fn every_reconnect_delay_is_jittered_within_half_the_cap() {
        use std::collections::BTreeSet;

        let secs = |n| Timeout::new(Duration::from_secs(n)).unwrap();
        let backoff = Backoff::new(secs(1), secs(60)).unwrap();
        for attempt in [0u32, 1, 2, 3, 4, 10, 16, u32::MAX] {
            let cap = backoff.cap(attempt);
            let mut seen = BTreeSet::new();
            for _ in 0..1000 {
                let delay = backoff.delay(attempt);
                assert!(
                    cap / 2 <= delay && delay <= cap,
                    "attempt {attempt}: {delay:?} is outside [{:?}, {cap:?}]",
                    cap / 2
                );
                seen.insert(delay);
            }
            assert!(
                seen.len() > 1,
                "attempt {attempt}: every delay was identical, so there is no jitter"
            );
        }
    }

    /// 梯子は `max` で止まり、待ち時間が `max` を超えることはない。
    #[test]
    fn the_ladder_stops_at_max_and_no_delay_exceeds_it() {
        let secs = |n| Timeout::new(Duration::from_secs(n)).unwrap();
        let backoff = Backoff::new(secs(1), secs(60)).unwrap();
        for attempt in 0..=40u32 {
            let expected = Duration::from_secs((1u64 << attempt.min(16)).min(60));
            assert_eq!(backoff.cap(attempt), expected, "attempt {attempt}");
            for _ in 0..16 {
                assert!(backoff.delay(attempt) <= Duration::from_secs(60));
            }
        }
        assert_eq!(backoff.cap(u32::MAX), Duration::from_secs(60));
    }

    /// 乱数源が失敗したときは、待ち時間は上限そのものになる。
    #[test]
    fn a_failing_random_source_falls_back_to_the_cap() {
        let secs = |n| Timeout::new(Duration::from_secs(n)).unwrap();
        let backoff = Backoff::new(secs(1), secs(60)).unwrap();
        for attempt in [0u32, 1, 3, 10, u32::MAX] {
            assert_eq!(
                backoff.delay_with(attempt, |_| None),
                backoff.cap(attempt),
                "attempt {attempt}"
            );
        }
        // 奇数ナノ秒の上限でも、失敗時の待ち時間は上限ちょうどになる
        // (半分と半分を足すだけでは 1 ナノ秒足りない設定)。
        let odd = Timeout::new(Duration::from_nanos(1_000_000_001)).unwrap();
        let odd_backoff = Backoff::new(odd, odd).unwrap();
        assert_eq!(
            odd_backoff.delay_with(0, |_| None),
            Duration::from_nanos(1_000_000_001)
        );
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
        let signed = client.authenticate(Credentials::new(
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
