//! WebSocket フィード。
//!
//! [`Feed`] は、切断をまたいで生き続ける 1 本の論理的な購読ストリームである。
//! 呼び出し側が駆動する。フィード自身はタスクもチャネルも持たない。
//! すべての WebSocket I/O (接続、ハンドシェイク、subscribe コマンド、pong、
//! 再接続) は [`Feed::next`] の中で起きる。したがってバックプレッシャは
//! ソケット自身のもので、上限を決めるべき隠れたバッファは存在しない。
//! 正確には、Private フィードのトークン操作 (`ws-auth`) は REST の接続
//! プールを通り、そのプールは hyper の接続ごとに 1 つのタスクを起動する。
//! フィードを駆動するタスクはそれだけである。
//!
//! 購読はコマンドではなく「望ましい状態」である。`subscribe` と `unsubscribe` は
//! 集合を編集してすぐ戻る。`next` はその集合と、現在の接続に送った内容との
//! 差分を、ペーサのスロット 1 つにつきコマンド 1 つずつ埋める。再接続の後は
//! 「送った」側が空になるので、同じ差分計算がすべての購読を再生する。
//! 二度 subscribe しても何も起きず、再接続が二重に subscribe することもない。
//!
//! このモジュールを隠す cargo feature はない。フレーミングは tokio-tungstenite
//! にあり、REST が既に使うクレート以外を必要としない。

mod codec;
mod events;
mod lease;
mod session;

pub use events::{
    ExecutionEvent, OrderBookUpdate, OrderEvent, PositionEvent, PositionSummaryEvent, TradeEvent,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::client::{Access, Authenticated, Client};
use crate::domain::{InvalidValue, Primitive, Symbol, Violation};
use crate::endpoint::Transport;
use crate::error::{ApiError, ContractViolation, ProtocolError, Result, TransportError};
use crate::rest::Ticker;

/// Public または Private WebSocket API のチャネル: [`PublicChannel`] か
/// [`PrivateChannel`]。封印されている: スーパートレイトは非公開モジュールに
/// あるので、他の型は実装できず、そのメソッドはクレートの外から呼べない。
/// `Feed::<C>::next` は `C::Event` ([`PublicEvent`] または [`PrivateEvent`]) を返す。
pub trait FeedChannel: codec::Codec + Clone + Send + 'static {}

/// Public チャネル。置き換えのキーは (チャネル, 銘柄) である。ドキュメントに
/// よれば同じ銘柄への後の subscribe が先のものを置き換えるので、
/// `Trades { taker_only: true }` は `Trades { taker_only: false }` を置き換える。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PublicChannel {
    Ticker(Symbol),
    OrderBooks(Symbol),
    /// `taker_only` は `option: "TAKER_ONLY"` を送る。
    Trades {
        symbol: Symbol,
        taker_only: bool,
    },
}

/// Private チャネル。接続ごとに各 1 つ。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PrivateChannel {
    ExecutionEvents,
    OrderEvents,
    PositionEvents,
    /// `periodic` は `option: "PERIODIC"` を送る (5 秒ごとのサマリー)。
    PositionSummaryEvents {
        periodic: bool,
    },
}

impl FeedChannel for PublicChannel {}
impl FeedChannel for PrivateChannel {}

/// Public フィードのイベント。
#[derive(Debug)]
#[non_exhaustive]
pub enum PublicEvent {
    Ticker(Ticker),
    OrderBook(OrderBookUpdate),
    Trade(TradeEvent),
    /// 接続が落ちた。`Reconnected` まで何も届かない。ストリームから導いた
    /// データ (ローカルの板) はここから先は古い。
    Disconnected(Disconnect),
    /// 新しい接続が上がり、購読が再生されつつある。切断中に送られた
    /// メッセージは失われている: REST で同期し直すこと。
    Reconnected,
    /// サーバがコマンドを拒否した。
    ///
    /// `channel` は、この接続に最後に書いたコマンドのチャネルである。ただし
    /// そのコマンドがまだデータイベントで裏付けられておらず、拒否の窓が
    /// 閉じる前にフィードが拒否を読んだときに限る。窓は、書き込みから
    /// コマンドの間隔 (1.05 秒) を過ぎた後に、ソケットに未読のフレームが
    /// ないことを `next` が確かめたときに閉じる。取引所は不正なコマンドに
    /// すぐ答え、コマンドはその間隔でしか送らないので、返答を待つコマンドは
    /// 普通は 1 つだけである。新しいキーの subscribe はそのキーの最初の
    /// データイベントで裏付けられる。同じキーを置き換える subscribe (たとえば
    /// `taker_only` の切り替え) はデータでは裏付けられない。古い購読のデータが
    /// 同じキーで流れ続けるからである。それ以外 (裏付け済みのコマンドしか
    /// ない、窓が閉じた後に読んだ) なら `None` で、購読の集合は変わらない。
    ///
    /// フィードは `next` の中でしかソケットを読まないので、拒否が届いた時刻は
    /// 分からない。窓が時刻ではなく `next` の観測で閉じるのはそのためで、
    /// 呼び出し方によって次のようになる。
    /// - `next` で待ち続けていれば、窓は書き込みのおよそ 1.05 秒後に閉じる。
    ///   それより後に届いた拒否は、サーバが自発的に送ったものとみなして
    ///   `None` になる。
    /// - `next` を呼ばずに別のものを `await` していた間に届いた拒否は、遅れて
    ///   読んでも窓が閉じる前に読むので、`channel` が付く。呼び出し側が
    ///   ランタイムのスレッドを塞いだ場合 (`std::thread::sleep` や、
    ///   current-thread ランタイムでの重い同期処理、すべてのワーカーが塞がった
    ///   状態) は当てはまらない。tokio が最初のポーリングで返すソケットの
    ///   準備状況が古く、窓が閉じるので、拒否は `channel: None` で戻る。
    ///   ランタイムのスレッドを塞ぐ使い方は、このフィードでは想定していない。
    ///   未読のフレームが途切れない (呼び出し側が
    ///   ソケットに追いつかない) 間も窓は閉じず、その間に読んだ拒否には、
    ///   サーバが自発的に送ったものでも `channel` が付く。届いた時刻で区別
    ///   できないときは、拒否された購読を黙って残すより、拒否として知らせる
    ///   ほうを選んでいる。
    /// - 遅れて戻ったときに送るべき次のコマンドがあると、フィードは未読の
    ///   拒否を読む前にそのコマンドを書く。その拒否には後のコマンドの
    ///   `channel` が付く。この取り違えは
    ///   <https://github.com/j5ik2o/gmo-coin-api-client/issues/14> で扱う。
    ///
    /// 拒否されたのが subscribe なら、そのチャネルは望ましい集合から外れる。
    /// 再接続が同じ拒否を繰り返すことはなく、もう一度 `subscribe` を呼べば
    /// 送り直す。unsubscribe の拒否は集合を変えない。置き換えの subscribe が
    /// 拒否されたときは、置き換え前の購読が取引所側で次の再接続まで生き
    /// 続ける。そのデータはキーが望ましい集合にないので捨てられ、unsubscribe
    /// は送らない。
    ///
    /// `text` はサーバが送ったままの文章 (たとえば別プロセスからの
    /// subscribe で IP ごとの上限を超えた場合)。512 バイトを超える分は
    /// 文字境界で切り捨てる。REST のメッセージと違い `Protocol` エラーには
    /// しない。文章の長さはモデルの不一致ではなく、フィードを閉じる理由に
    /// ならないからである。
    Rejected {
        channel: Option<PublicChannel>,
        text: Box<str>,
    },
}

/// Private フィードのイベント。
#[derive(Debug)]
#[non_exhaustive]
pub enum PrivateEvent {
    Execution(ExecutionEvent),
    Order(OrderEvent),
    Position(PositionEvent),
    PositionSummary(PositionSummaryEvent),
    /// Public と同じ。切断中の約定は再生されない: `Reconnected` の後に
    /// `latest_executions` と `active_orders` を呼ぶこと。
    Disconnected(Disconnect),
    Reconnected,
    /// [`PublicEvent::Rejected`] と同じ規則。
    Rejected {
        channel: Option<PrivateChannel>,
        text: Box<str>,
    },
}

/// 接続が落ちた理由と、次の試行がいつ始まるか。
#[derive(Debug)]
pub struct Disconnect {
    pub reason: DisconnectReason,
    pub retry_in: Duration,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// ネットワーク、TLS、フレーミング、またはタイムアウト (`ws_idle` を含む)。
    Transport(TransportError),
    /// `POST ws-auth` が一時的なコードで拒まれた: ERR-554、ERR-626、
    /// ERR-5003、ERR-5008、ERR-5009。ERR-5003 (呼び出し上限) はバックオフの
    /// 梯子を 2 段先 (4 倍の待ち) から始める。ERR-5008/5009 は時計のずれ
    /// なので、繰り返すなら時刻同期を疑うこと。
    Api(ApiError),
    /// `POST ws-auth` がエンベロープのない 5xx で答えた (ゲートウェイの
    /// 502/504 など)。
    Protocol(ProtocolError),
    /// サーバが close フレームを送った。
    ServerClosed { code: Option<u16> },
    /// ハンドシェイクで HTTP 503: 文書化されたメンテナンスの答え。
    Maintenance,
    /// ハンドシェイクが 101 でも 503 でもない HTTP 状態で拒否された。Private
    /// フィードでは 401、403、404 を、再利用していたトークンの失効とみなし、
    /// 次の試行でトークンを取り直す。取り直したトークンまで拒まれたら
    /// `next` は `Protocol(WsHandshake)` で終わる (設定の問題であり、
    /// 発行を続けると同じキーを使う別プロセスのトークンを追い出すため)。
    HandshakeRejected { http_status: u16 },
}

/// 1 本の購読ストリーム。遅延して構築される: 最初の `next` まで I/O はない。
///
/// # 使い方の契約
/// - `next` を少なくとも 2 分に 1 回は呼ぶこと。pong は `next` の中から送られ、
///   サーバは ping (1 分に 1 回) が 3 回続けて答えられないと接続を切る。
///   呼ばれていないフィードは、次に呼ばれたときに再接続する。
/// - `next` はキャンセルしても安全である。すべての進捗 (書きかけのフレーム、
///   進行中の接続、購読の差分) は `self` にあるので、`tokio::select!` の中で
///   `next` を使っても何も失われない。
/// - フィードを終えるには `close` を呼ぶこと。drop は close ハンドシェイクなしで
///   ソケットを閉じ、Private フィードではトークンを削除しない。そのトークンは
///   最長 60 分生き続け、その間は口座の上限 5 個の 1 つを占める。
/// - コマンドのペーサ (1 秒に 1 回) は `Client` ごとで、その複製は共有する。
///   取引所の上限は IP ごとなので、1 プロセスには 1 つの `Client` を使い、
///   すべてのフィードをそこから作ること。別々に作った `Client` は互いの
///   コマンドを知らず、合わせて上限を超え得る。
///
/// # 不変条件
/// 望ましい購読の集合は `Limits::max_subscriptions` 件を超えない。
/// `Disconnected` の後、`Reconnected` より前にデータイベントが届くことはない。
///
/// # 残存リスク
/// Private フィードの接続先 URI にはアクセストークンが含まれる。このクレートが
/// 組み立てる文字列はゼロ埋めされるが、tungstenite はハンドシェイクの間その
/// 複製を自身のバッファに持ち、それはゼロ埋めされない。
///
/// # 例
/// ```no_run
/// use gmo_coin_api_client::{Client, Symbol};
/// use gmo_coin_api_client::ws::{PublicChannel, PublicEvent};
///
/// # async fn run() -> gmo_coin_api_client::Result<()> {
/// let mut feed = Client::production().public_feed();
/// feed.subscribe(PublicChannel::Ticker(Symbol::BTC))?;
/// loop {
///     match feed.next().await? {
///         PublicEvent::Ticker(t) => println!("{} last {}", t.symbol, t.last),
///         PublicEvent::Disconnected(d) => println!("切断: {:?}、{:?} 後に再接続", d.reason, d.retry_in),
///         other => println!("{other:?}"),
///     }
/// }
/// # }
/// ```
pub struct Feed<C: FeedChannel> {
    session: session::Session<C>,
}

impl<C: FeedChannel> core::fmt::Debug for Feed<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Feed").finish_non_exhaustive()
    }
}

impl<C: FeedChannel> Feed<C> {
    /// `channel` を望ましい集合に加える。同じキーのものがあれば置き換える。
    ///
    /// # 事前条件
    /// `C` 自身の不変条件以外にはない。
    ///
    /// # 事後条件
    /// ペーサのスロットを得た次の `next` 呼び出しが subscribe を送る。
    ///
    /// # エラー
    /// 集合が既に `Limits::max_subscriptions` 件のキーを持つとき
    /// `TooManySubscriptions`。
    pub fn subscribe(&mut self, channel: C) -> Result<(), ContractViolation> {
        self.session.desire(channel)
    }

    /// `channel` のキーを持つチャネルを取り除く。あったかどうかを返す。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// そのキーが望まれていたかどうかを返す。現在の接続に伝えてあったなら、
    /// 後の `next` が次のペーサのスロットで unsubscribe を送る。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn unsubscribe(&mut self, channel: &C) -> bool {
        self.session.undesire(channel)
    }

    /// 次のイベント。
    ///
    /// # 事前条件
    /// [`Feed`] の使い方の契約: 少なくとも 2 分に 1 回呼ぶこと。
    ///
    /// # 事後条件
    /// - データイベントは、それが届いた時点で望ましい集合にあるチャネルの
    ///   ものだけが返る。
    /// - `next` が `Err` を返さない限り、どの `Disconnected` にも、次のデータ
    ///   イベントより前に `Reconnected` が続く。
    ///
    /// # エラー
    /// 終端的である。以後フィードは閉じており、さらに呼ぶと `FeedClosed` を返す:
    /// - `ws-auth` からの `Api` (たとえば ERR-5012、拒否されたキー)。
    ///   メンテナンス (ERR-5201、5202、5203) と一時的なコード (ERR-554、626、
    ///   5003、5008、5009) は例外で、`Disconnected` になる
    ///   ([`DisconnectReason::Api`])。
    /// - 購読中のチャネルのメッセージがデコードできないときの
    ///   `Protocol(WsMessage)`。フェイルクローズド: モデルがもはや取引所と
    ///   一致していない。
    /// - その試行で発行したばかりのトークンをハンドシェイクが 401、403、404
    ///   で拒んだときの `Protocol(WsHandshake)`。トークンは削除してある。
    ///   取引所が発行直後のトークンを拒むのは設定の問題 (API キーの権限など)
    ///   であり、発行を繰り返すと同じキーの別プロセスのトークンを追い出す。
    /// - `ws-auth` の答えが形を破ったときの、それ以外の `Protocol` と `Contract`。
    ///   エンベロープのない 5xx は例外で、`Disconnected` になる
    ///   ([`DisconnectReason::Protocol`])。
    ///
    /// ネットワークの失敗はここではエラーではない。`Disconnected` イベントに
    /// なり、`Config::reconnect` のバックオフで再接続する。
    pub async fn next(&mut self) -> Result<C::Event> {
        self.session.next().await
    }

    /// close ハンドシェイク (`ws_write` で有界) を送り、Private フィードでは
    /// トークンを削除する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// ソケットは閉じている。Private フィードでは、生きていると考えている
    /// トークンに DELETE を送る。既に失効したと分かっているトークンには送らない。
    /// DELETE が失敗したトークンは 60 分以内に自然に失効する。
    ///
    /// # エラー
    /// close ハンドシェイクまたは DELETE が失敗したときの `Transport` か `Api`。
    /// 両方失敗したときは最初のもの。どちらにせよソケットは閉じている。
    pub async fn close(self) -> Result<()> {
        self.session.close().await
    }
}

impl<A: Access> Client<A> {
    /// Public WebSocket API のフィード。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// I/O なし。フィードは最初の `next` で接続する。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn public_feed(&self) -> Feed<PublicChannel> {
        Feed {
            session: session::Session::public(std::sync::Arc::clone(&self.inner)),
        }
    }

    /// テスト用: WebSocket コマンドのペーシング (1 秒に 1 回) を、このクライアント
    /// とその複製すべてで止める。ループバックのモック取引所に対してだけ使える。
    ///
    /// # 事前条件
    /// Public と Private 両方の WebSocket ベースがループバック IP リテラルへの
    /// 平文トランスポートであること。
    ///
    /// # 事後条件
    /// `Ok` なら、以後のフィードは subscribe と unsubscribe を待たずに送る。
    /// 既に開いているフィードにも効く。
    ///
    /// # エラー
    /// どちらかの WebSocket ベースがループバックでないとき
    /// `Semantics("not loopback")`。本番の取引所に向けて止めることはできない。
    #[doc(hidden)]
    pub fn unpaced_ws_commands_for_loopback(&self) -> Result<(), InvalidValue> {
        self.require_loopback_ws()?;
        self.inner.public_ws_pacer.unpace();
        self.inner.private_ws_pacer.unpace();
        Ok(())
    }

    /// テスト専用の挙動の門: 両方の WebSocket ベースが平文 (ループバック
    /// IP リテラルにしか許されない) であることを求める。
    fn require_loopback_ws(&self) -> Result<(), InvalidValue> {
        let endpoints = &self.inner.endpoints;
        let loopback = [endpoints.public_ws.base(), endpoints.private_ws.base()]
            .iter()
            .all(|base| matches!(base.transport, Transport::Plain(_)));
        if loopback {
            Ok(())
        } else {
            Err(InvalidValue {
                primitive: Primitive::BaseUrl,
                violation: Violation::Semantics("not loopback"),
            })
        }
    }
}

impl Client<Authenticated> {
    /// Private WebSocket API のフィード。トークンは最初の `next` で取得し、
    /// 失効前に延長し、`close` で削除する。
    ///
    /// # 事前条件
    /// なし。
    ///
    /// # 事後条件
    /// I/O なし。トークンは最初の `next` で取得する。
    ///
    /// # エラー
    /// 失敗しない。
    pub fn private_feed(&self) -> Feed<PrivateChannel> {
        Feed {
            session: session::Session::private(self.clone()),
        }
    }

    /// テスト用: トークンの延長 (既定 45 分) と失効 (既定 60 分) の刻みを
    /// 縮めた Private フィード。ループバックのモック取引所に対してだけ使える。
    ///
    /// # 事前条件
    /// `extend_after <= expires_after`。両方の WebSocket ベースがループバック
    /// IP リテラルへの平文トランスポートであること。
    ///
    /// # 事後条件
    /// I/O なし。フィードは取得または延長の要求を送った時刻から
    /// `extend_after` 後に `PUT ws-auth` を送り、失敗なら `retry_after` 後
    /// (ただし失効まで) に再試行する。
    ///
    /// # エラー
    /// どちらかの WebSocket ベースがループバックでないとき
    /// `Semantics("not loopback")`。`extend_after > expires_after` のとき
    /// `Semantics("extend after expiry")`。
    #[doc(hidden)]
    pub fn private_feed_with_lease_schedule_for_loopback(
        &self,
        extend_after: Duration,
        retry_after: Duration,
        expires_after: Duration,
    ) -> Result<Feed<PrivateChannel>, InvalidValue> {
        self.require_loopback_ws()?;
        if extend_after > expires_after {
            return Err(InvalidValue {
                primitive: Primitive::Config,
                violation: Violation::Semantics("extend after expiry"),
            });
        }
        let schedule = lease::LeaseSchedule {
            extend_after,
            retry_after,
            expires_after,
        };
        Ok(Feed {
            session: session::Session::private_with(self.clone(), schedule),
        })
    }
}

/// IP ごとのコマンド予算: subscribe または unsubscribe は 1 秒に 1 回。
///
/// 上限は接続ごとではなく IP ごとなので、1 つの `Client` (とその複製) から
/// 作られたすべてのフィードが共有する。共有状態は「次に書いてよい時刻」の
/// アトミック 1 つである。予約はしない。書く直前に [`Pacer::take`] で
/// compare-and-swap し、通った側がその場で書く。次の時刻は実際に取った
/// 時刻から数えるので、遅れて書いたフィードがあっても、実際の送信同士が
/// `interval` 未満に近づくことはない。`.await` をまたいで握るロックはない。
///
/// # 不変条件
/// `take` が `Ok` を返す時刻は単調で、連続する 2 つは少なくとも `interval`
/// 離れている。
pub(crate) struct Pacer {
    origin: Instant,
    next_free_nanos: AtomicU64,
    /// `INTERVAL` のナノ秒。`unpace` が 0 にする。
    interval_nanos: AtomicU64,
}

/// 584 年を超える長さは `u64::MAX` に丸める。
fn as_nanos_u64(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

impl Pacer {
    /// 1 秒の上限に、タイマーの粗さとサーバ側の計時のずれの分を足したもの。
    pub(crate) const INTERVAL: Duration = Duration::from_millis(1_050);

    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
            next_free_nanos: AtomicU64::new(0),
            interval_nanos: AtomicU64::new(as_nanos_u64(Self::INTERVAL)),
        }
    }

    /// 次に書いてよい時刻。現在以前なら `take` が通り得る。
    pub(crate) fn next_free(&self) -> Instant {
        self.origin + Duration::from_nanos(self.next_free_nanos.load(Ordering::Acquire))
    }

    /// 今から `interval` の間を取る。呼び出し側は `Ok` の直後、`.await` を
    /// 挟まずに書くこと。
    ///
    /// # 事後条件
    /// `Ok` なら、次に書いてよい時刻は現在 + `interval` になっている。
    /// `Err` は次に書いてよい時刻 (現在より後、または別の呼び出し側が
    /// 先に取った後の値) で、呼び出し側はそれまで待って取り直す。
    pub(crate) fn take(&self) -> Result<(), Instant> {
        let interval = self.interval_nanos.load(Ordering::Relaxed);
        let now = as_nanos_u64(self.origin.elapsed());
        let current = self.next_free_nanos.load(Ordering::Acquire);
        if current > now {
            return Err(self.origin + Duration::from_nanos(current));
        }
        self.next_free_nanos
            .compare_exchange(current, now + interval, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|actual| self.origin + Duration::from_nanos(actual))
    }

    fn unpace(&self) {
        self.interval_nanos.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_take_blocks_the_next_one_for_the_interval() {
        let pacer = Pacer::new();
        assert!(pacer.next_free() <= Instant::now());
        let before = Instant::now();
        pacer.take().expect("the first take is free");
        let after = Instant::now();
        let next = pacer.take().expect_err("the interval has not elapsed");
        assert!(next >= before + Pacer::INTERVAL && next <= after + Pacer::INTERVAL);
        assert_eq!(pacer.next_free(), next);
    }

    #[test]
    fn an_unpaced_pacer_lets_consecutive_takes_through() {
        let pacer = Pacer::new();
        pacer.unpace();
        pacer.take().unwrap();
        pacer.take().unwrap();
        assert!(pacer.next_free() <= Instant::now());
    }

    #[test]
    fn only_one_of_many_threads_takes_the_same_interval() {
        let pacer = std::sync::Arc::new(Pacer::new());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pacer = std::sync::Arc::clone(&pacer);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    pacer.take().is_ok()
                })
            })
            .collect();
        let taken = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|took| *took)
            .count();
        assert_eq!(taken, 1);
    }

    #[test]
    fn unpacing_refuses_a_production_client() {
        let client = Client::production();
        let err = client.unpaced_ws_commands_for_loopback().unwrap_err();
        assert_eq!(err.violation, Violation::Semantics("not loopback"));
    }
}
