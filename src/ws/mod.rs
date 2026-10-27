//! WebSocket フィード。
//!
//! [`Feed`] は、切断をまたいで生き続ける 1 本の論理的な購読ストリームである。
//! 呼び出し側が駆動する。バックグラウンドタスクもチャネルもない。
//! すべての I/O (接続、ハンドシェイク、subscribe コマンド、pong、トークン延長、
//! 再接続) は [`Feed::next`] の中で起きる。したがってバックプレッシャは
//! ソケット自身のもので、上限を決めるべき隠れたバッファは存在しない。
//!
//! 購読はコマンドではなく「望ましい状態」である。`subscribe` と `unsubscribe` は
//! 集合を編集してすぐ戻る。`next` はその集合と、現在の接続に送った内容との
//! 差分を、ペーサのスロット 1 つにつきコマンド 1 つずつ埋める。再接続の後は
//! 「送った」側が空になるので、同じ差分計算がすべての購読を再生する。
//! 二度 subscribe しても何も起きず、再接続が二重に subscribe することもない。
//!
//! このモジュールを隠す cargo feature はない。フレーミングは tokio-tungstenite
//! にあり、REST が既に使うクレート以外を必要としない。

mod events;
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
pub trait FeedChannel: session::codec::Codec + Clone + Send + 'static {}

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
    /// `channel` は、この接続に最後に書いたコマンドのチャネルである (まだ
    /// データイベントで裏付けられていないもの)。コマンドは 1 秒に 1 つしか
    /// 送らないので、拒否が届いた時点で返答待ちのコマンドは高々 1 つという
    /// 前提に立つ。裏付け済みのコマンドしかない、またはサーバが自発的に
    /// 送った拒否なら `None`。
    ///
    /// 拒否されたのが subscribe なら、そのチャネルは望ましい集合から外れる。
    /// 再接続が同じ拒否を繰り返すことはなく、もう一度 `subscribe` を呼べば
    /// 送り直す。unsubscribe の拒否は集合を変えない。
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
///   進行中の接続、購読の差分、取得したペーサのスロット) は `self` にあるので、
///   `tokio::select!` の中で `next` を使っても何も失われない。
/// - フィードを終えるには `close` を呼ぶこと。drop は close ハンドシェイクなしで
///   ソケットを閉じ、Private フィードではトークンを削除せずに (60 分以内に)
///   失効させる。
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
            session: session::Session::public(self.inner.clone()),
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
    pub fn unpaced_ws_commands_for_loopback(&self) -> Result<(), InvalidValue> {
        let endpoints = &self.inner.endpoints;
        let loopback = [endpoints.public_ws.base(), endpoints.private_ws.base()]
            .iter()
            .all(|base| matches!(base.transport, Transport::Plain(_)));
        if !loopback {
            return Err(InvalidValue {
                primitive: Primitive::BaseUrl,
                violation: Violation::Semantics("not loopback"),
            });
        }
        self.inner.public_ws_pacer.unpace();
        self.inner.private_ws_pacer.unpace();
        Ok(())
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
}

/// IP ごとのコマンド予算: subscribe または unsubscribe は 1 秒に 1 回。
///
/// 上限は接続ごとではなく IP ごとなので、1 つの `Client` (とその複製) から
/// 作られたすべてのフィードが共有する。共有状態は「次に空くスロット」の
/// アトミック 1 つで、compare-and-swap で取得する。`.await` をまたいで握る
/// ロックはない。取得した後にキャンセルされたコマンドのスロットは無駄になる
/// が、それはペースを遅くするだけで、速くすることは決してない。
///
/// # 不変条件
/// `claim` が返す時刻は単調で、連続する 2 つは少なくとも `interval` 離れている。
pub(crate) struct Pacer {
    origin: Instant,
    next_free_nanos: AtomicU64,
    /// `INTERVAL` のナノ秒。`unpace` が 0 にする。
    interval_nanos: AtomicU64,
}

impl Pacer {
    /// 1 秒の上限に、タイマーの粗さとサーバ側の計時のずれの分を足したもの。
    pub(crate) const INTERVAL: Duration = Duration::from_millis(1_050);

    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
            next_free_nanos: AtomicU64::new(0),
            interval_nanos: AtomicU64::new(Self::INTERVAL.as_nanos() as u64),
        }
    }

    /// 次のスロットを取得し、その開始時刻を返す。
    ///
    /// # 事後条件
    /// 返る時刻は現在以降で、直前に取得されたスロットより `interval` 以上後。
    pub(crate) fn claim(&self) -> Instant {
        let interval = self.interval_nanos.load(Ordering::Relaxed);
        let now = self.origin.elapsed().as_nanos() as u64;
        let mut current = self.next_free_nanos.load(Ordering::Relaxed);
        loop {
            let start = current.max(now);
            match self.next_free_nanos.compare_exchange_weak(
                current,
                start + interval,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return self.origin + Duration::from_nanos(start),
                Err(actual) => current = actual,
            }
        }
    }

    fn unpace(&self) {
        self.interval_nanos.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_spaced_by_the_interval_and_never_in_the_past() {
        let pacer = Pacer::new();
        let before = Instant::now();
        let first = pacer.claim();
        let second = pacer.claim();
        let third = pacer.claim();
        assert!(first >= before);
        assert!(first <= Instant::now());
        assert_eq!(second - first, Pacer::INTERVAL);
        assert_eq!(third - second, Pacer::INTERVAL);
    }

    #[test]
    fn an_unpaced_pacer_hands_out_the_present() {
        let pacer = Pacer::new();
        pacer.unpace();
        let first = pacer.claim();
        let second = pacer.claim();
        assert!(second >= first);
        assert!(second <= Instant::now());
    }

    #[test]
    fn claims_from_many_threads_never_overlap() {
        let pacer = std::sync::Arc::new(Pacer::new());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pacer = pacer.clone();
                std::thread::spawn(move || (0..50).map(|_| pacer.claim()).collect::<Vec<_>>())
            })
            .collect();
        let mut slots: Vec<Instant> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        slots.sort();
        assert!(slots.windows(2).all(|w| w[1] - w[0] >= Pacer::INTERVAL));
    }

    #[test]
    fn unpacing_refuses_a_production_client() {
        let client = Client::production();
        let err = client.unpaced_ws_commands_for_loopback().unwrap_err();
        assert_eq!(err.violation, Violation::Semantics("not loopback"));
    }
}
