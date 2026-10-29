//! `Feed` の背後にある、再接続するセッション。所有者は 1 つ、タスクなし、
//! ロックなし。
//!
//! 状態機械 (すべて `Session` の中にあり、future のローカル変数には何もない。
//! それが `next` をキャンセル安全にしている):
//!
//! ```text
//!            +--------------------- バックオフ経過 ----------------------+
//!            v                                                          |
//!   Idle -> Dialing(fut) --ok--> Connected{sent} --切断--> Waiting{until}
//!            |                     |                                    ^
//!            +--終端エラー--> Closed <--- close() / 終端エラー            |
//!            +--再試行可能なエラー----------------------------------------+
//! ```
//!
//! # 不変条件
//! - `Connected` の間、`sent.subscribed` はこの接続に書き込みを始め、
//!   サーバに拒否されていない subscribe の集合と等しい。再接続のたびに
//!   空から始まる。
//! - コマンドは、同じポーリングの中で `Pacer::take` が通った直後にだけ書く。
//!   ペーサの予約は持たないので、キャンセルで失われるものも、遅れて使われる
//!   ものもない。
//! - `sent.last` がある間は、次のコマンドを書かない。先にソケットを読む。
//!   未読の拒否を読む前に次を書くと、その拒否が後のコマンドに付いてしまう
//!   (issue #14)。窓が閉じるまで (期限を過ぎた後は `REJECTION_DRAIN_LIMIT`
//!   フレームまで) 読み、それでもソケットが空にならない (受信が途切れない)
//!   ときは、帰属をあきらめて書く。書き込みが永久に止まらないようにするため
//!   である。あきらめている間も、窓の期限が来るたびにソケットを一度だけ
//!   読んでから書く。読まずに書くと、書き込みが窓を開き直し、あきらめた
//!   状態が続く (issue #23)。読んだ拒否はどのコマンドにも結び付けない
//!   (`Rejected { channel: None }`)。拒否されたのが最後に書いた subscribe で、
//!   そのチャネルがまだ望まれているなら、書き直しの対象にして差分に書かせる。
//!   書き直しはチャネルごとに 1 回までで、それも匿名の拒否で返ってきたら望みも
//!   外す (無効なチャネルを送り続けない)。送った集合は残すので、購読が取引所で
//!   生きている可能性があるときは unsubscribe を書ける。あきらめている間に
//!   書いたコマンドも `last` に残す。
//! - `sent.last` が残るのは、その書き込みから `REJECTION_WINDOW` を過ぎた後に、
//!   ソケットに未読のフレームがないことを `next` がまだ観測していない間だけ
//!   である。期限を過ぎれば、コマンドが待っていても窓ごとに一度は読むので、
//!   空ならその時点で窓が閉じる。あきらめている場合は、空を観測しただけでは
//!   消さない。空の観測と、`last` (あきらめている間に書いた最後のコマンド) の
//!   窓の期限が揃って初めて帰属を再開する。揃うまでに届いた拒否は `None` の
//!   ままにする。

use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::codec::{Codec, Decoded};
use super::lease::{Auth, LeaseSchedule, TokenLease, TokenSource};
use super::{Disconnect, DisconnectReason, FeedChannel, Pacer, PrivateChannel, PublicChannel};
use crate::client::{Authenticated, Client, Inner, Limits};
use crate::error::{
    ApiGroup, ContractViolation, Delivery, Error, ErrorCode, Phase, ProtocolError,
    ProtocolErrorKind, Result, TransportError, TransportErrorKind,
};
use crate::transport::ws::{self, WsConnectError, WsStream};

/// エラーに載せる操作名。ハンドシェイクのパス。トークンは決して含まない。
pub(super) const OPERATION: &str = "/v1";

/// 拒否の窓の長さ。書き込みからこの時間を過ぎた後に、ソケットに未読の
/// フレームがないことを `next` が観測したら、窓を閉じる。それ以後に読む
/// 拒否は、どのコマンドのものともしない。取引所は不正なコマンドにすぐ答え、
/// コマンドはこの間隔でしか送らないので、これより古いコマンドへの返答は
/// もう来ない。静かなチャネル (たとえば `orderEvents`) の subscribe は
/// データで裏付けられるまで長く待つので、窓を閉じないと、後のサーバ発の
/// 拒否がその購読を落とす。
///
/// 窓は、拒否が届いた時刻ではなく、フィードが観測できることで閉じる。
/// フィードは `next` の中でしかソケットを読まないので、`next` が呼ばれて
/// いない間に届いたフレームの到着時刻は分からない。未読のフレームが残って
/// いる限り、その中の拒否は窓の中に届いたかもしれないので、窓を閉じない。
/// ただし、呼び出し側がランタイムのスレッドを塞いでいた場合は、tokio が最初の
/// ポーリングで返すソケットの準備状況が古く、読み込みが `Pending` を返して窓が
/// 閉じることがある。`next` の外で別のものを `await` していた場合だけが、この
/// 約束の対象である。
/// 窓を閉じるのは、期限を過ぎてから始めたポーリングで読み込みが `Pending`
/// を返し、しかもその読み込みが協調スケジューリングの予算切れで止められたの
/// ではないと分かったとき (`Connection::poll_socket`) だけである。予算切れの
/// `Pending` は、読めるデータが残っていても返るので、空のソケットの証拠に
/// ならない。期限のタイマーはタスクを起こすだけで、窓を閉じない。
pub(crate) const REJECTION_WINDOW: Duration = Pacer::INTERVAL;

/// 拒否の窓の期限を過ぎてから、次のコマンドを待たせたまま読むフレーム数の
/// 上限。受信が途切れないソケットでは空を観測できないので、この数に達したら
/// 前のコマンドへの帰属をあきらめて書く。ここで待ち続けると、受信が途切れない
/// 間、書き込みが永久に止まる。取引所は不正なコマンドにすぐ答えるので、
/// 拒否は普通この数より手前に並ぶ。
///
/// あきらめた帰属は、空のソケットを観測しただけでは戻さない。あきらめている
/// 間に書いた最後のコマンドの窓の期限も過ぎている必要がある。そのコマンドへの
/// 返答はまだ届いていないかもしれず、戻した直後に読む拒否を後のコマンドに
/// 付けてしまうからである。
pub(crate) const REJECTION_DRAIN_LIMIT: u32 = 64;

/// 1 回の接続試行。`State::Dialing` に保存され、キャンセルの後も続きから
/// 進む。
type DialFuture = Pin<Box<dyn Future<Output = Dialed> + Send>>;

/// 接続試行の結果。
struct Dialed {
    result: core::result::Result<WsStream, DialFailure>,
    token: TokenSource,
}

enum DialFailure {
    /// フィードを終える: `Api`、`Contract`、メンテナンス以外の `Protocol`、
    /// この試行で発行したトークンのハンドシェイクでの拒否。
    Terminal(Error),
    /// `Disconnected` を返してバックオフ後に再試行する。
    Retry(DisconnectReason),
    /// 再利用したトークンをハンドシェイクが 401/403/404 で拒んだ。リースは
    /// 死んだものとして捨て、次の試行が取り直す。新しいトークンの拒否は
    /// 接続試行の中で `Terminal` になるので、ここには来ない。
    ReusedTokenRejected { http_status: u16 },
}

pub(crate) struct Session<C: FeedChannel> {
    inner: Arc<Inner>,
    auth: Auth,
    /// 呼び出し側の意図。`Limits::max_subscriptions` で有界。
    desired: BTreeMap<<C as Codec>::Key, C>,
    state: State<C>,
    /// 最後に維持された接続以降の失敗した試行の数。`disconnected` でだけ
    /// 増えるので、接続成功時に 0 でなければ直前に `Disconnected` を返して
    /// おり、`Reconnected` を返す番である。
    attempt: u32,
}

enum State<C: FeedChannel> {
    Idle,
    Dialing(DialFuture),
    /// tokio-tungstenite の接続は大きいので Box に入れる。
    Connected(Box<Connection>, Sent<C>),
    Waiting {
        until: Instant,
    },
    Closed,
}

/// この接続に伝えた内容。新しい接続のたびに空にリセットされ、それが
/// 再接続で望ましい集合を再生する仕組みである。
struct Sent<C: FeedChannel> {
    subscribed: BTreeMap<<C as Codec>::Key, C>,
    /// 最後に書いたコマンドと書いた時刻のうち、まだ返答で裏付けられておらず、
    /// 拒否の窓 (`REJECTION_WINDOW`) が閉じていないもの。これがある間に
    /// 読んだサーバの拒否は、これに帰属する。新しいキーの subscribe はその
    /// キーの最初のデータイベントで裏付けられる。拒否は 1 つのコマンドに
    /// しか帰属しない。これがある間は次のコマンドを書かず、先にソケットを
    /// 読む。
    last: Option<(Instant, Written<C>)>,
    /// `last` の窓の期限を過ぎてから、次に書くコマンドを待たせたまま読んだ
    /// フレームの数。`REJECTION_DRAIN_LIMIT` に達したら `last` の帰属を
    /// あきらめる。
    read_after_deadline: u32,
    /// 期限までにソケットを空にできず、帰属をあきらめた。未読の拒否が残って
    /// いるかもしれないので、どの拒否も新しいコマンドに結び付けず、`None` で
    /// 知らせる。書き込みは止めない。あきらめている間に書いたコマンドは `last`
    /// に残す。帰属を再開するのは、ソケットが空だと観測し、かつ `last` の窓の
    /// 期限も過ぎたときである。それまでは、遅れて届いた拒否がどのコマンドの
    /// ものか決められない。
    attribution_abandoned: bool,
    /// `last` の窓の期限を過ぎてから、この窓のためにソケットを一度読んだ。
    /// あきらめている間は、これが立つまで次のコマンドを書かない。書いてしまうと
    /// `last` が置き換わり、窓の期限が来る前に新しい窓が開いて、空のソケットを
    /// 観測する機会がなくなる (issue #23)。読み込みが空を観測すれば
    /// `Step::RejectionWindowClosed` が `last` ごとこれを消す。フレームを
    /// 読んだ場合は、それを返した後のポーリングで書く。窓ごとの印が古く
    /// ならないよう、書き込みのたびに false に戻す。
    probed_deadline: bool,
    /// 匿名の拒否で「書いたが取引所に通っていないかもしれない」subscribe。
    /// `subscribed` からは外さない: 拒否が別のコマンドのものだった場合、その
    /// 購読は取引所で生きているかもしれず、呼び出し側が `unsubscribe` を
    /// 呼んだときに差分が unsubscribe を書ける必要がある。差分は、ここに
    /// 入っているキーを `subscribed` に載っていても書き直す。
    retries: BTreeSet<<C as Codec>::Key>,
    /// 匿名の拒否による書き直しを使ったキー。書き直しはチャネルごとに 1 回まで
    /// である。チャネル自体が無効 (ERR-5106 など) だと送り直すたびに拒否され、
    /// 1.05 秒ごとに同じ subscribe を送り続けて、IP ごとのコマンド枠を使い切る。
    /// 2 度目の匿名の拒否では望みを外し、自動の書き直しをやめる (購読の状態は
    /// 残すので、差分は unsubscribe を書ける)。印は帰属が戻ったとき
    /// (`Step::RejectionWindowClosed`) と、呼び出し側が `subscribe` を呼び直した
    /// とき (`Session::desire`) に消す。新しい接続では `empty` で空から始まる。
    retried: BTreeSet<<C as Codec>::Key>,
}

impl<C: FeedChannel> Sent<C> {
    fn empty() -> Self {
        Self {
            subscribed: BTreeMap::new(),
            last: None,
            read_after_deadline: 0,
            attribution_abandoned: false,
            probed_deadline: false,
            retries: BTreeSet::new(),
            retried: BTreeSet::new(),
        }
    }

    /// 呼び出し側が `key` を購読し直した。匿名の拒否による書き直しの印を消し、
    /// 書き直しの対象になっているチャネルなら、もう一度だけ書けるようにする。
    /// 明示的な購読は、自動の書き直しの 1 回に数えない。
    fn resubscribed(&mut self, key: &<C as Codec>::Key) {
        if self.retried.remove(key) {
            self.retries.insert(key.clone());
        }
    }

    /// `key` のデータイベントが届いた。新しいキーの subscribe ならそれで
    /// 通ったと分かる。置き換えは分からない: 古い購読のデータが同じキーで
    /// 流れ続けるからである。
    ///
    /// 帰属をあきらめている間は `last` を残す。`last` の書き込み時刻が、帰属を
    /// 再開してよいかを決める窓の起点になる。データで裏付けられたコマンドへの
    /// 拒否はもう来ないが、それより前に送ったコマンドへの未読の拒否は残り得る。
    fn confirmed(&mut self, key: &<C as Codec>::Key) {
        if self.attribution_abandoned {
            return;
        }
        if let Some((
            _,
            Written::Subscribe {
                channel,
                replaced: false,
            },
        )) = &self.last
            && channel.key() == *key
        {
            self.last = None;
            self.read_after_deadline = 0;
            self.probed_deadline = false;
        }
    }

    /// 読んだ拒否を、窓が開いている最後のコマンドに帰属させ、拒否された
    /// subscribe を取り消す。`desired` からは、そのキーがまだ同じチャネルを
    /// 望んでいるときだけ外す。別のオプションで望み直した subscribe はまだ
    /// 送っていない別のコマンドだからである。そのようなコマンドがなければ
    /// `None` を返し、何も変えない。読んだ時刻は見ない。窓を閉じるのは
    /// ソケットが空だと観測したときの `Step::RejectionWindowClosed` だけである。
    ///
    /// 帰属をあきらめている間 (`attribution_abandoned`) は、未読の拒否がどの
    /// コマンドのものか決められないので、`None` を返す。`last` は帰属を再開して
    /// よいかの窓の起点として残す。最後に書いた subscribe のチャネルをまだ
    /// 望んでいるなら、送った集合は残したまま書き直しの対象にし、差分に 1 度
    /// だけ書き直させる。拒否がその subscribe のものなら購読が黙って欠けず、
    /// 別のコマンドの拒否だった場合も、重複した subscribe は取引所で置き換えに
    /// なるだけである。同じチャネルが 2 度目の匿名の拒否で返ってきたら、望みも
    /// 外して自動の書き直しをやめる (`retried`)。送った集合に残すのは、購読が
    /// 取引所で生きている可能性があるときに unsubscribe を書くためである。
    /// 望み直していないチャネルも、同じ理由でそのまま残す。
    fn rejected(&mut self, desired: &mut BTreeMap<<C as Codec>::Key, C>) -> Option<C> {
        if self.attribution_abandoned {
            self.reject_anonymously(desired);
            return None;
        }
        let (_, command) = self.last.take()?;
        self.read_after_deadline = 0;
        self.probed_deadline = false;
        match command {
            Written::Subscribe { channel, .. } => {
                let key = channel.key();
                self.subscribed.remove(&key);
                if desired.get(&key) == Some(&channel) {
                    desired.remove(&key);
                }
                Some(channel)
            }
            Written::Unsubscribe(channel) => Some(channel),
        }
    }

    /// 帰属をあきらめている間に読んだ、どのコマンドのものか決められない拒否。
    /// 最後に書いた subscribe がまだ望まれていれば、書き直しの対象にして差分に
    /// 1 度だけ書かせる。送った集合からは外さない (取引所で購読が生きている
    /// 可能性があるため)。2 度目の匿名の拒否では望みも外し、自動の書き直しを
    /// やめる。チャネル自体が無効なら 1.05 秒ごとに同じコマンドを送り続け、
    /// IP ごとのコマンド枠を使い切るからである。印は帰属が戻るか、呼び出し側が
    /// `subscribe` を呼び直すと消える。
    fn reject_anonymously(&mut self, desired: &mut BTreeMap<<C as Codec>::Key, C>) {
        let Some((_, Written::Subscribe { channel, .. })) = &self.last else {
            return;
        };
        let key = channel.key();
        if desired.get(&key) != Some(channel) {
            return;
        }
        if self.retried.contains(&key) {
            desired.remove(&key);
        } else {
            self.retried.insert(key.clone());
            self.retries.insert(key);
        }
    }
}

/// この接続に書いた 1 つのコマンド。
enum Written<C> {
    Subscribe {
        channel: C,
        /// この接続で同じキーの subscribe を置き換えた。
        replaced: bool,
    },
    Unsubscribe(C),
}

pub(crate) struct Connection {
    /// tokio-tungstenite の接続。フレーミング、ping への pong、サイズ上限は
    /// すべてこの中にある。書き込みは `poll_ready`、`start_send`、`poll_flush`
    /// で行う。`SinkExt::send` はキャンセル安全でなく、`select!` の中で
    /// 打ち切られると書きかけのフレームを失うからである。
    stream: WsStream,
    connected_at: Instant,
    last_frame_at: Instant,
    /// 最初に `poll_flush` が `Pending` を返した時刻。`ws_write` を超えたら切る。
    flush_since: Option<Instant>,
    /// サーバの close フレームを受けた。tungstenite が積んだ返答の close を
    /// flush してから、この理由で捨てる。
    closing: Option<DisconnectReason>,
}

/// `Connected` で 1 回のポーリングが観測したこと。
enum Step {
    Message(Message),
    Failed(TransportErrorKind),
    /// ペーサの時刻が来て、ソケットが書き込みを受け付けられる。
    WriteReady,
    /// ペーサの「次に書いてよい時刻」に達した。
    SlotReached,
    Idle,
    WriteTimedOut,
    ExtendDue,
    Extended(Instant, Result<()>),
    /// 拒否の窓の期限を過ぎてから、予算の残った読み込みが `Pending` を
    /// 返した。ソケットに未読のフレームがない。拒否の窓を閉じ、あきらめて
    /// いた帰属も再開する (空の観測と `last` の期限が揃ったときだけである)。
    /// `poll_socket` だけが返す。
    RejectionWindowClosed,
    /// 拒否の窓の期限に達した。タスクを起こすだけで状態は変えない。窓を
    /// 閉じるかどうかは、次のポーリングの `poll_socket` が決める。
    RejectionWindowDue,
}

impl Connection {
    /// 1 回のポーリング: まず flush (キャンセル前に積んだフレームと、
    /// tungstenite が読みながら積んだ pong を押し出す)、次に書きたければ
    /// 書き込み準備、最後に読む。
    ///
    /// 書き込みの期限は flush が `Pending` を返した直後に、ここで検査する。
    /// 期限の状態を知るのは flush だけなので、タイマーの腕で先に検査すると
    /// 完了済みの flush を期限切れと誤り、後に検査すると読める限りソケットが
    /// タイマーを飢えさせる。
    ///
    /// 拒否の窓も同じように、読み込みが `Pending` を返した直後にここで閉じる。
    /// ソケットが空だと知るのは読み込みだけだからである。`window_elapsed` は、
    /// このポーリングを始めた時点で窓の期限を過ぎていたことを表す。帰属を
    /// あきらめている間も、窓の起点は `last` (あきらめている間に書いた最後の
    /// コマンド) なので、期限の後に空を観測したときだけ帰属を再開する。
    /// ただし、予算切れの `Pending` は空のソケットの証拠にならないので、
    /// 読み込みの前後で予算が残っていたかを確かめる。
    fn poll_socket(
        &mut self,
        cx: &mut Context<'_>,
        want_write: bool,
        window_elapsed: bool,
        write_bound: Duration,
        limits: &Limits,
    ) -> Poll<Step> {
        match self.stream.poll_flush_unpin(cx) {
            Poll::Ready(Ok(())) => self.flush_since = None,
            Poll::Ready(Err(e)) => return Poll::Ready(Step::Failed(ws::error_kind(&e, limits))),
            Poll::Pending => {
                let since = *self.flush_since.get_or_insert_with(Instant::now);
                if since + write_bound <= Instant::now() {
                    return Poll::Ready(Step::WriteTimedOut);
                }
            }
        }
        if want_write {
            match self.stream.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => return Poll::Ready(Step::WriteReady),
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Step::Failed(ws::error_kind(&e, limits)));
                }
                Poll::Pending => {}
            }
        }
        // tokio は 1 回のタスクのポーリングに使える作業量 (協調スケジューリングの
        // 予算) を決めており、使い切ったタスクの読み込みには、読めるデータが
        // 残っていても `Pending` を返す。予算切れの `Pending` を空のソケットと
        // 取り違えないよう、読み込みの前後で予算が残っていたかを見る。両方で
        // 残っていれば、`Pending` はソケットが空だという意味になる。
        let budget_before = tokio::task::coop::has_budget_remaining();
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(message))) => {
                self.last_frame_at = Instant::now();
                Poll::Ready(Step::Message(message))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Step::Failed(ws::error_kind(&e, limits))),
            Poll::Ready(None) => Poll::Ready(Step::Failed(TransportErrorKind::ClosedEarly)),
            Poll::Pending
                if window_elapsed && budget_before && tokio::task::coop::has_budget_remaining() =>
            {
                Poll::Ready(Step::RejectionWindowClosed)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 望ましい集合と送った集合の最初の差: 足りない (または置き換えられた、
/// または匿名の拒否で書き直す) subscribe が先、それがなければ余った
/// unsubscribe。
fn next_command<C: FeedChannel>(
    desired: &BTreeMap<<C as Codec>::Key, C>,
    sent: &BTreeMap<<C as Codec>::Key, C>,
    retries: &BTreeSet<<C as Codec>::Key>,
) -> Option<(C, bool)> {
    desired
        .iter()
        .find(|(key, channel)| retries.contains(key) || sent.get(key) != Some(channel))
        .map(|(_, channel)| (channel.clone(), true))
        .or_else(|| {
            sent.iter()
                .find(|(key, _)| !desired.contains_key(key))
                .map(|(_, channel)| (channel.clone(), false))
        })
}

fn protocol_error(api: ApiGroup, kind: ProtocolErrorKind) -> Error {
    Error::Protocol(ProtocolError {
        api,
        operation: OPERATION,
        kind,
    })
}

fn transport_error(api: ApiGroup, kind: TransportErrorKind, delivery: Delivery) -> TransportError {
    TransportError {
        api,
        operation: OPERATION,
        kind,
        delivery,
    }
}

fn live_failure(api: ApiGroup, kind: TransportErrorKind) -> DisconnectReason {
    DisconnectReason::Transport(transport_error(api, kind, Delivery::MaybeSent))
}

/// ハンドシェイクの失敗を再試行の理由に写す。トークンの拒否 (Private の
/// 401/403/404) は出所で意味が変わるので、接続試行が先に扱う。
fn dial_failure(api: ApiGroup, e: WsConnectError) -> DialFailure {
    DialFailure::Retry(match e {
        WsConnectError::Transport(kind) => {
            DisconnectReason::Transport(transport_error(api, kind, Delivery::NotSent))
        }
        WsConnectError::Rejected { http_status: 503 } => DisconnectReason::Maintenance,
        WsConnectError::Rejected { http_status } => {
            DisconnectReason::HandshakeRejected { http_status }
        }
    })
}

/// 取引所がトークンを拒んだとみなす HTTP 状態。
fn rejects_token(http_status: u16) -> bool {
    matches!(http_status, 401 | 403 | 404)
}

/// `POST ws-auth` の失敗。再試行するのは、メンテナンス (ERR-5201/5202/5203、
/// またはエンベロープのない 503)、一時的なコード (ERR-554、626、5003、5008、
/// 5009)、エンベロープのない 5xx、ネットワーク。それ以外 (キーの拒否
/// ERR-5011/5012、形の違反、契約違反) は終端。
fn acquire_failure(e: Error) -> DialFailure {
    const MAINTENANCE: [ErrorCode; 3] = [
        ErrorCode::SCHEDULED_MAINTENANCE,
        ErrorCode::EMERGENCY_MAINTENANCE,
        ErrorCode::PRE_OPEN,
    ];
    const TRANSIENT: [ErrorCode; 5] = [
        ErrorCode::SERVER_UNAVAILABLE,
        ErrorCode::SERVER_BUSY,
        ErrorCode::RATE_LIMIT,
        ErrorCode::TIMESTAMP_TOO_LATE,
        ErrorCode::TIMESTAMP_TOO_EARLY,
    ];
    let has_any =
        |api: &crate::error::ApiError, codes: &[ErrorCode]| codes.iter().any(|code| api.has(*code));
    match e {
        Error::Api(api) if has_any(&api, &MAINTENANCE) => {
            DialFailure::Retry(DisconnectReason::Maintenance)
        }
        Error::Api(api) if has_any(&api, &TRANSIENT) => {
            DialFailure::Retry(DisconnectReason::Api(api))
        }
        Error::Protocol(p) if *p.kind() == ProtocolErrorKind::HttpStatus(503) => {
            DialFailure::Retry(DisconnectReason::Maintenance)
        }
        Error::Protocol(p) if matches!(p.kind(), ProtocolErrorKind::HttpStatus(500..=599)) => {
            DialFailure::Retry(DisconnectReason::Protocol(p))
        }
        Error::Transport(t) => DialFailure::Retry(DisconnectReason::Transport(t)),
        other => DialFailure::Terminal(other),
    }
}

/// 理由ごとの、バックオフの梯子を先に進める段数。呼び出し上限 (ERR-5003)
/// は 2 段 (4 倍): 1 秒の窓を確実に越え、上限を叩き続けない。
fn extra_backoff_steps(reason: &DisconnectReason) -> u32 {
    match reason {
        DisconnectReason::Api(api) if api.has(ErrorCode::RATE_LIMIT) => 2,
        _ => 0,
    }
}

fn sleep_until(at: Instant) -> tokio::time::Sleep {
    tokio::time::sleep_until(tokio::time::Instant::from_std(at))
}

impl<C: FeedChannel> Session<C> {
    pub(crate) fn desire(&mut self, channel: C) -> core::result::Result<(), ContractViolation> {
        let max = self.inner.config.limits.max_subscriptions;
        let key = channel.key();
        if !self.desired.contains_key(&key) && self.desired.len() >= usize::from(max) {
            return Err(ContractViolation::TooManySubscriptions { max });
        }
        // 呼び出し側の明示的な購読は、匿名の拒否による送り直しの 1 回に
        // 数えない。呼び直されたら、また 1 度だけ送り直せる。
        if let State::Connected(_, sent) = &mut self.state {
            sent.resubscribed(&key);
        }
        self.desired.insert(key, channel);
        Ok(())
    }

    pub(crate) fn undesire(&mut self, channel: &C) -> bool {
        self.desired.remove(&channel.key()).is_some()
    }

    /// 契約は `Feed::next` を参照。
    pub(crate) async fn next(&mut self) -> Result<<C as Codec>::Event> {
        loop {
            match &mut self.state {
                State::Closed => return Err(Error::FeedClosed),
                State::Idle => self.state = State::Dialing(self.dial()),
                State::Waiting { until } => {
                    sleep_until(*until).await;
                    self.state = State::Idle;
                }
                State::Dialing(fut) => {
                    let dialed = fut.as_mut().await;
                    if let Some(event) = self.dialed(dialed) {
                        return event;
                    }
                }
                State::Connected(..) => {
                    if let Some(event) = self.step().await {
                        return event;
                    }
                }
            }
        }
    }

    /// 接続試行の結果を状態に写す。`None` なら呼び出し側はループを続ける。
    fn dialed(&mut self, dialed: Dialed) -> Option<Result<<C as Codec>::Event>> {
        self.auth.adopt(dialed.token);
        match dialed.result {
            Ok(stream) => {
                let now = Instant::now();
                let connection = Connection {
                    stream,
                    connected_at: now,
                    last_frame_at: now,
                    flush_since: None,
                    closing: None,
                };
                self.state = State::Connected(Box::new(connection), Sent::empty());
                if self.attempt > 0 {
                    Some(Ok(C::reconnected()))
                } else {
                    None
                }
            }
            Err(DialFailure::Terminal(e)) => {
                self.state = State::Closed;
                Some(Err(e))
            }
            Err(DialFailure::ReusedTokenRejected { http_status }) => {
                if let Auth::Private { lease, .. } = &mut self.auth {
                    *lease = None;
                }
                Some(Ok(self.disconnected(DisconnectReason::HandshakeRejected {
                    http_status,
                })))
            }
            Err(DialFailure::Retry(reason)) => Some(Ok(self.disconnected(reason))),
        }
    }

    /// `Waiting` に移り、`Disconnected` を作る。進行中の延長は捨てる。
    fn disconnected(&mut self, reason: DisconnectReason) -> <C as Codec>::Event {
        let step = self.attempt.saturating_add(extra_backoff_steps(&reason));
        let delay = self.inner.config.reconnect.delay(step);
        self.attempt = self.attempt.saturating_add(1);
        self.state = State::Waiting {
            until: Instant::now() + delay,
        };
        self.auth.abandon_extend();
        C::disconnected(Disconnect {
            reason,
            retry_in: delay,
        })
    }

    /// 生きていた接続を捨てる。バックオフの最上段 (`Backoff` の `max`。
    /// `delay` は 16 段で飽和するので、`initial` が `max / 65536` より
    /// 小さい設定でだけ `max` を下回る) の間維持されていたら試行回数を戻す。
    fn drop_connection(&mut self, reason: DisconnectReason) -> <C as Codec>::Event {
        if let State::Connected(conn, _) = &self.state
            && conn.connected_at.elapsed() >= self.inner.config.reconnect.delay(u32::MAX)
        {
            self.attempt = 0;
        }
        self.disconnected(reason)
    }

    /// `Connected` での 1 歩。`None` なら何も返すものがなく、ループを続ける。
    #[expect(
        clippy::too_many_lines,
        reason = "1 つの select! の待ち受けと、その結果ごとの処理を 1 か所で読めるようにしている。分けると状態の受け渡しが増える"
    )]
    async fn step(&mut self) -> Option<Result<<C as Codec>::Event>> {
        let Self {
            inner,
            auth,
            desired,
            state,
            ..
        } = self;
        let State::Connected(conn, sent) = state else {
            unreachable!("step runs only in Connected");
        };
        let pacer = match C::API {
            ApiGroup::PrivateWs => &inner.private_ws_pacer,
            _ => &inner.public_ws_pacer,
        };
        let timeouts = inner.config.timeouts;
        if conn.closing.is_some() {
            let _ = tokio::time::timeout(timeouts.ws_write.get(), conn.stream.flush()).await;
            if let Some(reason) = conn.closing.take() {
                return Some(Ok(self.drop_connection(reason)));
            }
        }
        let now = Instant::now();

        let command = next_command::<C>(desired, &sent.subscribed, &sent.retries);
        let slot = command.as_ref().map(|_| pacer.next_free());
        let idle_at = conn.last_frame_at + timeouts.ws_idle.get();
        let write_deadline = conn
            .flush_since
            .map(|since| since + timeouts.ws_write.get());
        let extend_at = auth.extend_due_at();
        let mut extend = auth.extend_in_flight();
        let window_closes_at = sent
            .last
            .as_ref()
            .map(|(written_at, _)| *written_at + REJECTION_WINDOW);
        let window_elapsed = window_closes_at.is_some_and(|at| at <= now);
        // 期限を過ぎてもソケットが空にならない (受信が途切れない) ときは、
        // 前のコマンドへの帰属をあきらめて書き込みに進む。ここで待ち続けると、
        // 受信が途切れない間、書き込みが永久に止まる。`last` は残す: 帰属を
        // 再開してよいかを決める窓の起点であり、あきらめている間に書く次の
        // コマンドの書き込み時刻で置き換わる。
        if window_elapsed && command.is_some() && sent.read_after_deadline >= REJECTION_DRAIN_LIMIT
        {
            sent.attribution_abandoned = true;
            sent.read_after_deadline = 0;
        }
        // 窓の期限が来たら、あきらめている間でも一度は読んでから書く。読まずに
        // 書くと、書き込みが窓を開き直し、空のソケットを観測する機会が来ない
        // まま帰属をあきらめた状態が続く (issue #23)。期限より前の読み込みは
        // 窓を閉じない (`poll_socket` は `window_elapsed` のときだけ閉じる)。
        let read_due = window_elapsed && !sent.probed_deadline;
        let probed = window_elapsed && sent.probed_deadline;
        // 前のコマンドへの拒否を読む前に次を書くと、その拒否が後のコマンドに
        // 付いてしまう (issue #14)。`last` がある間は書かず、まず読む。
        // あきらめている間は、窓の期限が来るのを待ち、その窓のために一度読んだ
        // 後だけ書き込みを許す。書き込みを止め続けないための逃げ道である。
        let gated = sent.last.is_some() && (!sent.attribution_abandoned || !probed);
        let want_write = !gated && slot.is_some_and(|at| at <= now);
        // 期限の後に読んだフレームを数えるのは、あきらめる前だけである。
        let draining = command.is_some() && sent.last.is_some() && !sent.attribution_abandoned;

        let step = tokio::select! {
            biased;
            (sent_at, result) = poll_fn(|cx| match extend.as_mut() {
                Some(fut) => fut.as_mut().poll(cx),
                None => Poll::Pending,
            }) => Step::Extended(sent_at, result),
            () = sleep_until(extend_at.unwrap_or(now)), if extend_at.is_some() => Step::ExtendDue,
            () = sleep_until(slot.unwrap_or(now)), if slot.is_some_and(|at| at > now) => Step::SlotReached,
            step = poll_fn(|cx| conn.poll_socket(cx, want_write, window_elapsed, timeouts.ws_write.get(), &inner.config.limits)) => step,
            () = sleep_until(window_closes_at.unwrap_or(now)), if window_closes_at.is_some() && !window_elapsed => Step::RejectionWindowDue,
            () = sleep_until(write_deadline.unwrap_or(now)), if write_deadline.is_some() => Step::WriteTimedOut,
            () = sleep_until(idle_at) => Step::Idle,
        };

        // 次のコマンドを待たせている間は、読んだフレームを数える。期限の後に
        // 上限まで読んでソケットが空にならなければ、帰属をあきらめる。
        if matches!(step, Step::Message(_)) && draining && window_elapsed {
            sent.read_after_deadline = sent.read_after_deadline.saturating_add(1);
        }
        // あきらめている間に期限を過ぎて読み込んだフレームは、空でなかったと
        // しても窓ごとの一度を済ませたことになる。次のポーリングは書いてよい。
        // あきらめる前の読み込みは数えない: あきらめた時点から改めて一度読む。
        if matches!(step, Step::Message(_)) && read_due && sent.attribution_abandoned {
            sent.probed_deadline = true;
        }

        match step {
            Step::Message(Message::Text(text)) => match C::decode(&text) {
                Ok(Decoded::Event(event)) => match C::event_key(&event) {
                    Some(key) if !desired.contains_key(&key) => None,
                    Some(key) => {
                        sent.confirmed(&key);
                        Some(Ok(event))
                    }
                    None => Some(Ok(event)),
                },
                Ok(Decoded::Rejected(text)) => {
                    let channel = sent.rejected(desired);
                    Some(Ok(C::rejected(channel, text)))
                }
                Err(kind) => {
                    self.state = State::Closed;
                    Some(Err(protocol_error(C::API, kind)))
                }
            },
            Step::Message(Message::Binary(_)) => Some(Ok(self.drop_connection(live_failure(
                C::API,
                TransportErrorKind::Framing("websocket binary frame"),
            )))),
            Step::Message(Message::Close(frame)) => {
                let code = frame.map(|f| u16::from(f.code));
                conn.closing = Some(DisconnectReason::ServerClosed { code });
                None
            }
            Step::Failed(kind) => Some(Ok(self.drop_connection(live_failure(C::API, kind)))),
            Step::WriteReady => {
                #[expect(
                    clippy::expect_used,
                    reason = "want_write は command が Some のときだけ真になり、poll_socket は want_write が真のときだけ WriteReady を返す"
                )]
                let (channel, subscribe) = command.expect("want_write implies a command");
                if pacer.take().is_err() {
                    return None;
                }
                let frame = Message::text(channel.command(subscribe));
                if let Err(e) = conn.stream.start_send_unpin(frame) {
                    let kind = ws::error_kind(&e, &inner.config.limits);
                    return Some(Ok(self.drop_connection(live_failure(C::API, kind))));
                }
                let key = channel.key();
                let written = if subscribe {
                    sent.retries.remove(&key);
                    let replaced = sent.subscribed.insert(key, channel.clone()).is_some();
                    Written::Subscribe { channel, replaced }
                } else {
                    sent.subscribed.remove(&key);
                    sent.retries.remove(&key);
                    Written::Unsubscribe(channel)
                };
                // あきらめている間も、このコマンドを `last` に残す。`last` の
                // 書き込み時刻が、帰属を再開してよいかを決める窓の起点になる。
                // 拒否は再開までどのコマンドにも結び付かない。
                sent.last = Some((Instant::now(), written));
                sent.read_after_deadline = 0;
                sent.probed_deadline = false;
                conn.flush_since.get_or_insert_with(Instant::now);
                None
            }
            Step::Message(_) | Step::SlotReached | Step::RejectionWindowDue => None,
            Step::RejectionWindowClosed => {
                // 空のソケットと `last` の窓の期限が揃った。あきらめていた
                // 帰属もここで再開する。それまでに読んだ拒否は、どのコマンドの
                // ものか決められないので `None` のままにしておく。
                sent.last = None;
                sent.attribution_abandoned = false;
                sent.read_after_deadline = 0;
                sent.probed_deadline = false;
                sent.retried.clear();
                None
            }
            Step::Idle => Some(Ok(self.drop_connection(live_failure(
                C::API,
                TransportErrorKind::Timeout(Phase::WsIdle),
            )))),
            Step::WriteTimedOut => match conn.flush_since {
                Some(since) if since + timeouts.ws_write.get() <= Instant::now() => Some(Ok(self
                    .drop_connection(live_failure(
                        C::API,
                        TransportErrorKind::Timeout(Phase::WsWrite),
                    )))),
                _ => None,
            },
            Step::ExtendDue => {
                self.auth.start_extend();
                None
            }
            Step::Extended(sent_at, result) => {
                self.auth.extended(sent_at, &result);
                None
            }
        }
    }

    /// 1 回の接続試行を自己完結した future として始める。`State::Dialing` に
    /// 保存でき、キャンセルの後に再開できる。
    ///
    /// Private: 生きているリースがあればそのトークンで接続する。なければ
    /// `POST ws-auth` で取得し、新しいリースを結果に載せる。ハンドシェイクが
    /// トークンを拒んだら (401/403/404):
    /// - この試行で発行したトークンなら、取引所が発行したばかりのものを
    ///   拒んでいる。設定の問題なので `DELETE` してから `Terminal`
    ///   (`Protocol(WsHandshake)`) で終える。`DELETE` の失敗は報告する
    ///   エラーを変えない。トークンは 60 分以内に失効する。
    /// - 再利用したトークンなら `ReusedTokenRejected`。セッションがリースを
    ///   捨て、次の試行が取り直す。
    fn dial(&mut self) -> DialFuture {
        let inner = Arc::clone(&self.inner);
        match &self.auth {
            Auth::Public => Box::pin(async move {
                let result = ws::connect(
                    &inner.connector,
                    inner.endpoints.public_ws.base(),
                    OPERATION,
                    &inner.config.timeouts,
                    &inner.config.limits,
                )
                .await
                .map_err(|e| dial_failure(ApiGroup::PublicWs, e));
                Dialed {
                    result,
                    token: TokenSource::None,
                }
            }),
            Auth::Private {
                client,
                lease,
                schedule,
                ..
            } => {
                let client = client.clone();
                let schedule = *schedule;
                let reused = lease
                    .as_ref()
                    .filter(|l| l.is_live(Instant::now()))
                    .map(TokenLease::path);
                Box::pin(async move {
                    let (path, source) = if let Some(path) = reused {
                        (path, TokenSource::Reused)
                    } else {
                        let sent_at = Instant::now();
                        match client.acquire_ws_token().await {
                            Ok(token) => {
                                let lease = TokenLease::new(token, sent_at, schedule);
                                (lease.path(), TokenSource::Fresh(lease))
                            }
                            Err(e) => {
                                return Dialed {
                                    result: Err(acquire_failure(e)),
                                    token: TokenSource::None,
                                };
                            }
                        }
                    };
                    let result = ws::connect(
                        &inner.connector,
                        inner.endpoints.private_ws.base(),
                        &path,
                        &inner.config.timeouts,
                        &inner.config.limits,
                    )
                    .await;
                    match (result, source) {
                        (Ok(stream), token) => Dialed {
                            result: Ok(stream),
                            token,
                        },
                        (
                            Err(WsConnectError::Rejected { http_status }),
                            TokenSource::Fresh(lease),
                        ) if rejects_token(http_status) => {
                            let _ = client.delete_ws_token(lease.token).await;
                            Dialed {
                                result: Err(DialFailure::Terminal(protocol_error(
                                    ApiGroup::PrivateWs,
                                    ProtocolErrorKind::WsHandshake { http_status },
                                ))),
                                token: TokenSource::None,
                            }
                        }
                        (Err(WsConnectError::Rejected { http_status }), token)
                            if rejects_token(http_status) =>
                        {
                            Dialed {
                                result: Err(DialFailure::ReusedTokenRejected { http_status }),
                                token,
                            }
                        }
                        (Err(e), token) => Dialed {
                            result: Err(dial_failure(ApiGroup::PrivateWs, e)),
                            token,
                        },
                    }
                })
            }
        }
    }

    /// close フレーム、エコーの有界な待機、Private フィードでは `DELETE ws-auth`。
    /// 進行中の接続試行は待ってから閉じる。試行の中で発行されたトークンを
    /// 取り戻して削除するためである。
    pub(crate) async fn close(mut self) -> Result<()> {
        let timeouts = self.inner.config.timeouts;
        let stream = match std::mem::replace(&mut self.state, State::Closed) {
            State::Connected(conn, _) => Some(conn.stream),
            State::Dialing(fut) => {
                let dialed = fut.await;
                self.auth.adopt(dialed.token);
                dialed.result.ok()
            }
            State::Idle | State::Waiting { .. } | State::Closed => None,
        };
        let mut outcome = Ok(());
        if let Some(mut stream) = stream {
            outcome = close_handshake(
                &mut stream,
                timeouts.ws_write.get(),
                &self.inner.config.limits,
            )
            .await
            .map_err(|kind| Error::Transport(transport_error(C::API, kind, Delivery::MaybeSent)));
        }
        if let Auth::Private {
            client,
            lease: Some(lease),
            ..
        } = self.auth
            && lease.is_live(Instant::now())
        {
            let deleted = client.delete_ws_token(lease.token).await;
            outcome = outcome.and(deleted);
        }
        outcome
    }
}

/// close フレームを送り、サーバのエコーまで (または `bound` まで) 読む。
async fn close_handshake(
    stream: &mut WsStream,
    bound: Duration,
    limits: &Limits,
) -> core::result::Result<(), TransportErrorKind> {
    let handshake = async {
        stream.close(None).await?;
        while let Some(item) = stream.next().await {
            item?;
        }
        Ok(())
    };
    match tokio::time::timeout(bound, handshake).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(ws::error_kind(&e, limits)),
        Err(_elapsed) => Err(TransportErrorKind::Timeout(Phase::WsWrite)),
    }
}

impl Session<PublicChannel> {
    pub(crate) fn public(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            auth: Auth::Public,
            desired: BTreeMap::new(),
            state: State::Idle,
            attempt: 0,
        }
    }
}

impl Session<PrivateChannel> {
    pub(crate) fn private(client: Client<Authenticated>) -> Self {
        Self::private_with(client, LeaseSchedule::default())
    }

    pub(crate) fn private_with(client: Client<Authenticated>, schedule: LeaseSchedule) -> Self {
        let inner = Arc::clone(&client.inner);
        Self {
            inner,
            auth: Auth::Private {
                client,
                lease: None,
                schedule,
                extend: None,
            },
            desired: BTreeMap::new(),
            state: State::Idle,
            attempt: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Symbol;

    #[test]
    fn the_first_difference_is_a_missing_subscribe_then_a_stale_unsubscribe() {
        let mut desired = BTreeMap::new();
        let mut sent = BTreeMap::new();
        let mut retries = BTreeSet::new();
        assert!(next_command::<PublicChannel>(&desired, &sent, &retries).is_none());

        let ticker = PublicChannel::Ticker(Symbol::BTC);
        desired.insert(ticker.key(), ticker.clone());
        assert_eq!(
            next_command::<PublicChannel>(&desired, &sent, &retries),
            Some((ticker.clone(), true))
        );
        sent.insert(ticker.key(), ticker.clone());
        assert!(next_command::<PublicChannel>(&desired, &sent, &retries).is_none());

        let replaced = PublicChannel::Trades {
            symbol: Symbol::BTC,
            taker_only: true,
        };
        sent.insert(
            replaced.key(),
            PublicChannel::Trades {
                symbol: Symbol::BTC,
                taker_only: false,
            },
        );
        desired.insert(replaced.key(), replaced.clone());
        assert_eq!(
            next_command::<PublicChannel>(&desired, &sent, &retries),
            Some((replaced.clone(), true)),
            "a changed option is re-sent as a subscribe"
        );
        sent.insert(replaced.key(), replaced);

        retries.insert(ticker.key());
        assert_eq!(
            next_command::<PublicChannel>(&desired, &sent, &retries),
            Some((ticker.clone(), true)),
            "an anonymous rejection writes the subscribe again"
        );
        retries.remove(&ticker.key());

        desired.remove(&ticker.key());
        assert_eq!(
            next_command::<PublicChannel>(&desired, &sent, &retries),
            Some((ticker, false))
        );
    }

    /// 帰属をあきらめている間は、未読の拒否が前のコマンドのものかもしれない。
    /// 拒否はどのコマンドにも結び付かない。最後に書いた subscribe がまだ
    /// 望まれているなら、書き直しの対象にして差分に 1 度だけ書かせる。購読が
    /// 黙って欠けたまま残らないようにするためである (issue #23)。送った集合は
    /// 残す: 呼び出し側が unsubscribe を呼んだときに、そのコマンドを書ける
    /// 必要がある。
    #[test]
    fn an_abandoned_attribution_names_no_channel_and_schedules_one_rewrite() {
        let ticker = PublicChannel::Ticker(Symbol::ETH);
        let mut desired = BTreeMap::new();
        desired.insert(ticker.key(), ticker.clone());
        let mut sent = Sent::<PublicChannel>::empty();
        sent.subscribed.insert(ticker.key(), ticker.clone());
        sent.last = Some((
            Instant::now(),
            Written::Subscribe {
                channel: ticker.clone(),
                replaced: false,
            },
        ));
        sent.attribution_abandoned = true;

        assert_eq!(sent.rejected(&mut desired), None);
        assert!(
            desired.contains_key(&ticker.key()),
            "the caller's intent stays, so the difference writes the subscribe again"
        );
        assert!(
            sent.subscribed.contains_key(&ticker.key()),
            "the sent state stays, so a later unsubscribe can be written"
        );
        assert!(
            sent.retries.contains(&ticker.key()),
            "the difference rewrites this subscribe once"
        );
        assert!(
            sent.last.is_some(),
            "the write time stays as the anchor for resuming attribution"
        );
    }

    /// 同じチャネルが 2 度目の匿名の拒否で返ってきたら、望みも外して自動の
    /// 書き直しをやめる。チャネル自体が無効なとき、1.05 秒ごとに同じコマンドを
    /// 送り続けて IP ごとの枠を使い切らないためである (issue #23 のレビュー
    /// 指摘)。送った集合は残すので、差分は unsubscribe を書ける。
    #[test]
    fn a_second_anonymous_rejection_stops_rewriting_the_channel() {
        let ticker = PublicChannel::Ticker(Symbol::ETH);
        let mut desired = BTreeMap::new();
        desired.insert(ticker.key(), ticker.clone());
        let mut sent = Sent::<PublicChannel>::empty();
        sent.subscribed.insert(ticker.key(), ticker.clone());
        sent.last = Some((
            Instant::now(),
            Written::Subscribe {
                channel: ticker.clone(),
                replaced: false,
            },
        ));
        sent.attribution_abandoned = true;

        assert_eq!(sent.rejected(&mut desired), None);
        assert!(
            sent.retried.contains(&ticker.key()) && sent.retries.contains(&ticker.key()),
            "the first anonymous rejection allows one rewrite"
        );

        // 差分が書き直した体で、同じ拒否がもう一度返る。
        sent.retries.remove(&ticker.key());
        assert_eq!(sent.rejected(&mut desired), None);
        assert!(
            !desired.contains_key(&ticker.key()),
            "the second anonymous rejection must not be written again"
        );
        assert!(
            !sent.retries.contains(&ticker.key()) && sent.subscribed.contains_key(&ticker.key()),
            "the sent state stays so the difference writes one unsubscribe"
        );
    }

    /// 呼び出し側が購読を呼び直したら、匿名の拒否による書き直しの印を消し、
    /// そのチャネルをもう一度だけ書けるようにする。
    #[test]
    fn a_caller_subscribe_forgets_the_rewrite_and_schedules_another() {
        let ticker = PublicChannel::Ticker(Symbol::ETH);
        let mut sent = Sent::<PublicChannel>::empty();
        sent.retried.insert(ticker.key());

        sent.resubscribed(&ticker.key());

        assert!(!sent.retried.contains(&ticker.key()));
        assert!(
            sent.retries.contains(&ticker.key()),
            "an explicit subscribe writes the command again"
        );
    }

    /// 匿名の拒否による書き直しの印が付いていないチャネルを購読し直しても、
    /// 差分に余計なコマンドを足さない。二重の subscribe は何もしない。
    #[test]
    fn a_plain_repeat_subscribe_does_not_schedule_a_rewrite() {
        let ticker = PublicChannel::Ticker(Symbol::ETH);
        let mut sent = Sent::<PublicChannel>::empty();

        sent.resubscribed(&ticker.key());

        assert!(sent.retries.is_empty() && sent.retried.is_empty());
    }

    /// 望み直していないチャネルは、送った集合に残す。拒否がその subscribe の
    /// ものだったとしても、送るべき unsubscribe を失わないためである。
    #[test]
    fn an_abandoned_attribution_keeps_a_channel_the_caller_no_longer_wants() {
        let ticker = PublicChannel::Ticker(Symbol::ETH);
        let mut desired = BTreeMap::new();
        let mut sent = Sent::<PublicChannel>::empty();
        sent.subscribed.insert(ticker.key(), ticker.clone());
        sent.last = Some((
            Instant::now(),
            Written::Subscribe {
                channel: ticker.clone(),
                replaced: false,
            },
        ));
        sent.attribution_abandoned = true;

        assert_eq!(sent.rejected(&mut desired), None);
        assert!(
            sent.subscribed.contains_key(&ticker.key()),
            "an unsubscribe still has to be written for a channel the caller dropped"
        );
        assert!(
            sent.retries.is_empty() && sent.retried.is_empty(),
            "a channel the caller no longer wants is not written again"
        );
    }

    #[test]
    fn a_token_is_rejected_only_by_the_documented_statuses() {
        assert!([401, 403, 404].into_iter().all(rejects_token));
        assert!(![400, 429, 500, 503].into_iter().any(rejects_token));
        assert!(matches!(
            dial_failure(
                ApiGroup::PrivateWs,
                WsConnectError::Rejected { http_status: 503 }
            ),
            DialFailure::Retry(DisconnectReason::Maintenance)
        ));
        assert!(matches!(
            dial_failure(
                ApiGroup::PublicWs,
                WsConnectError::Rejected { http_status: 404 }
            ),
            DialFailure::Retry(DisconnectReason::HandshakeRejected { http_status: 404 })
        ));
    }

    /// 本番の Public WebSocket に不正な subscribe を 1 つ送り、生の返答を
    /// `tests/fixtures/live/ws_public_error.json` に保存する。読み取り専用。
    /// 直前の 1 秒間に他のコマンドを送っていないこと (別の呼び出しで走らせる)。
    #[tokio::test]
    #[ignore = "live"]
    async fn probe_the_public_error_shape() {
        use crate::client::Config;
        use crate::endpoint::Endpoints;
        use crate::transport::Connector;

        let endpoints = Endpoints::production();
        let config = Config::default();
        let mut stream = ws::connect(
            &Connector::new(),
            endpoints.public_ws.base(),
            OPERATION,
            &config.timeouts,
            &config.limits,
        )
        .await
        .expect("handshake");
        stream
            .send(Message::text(
                r#"{"command":"subscribe","channel":"nonexistent","symbol":"BTC"}"#,
            ))
            .await
            .expect("send");
        let reply = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(item) = stream.next().await {
                if let Message::Text(text) = item.expect("read") {
                    return Some(text.to_string());
                }
            }
            None
        })
        .await;
        let _ = stream.close(None).await;
        match reply {
            Ok(Some(text)) => {
                let path = format!(
                    "{}/tests/fixtures/live/ws_public_error.json",
                    env!("CARGO_MANIFEST_DIR")
                );
                std::fs::write(&path, format!("{text}\n")).expect("write fixture");
                println!("reply: {text}");
            }
            Ok(None) => println!("the server closed without a text reply"),
            Err(_) => println!("no reply within 5 seconds"),
        }
    }
}
