//! Private フィードのトークンのリース: 取得した時刻から延長と失効を刻み、
//! 延長 (`PUT ws-auth`) を future として進める。
//!
//! # 不変条件
//! `TokenLease` は `extend_at <= expires_at` を保ち、どちらも取得または
//! 延長の要求を「送った」時刻から計算するので過大にならない。

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use zeroize::Zeroizing;

use super::session::OPERATION;
use crate::client::{Authenticated, Client};
use crate::error::Result;
use crate::secret::AccessToken;

/// 進行中の `PUT ws-auth`。送った時刻と結果を返す。
pub(super) type ExtendFuture = Pin<Box<dyn Future<Output = (Instant, Result<()>)> + Send>>;

/// この試行で使ったトークンの出所。ハンドシェイクがトークンを拒んだとき、
/// 次に何をするかはこれで決まる。
pub(super) enum TokenSource {
    /// Public、または取得に失敗した、または試行の中で削除した。
    None,
    /// 前の試行で発行したリースを再利用した。
    Reused,
    /// この試行で発行した。接続の成否にかかわらずセッションが引き取る。
    Fresh(TokenLease),
}

pub(super) enum Auth {
    Public,
    /// トークンは接続より長生きする: 生きている間は再接続が再利用するので、
    /// 発行 (別プロセスのトークンを追い出し得る) はまれにしか起きない。
    Private {
        client: Client<Authenticated>,
        lease: Option<TokenLease>,
        schedule: LeaseSchedule,
        /// 進行中の延長。接続が落ちたら捨て、スケジュールが改めて起動する。
        extend: Option<ExtendFuture>,
    },
}

impl Auth {
    /// 試行の中で発行されたトークンを引き取る。接続の成否は問わない。
    pub(super) fn adopt(&mut self, token: TokenSource) {
        if let (Auth::Private { lease, .. }, TokenSource::Fresh(acquired)) = (self, token) {
            *lease = Some(acquired);
        }
    }

    /// 次の延長の期限。延長が進行中、`extend_at == expires_at`
    /// (もう延長しない)、または Public なら `None`。
    pub(super) fn extend_due_at(&self) -> Option<Instant> {
        match self {
            Auth::Private {
                lease: Some(lease),
                extend: None,
                ..
            } if lease.extend_at < lease.expires_at => Some(lease.extend_at),
            _ => None,
        }
    }

    pub(super) fn extend_in_flight(&mut self) -> Option<&mut ExtendFuture> {
        match self {
            Auth::Private { extend, .. } => extend.as_mut(),
            Auth::Public => None,
        }
    }

    /// 進行中の延長を捨てる。接続が落ちたときに呼び、スケジュールが改めて
    /// 始める。
    pub(super) fn abandon_extend(&mut self) {
        if let Auth::Private { extend, .. } = self {
            *extend = None;
        }
    }

    /// `PUT ws-auth` を future として始め、`Auth::Private::extend` に置く。
    /// 冪等: キャンセルされた延長はスケジュールが改めて始める。
    pub(super) fn start_extend(&mut self) {
        let Auth::Private {
            client,
            lease: Some(lease),
            extend,
            ..
        } = self
        else {
            return;
        };
        let client = client.clone();
        let token = lease.token_copy();
        let sent_at = Instant::now();
        *extend = Some(Box::pin(async move {
            (sent_at, client.extend_ws_token(&token).await)
        }));
    }

    /// 延長の結果。成功なら `extend_at` と `expires_at` は送った時刻から
    /// 45/60 分後に動く。失敗ならリースは保ち、1 分後 (ただし `expires_at`
    /// まで) に再試行する。その後は次の接続が新しいトークンを取得する。
    pub(super) fn extended(&mut self, sent_at: Instant, result: &Result<()>) {
        let Auth::Private {
            lease: Some(lease),
            schedule,
            extend,
            ..
        } = self
        else {
            return;
        };
        *extend = None;
        match result {
            Ok(()) => {
                lease.extend_at = sent_at + schedule.extend_after;
                lease.expires_at = sent_at + schedule.expires_after;
            }
            Err(_) => {
                lease.extend_at = (Instant::now() + schedule.retry_after).min(lease.expires_at);
            }
        }
    }
}

/// # 不変条件
/// `extend_at <= expires_at`。両方とも取得または延長の要求を「送った」時刻
/// から計算するので、決して過大にならない。`extend_at == expires_at` は
/// 「もう延長しない」を意味し、次の接続でトークンを取り直す。
///
/// # 残存リスク
/// トークンの所有者はセッションだけなので、`close` は常に削除できる。
/// 接続試行は接続先パス (ゼロ埋めされる `String`) を、延長は `PUT` の本文の
/// ための複製 (`AccessToken`、ゼロ埋めされる) を持つ。
pub(super) struct TokenLease {
    pub(super) token: AccessToken,
    extend_at: Instant,
    expires_at: Instant,
}

/// トークンの寿命の刻み。既定は取引所の 60 分に対して 45 分で延長、失敗なら
/// 1 分後に再試行。テストは短くする。
#[derive(Clone, Copy, Debug)]
#[expect(
    clippy::struct_field_names,
    reason = "どの接尾辞も「送った時刻から何分後か」という意味を持つので、そろえて after と呼ぶ"
)]
pub(crate) struct LeaseSchedule {
    pub(crate) extend_after: Duration,
    pub(crate) retry_after: Duration,
    pub(crate) expires_after: Duration,
}

impl Default for LeaseSchedule {
    fn default() -> Self {
        Self {
            extend_after: Duration::from_mins(45),
            retry_after: Duration::from_secs(60),
            expires_after: Duration::from_hours(1),
        }
    }
}

impl TokenLease {
    pub(super) fn new(token: AccessToken, sent_at: Instant, schedule: LeaseSchedule) -> Self {
        Self {
            token,
            extend_at: sent_at + schedule.extend_after,
            expires_at: sent_at + schedule.expires_after,
        }
    }

    pub(super) fn is_live(&self, now: Instant) -> bool {
        now < self.expires_at
    }

    /// Private WebSocket の接続先パス。
    pub(super) fn path(&self) -> Zeroizing<String> {
        Zeroizing::new(format!("{OPERATION}/{}", self.token.reveal()))
    }

    /// 延長の future に渡す複製。
    #[expect(
        clippy::expect_used,
        reason = "複製元は AccessToken::parse を通った値なので、同じ文字列は必ず再びパースできる"
    )]
    fn token_copy(&self) -> AccessToken {
        AccessToken::parse(self.token.reveal()).expect("a copy of a parsed token")
    }
}
