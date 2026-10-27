//! `ws-auth`: Private WebSocket トークンのライフサイクル。クレート内部。
//!
//! この 3 つの呼び出しは公開しない。Private フィードが発行から削除まで
//! トークンを所有するので、Private WebSocket の使い方は 1 つだけになり、
//! トークンが公開 API を越えることはない (トークンはベアラ資格情報であり、
//! 持っていれば誰でも口座の注文・約定ストリームを読める)。

use serde::Serialize;
use zeroize::Zeroizing;

use super::{Answer, Call, endpoint};
use crate::client::{Authenticated, Client};
use crate::error::{ProtocolErrorKind, Result};
use crate::secret::AccessToken;

#[derive(Serialize)]
struct TokenBody<'a> {
    token: &'a str,
}

/// `POST /ws-auth` にはパラメータがない。公式サンプルは空オブジェクト `{}` を
/// 署名して送るので、ここでもそれに合わせる。
#[derive(Serialize)]
struct EmptyBody {}

impl Client<Authenticated> {
    /// `POST /private/v1/ws-auth`。60 分間有効。
    ///
    /// 6 つ目のトークンを発行すると、口座のトークンのうち有効期限が最も近い
    /// ものが削除される。それは同じキーを使う別のプロセスのものかもしれない。
    /// したがってフィードは、生きているトークンを持たないときだけ発行し、
    /// 再接続をまたいで再利用する。
    ///
    /// # 事後条件
    /// トークンは [`AccessToken::parse`] を通過している (URL パスに安全、有界)。
    /// 応答本文と `data` の生テキストはゼロ埋めされるバッファにだけ存在した。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。`data` が文字列でない、または
    /// トークンの字句規則に反するときは `Protocol(Shape)` (serde のメッセージは
    /// 捨てる。トークンを引用するからである)。
    pub(crate) async fn acquire_ws_token(&self) -> Result<AccessToken> {
        let call = Call::new(endpoint!(POST "/v1/ws-auth")).json(&EmptyBody {});
        token_from(&self.private(call).await?)
    }

    /// `PUT /private/v1/ws-auth {"token"}`: 有効期限を 60 分にリセットする。
    /// 冪等なので、キャンセルや繰り返しは無害である。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    pub(crate) async fn extend_ws_token(&self, token: &AccessToken) -> Result<()> {
        let call = Call::new(endpoint!(PUT "/v1/ws-auth")).json(&TokenBody {
            token: token.reveal(),
        });
        self.private(call).await?.empty()
    }

    /// `DELETE /private/v1/ws-auth {"token"}`。トークンを消費する。
    ///
    /// # エラー
    /// [REST 共通の集合](crate::rest#エラー)。
    pub(crate) async fn delete_ws_token(&self, token: AccessToken) -> Result<()> {
        let call = Call::new(endpoint!(DELETE "/v1/ws-auth")).json(&TokenBody {
            token: token.reveal(),
        });
        self.private(call).await?.empty()
    }
}

/// `data` の文字列を [`AccessToken`] にする。
///
/// # エラー
/// `data` がない、文字列でない、または字句規則に反するとき `Protocol`
/// (`MissingData` または `Shape`)。エラーはトークンの値を持たない。
fn token_from(answer: &Answer) -> Result<AccessToken> {
    let token = Zeroizing::new(answer.data::<String>()?);
    AccessToken::parse(&token)
        .map_err(|_| answer.protocol(ProtocolErrorKind::Shape { line: 1, column: 1 }))
}

#[cfg(test)]
mod tests {
    use super::token_from;
    use crate::error::{ApiGroup, Error, ProtocolErrorKind};
    use crate::rest::envelope;

    fn answer(body: &[u8]) -> crate::rest::Answer {
        envelope::parse(ApiGroup::PrivateRest, "POST /v1/ws-auth", 200, body).unwrap()
    }

    #[test]
    fn the_three_ws_auth_fixtures_decode() {
        let post = answer(include_bytes!(
            "../../tests/fixtures/private_post_ws-auth.json"
        ));
        assert_eq!(token_from(&post).unwrap().reveal(), "xxxxxxxxxxxxxxxxxxxx");
        for fixture in [
            &include_bytes!("../../tests/fixtures/private_put_ws-auth.json")[..],
            &include_bytes!("../../tests/fixtures/private_delete_ws-auth.json")[..],
        ] {
            let answer =
                envelope::parse(ApiGroup::PrivateRest, "PUT /v1/ws-auth", 200, fixture).unwrap();
            assert!(answer.raw().is_none());
            assert!(answer.empty().is_ok());
        }
    }

    #[test]
    fn a_token_that_is_not_a_string_or_breaks_the_lexical_rule_is_a_shape_error_without_the_value()
    {
        for body in [
            &br#"{"status":0,"data":{"token":"x"}}"#[..],
            &br#"{"status":0,"data":"has/slash"}"#[..],
        ] {
            let err = token_from(&answer(body)).unwrap_err();
            match err {
                Error::Protocol(p) => {
                    assert!(matches!(p.kind(), ProtocolErrorKind::Shape { .. }));
                    let text = p.to_string();
                    assert!(!text.contains("token") && !text.contains("slash"), "{text}");
                }
                other => panic!("{other:?}"),
            }
        }
        let err = token_from(&answer(br#"{"status":0}"#)).unwrap_err();
        assert!(matches!(err, Error::Protocol(p) if *p.kind() == ProtocolErrorKind::MissingData));
    }
}
