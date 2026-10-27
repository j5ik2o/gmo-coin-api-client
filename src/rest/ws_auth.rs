//! `ws-auth`: Private WebSocket トークンのライフサイクル。クレート内部。
//!
//! この 3 つの呼び出しは公開しない。Private フィードが発行から削除まで
//! トークンを所有するので、Private WebSocket の使い方は 1 つだけになり、
//! トークンが公開 API を越えることはない (トークンはベアラ資格情報であり、
//! 持っていれば誰でも口座の注文・約定ストリームを読める)。

use serde::Serialize;
use zeroize::Zeroizing;

use super::{Call, endpoint};
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
        let answer = self.private(call).await?;
        let Some(raw) = answer.raw() else {
            return Err(answer.protocol(ProtocolErrorKind::MissingData));
        };
        let token: Zeroizing<String> = match serde_json::from_str::<String>(raw) {
            Ok(text) => Zeroizing::new(text),
            Err(e) => return Err(answer.shape_error(&e)),
        };
        AccessToken::parse(&token)
            .map_err(|_| answer.protocol(ProtocolErrorKind::Shape { line: 1, column: 1 }))
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::error::{ApiGroup, Error, ErrorCode, ProtocolErrorKind, TransportErrorKind};
    use crate::mock::{MockExchange, Reply, SignatureCheck};
    use crate::rest::envelope;
    use crate::secret::AccessToken;
    use crate::{ApiKey, ApiSecret, Credentials};

    #[tokio::test]
    async fn the_built_in_token_store_issues_extends_and_deletes_signed_requests() {
        let mock = MockExchange::start().await;
        let client = mock.client().authenticate(MockExchange::credentials());

        let token = client.acquire_ws_token().await.expect("issued");
        client.extend_ws_token(&token).await.expect("extended");
        let text = token.reveal().to_owned();
        client.delete_ws_token(token).await.expect("deleted");

        mock.with_tokens(|t| {
            assert_eq!(t.issued, std::slice::from_ref(&text));
            assert_eq!(t.extended, std::slice::from_ref(&text));
            assert_eq!(t.deleted, std::slice::from_ref(&text));
            assert!(!t.is_live(&text));
        });
        let requests = mock.requests();
        let methods: Vec<&str> = requests.iter().map(|r| r.method.as_str()).collect();
        assert_eq!(methods, ["POST", "PUT", "DELETE"]);
        assert!(
            requests
                .iter()
                .all(|r| r.path == "/private/v1/ws-auth" && r.signature == SignatureCheck::Valid)
        );
        assert_eq!(
            requests[0].body, b"{}",
            "the official samples sign and send an empty object"
        );
        assert_eq!(
            requests[1].body,
            format!(r#"{{"token":"{text}"}}"#).as_bytes()
        );
        mock.assert_done();
    }

    #[tokio::test]
    async fn the_three_fixtures_drive_the_calls_end_to_end() {
        let mock = MockExchange::start().await;
        mock.on("POST", "/private/v1/ws-auth")
            .signed()
            .json_body(json!({}))
            .times(1)
            .fixture("private_post_ws-auth.json");
        mock.on("PUT", "/private/v1/ws-auth")
            .signed()
            .json_body(json!({"token": "xxxxxxxxxxxxxxxxxxxx"}))
            .times(1)
            .fixture("private_put_ws-auth.json");
        mock.on("DELETE", "/private/v1/ws-auth")
            .signed()
            .json_body(json!({"token": "xxxxxxxxxxxxxxxxxxxx"}))
            .times(1)
            .fixture("private_delete_ws-auth.json");
        let client = mock.client().authenticate(MockExchange::credentials());

        let token = client.acquire_ws_token().await.unwrap();
        assert_eq!(token.reveal(), "xxxxxxxxxxxxxxxxxxxx");
        client.extend_ws_token(&token).await.unwrap();
        client.delete_ws_token(token).await.unwrap();

        mock.assert_done();
    }

    #[tokio::test]
    async fn a_bad_secret_is_answered_with_err_5010_in_a_200_envelope() {
        let mock = MockExchange::start().await;
        let wrong = Credentials::new(
            ApiKey::new(crate::mock::TEST_KEY.to_owned()).unwrap(),
            ApiSecret::new("not-the-secret".to_owned()).unwrap(),
        );
        let client = mock.client().authenticate(wrong);

        let err = client
            .acquire_ws_token()
            .await
            .expect_err("signature mismatch");

        match err {
            Error::Api(api) => assert!(api.has(ErrorCode::BAD_SIGNATURE)),
            other => panic!("expected ERR-5010, got {other:?}"),
        }
        assert_eq!(mock.requests()[0].signature, SignatureCheck::Invalid);
    }

    #[tokio::test]
    async fn a_connection_closed_after_a_post_is_outcome_unknown() {
        let mock = MockExchange::start().await;
        mock.on("POST", "/private/v1/ws-auth")
            .times(1)
            .reply(Reply::CloseAfterRequest);
        let client = mock.client().authenticate(MockExchange::credentials());

        let err = client.acquire_ws_token().await.expect_err("no answer");

        assert!(err.outcome_unknown(), "{err:?}");
        assert!(
            matches!(&err, Error::Transport(t) if *t.kind() == TransportErrorKind::ClosedEarly)
        );
        mock.assert_done();
    }

    #[tokio::test]
    async fn a_connection_dropped_on_headers_is_also_outcome_unknown() {
        let mock = MockExchange::start().await;
        mock.on("POST", "/private/v1/ws-auth")
            .times(1)
            .reply(Reply::DropConnection);
        let client = mock.client().authenticate(MockExchange::credentials());

        let err = client.acquire_ws_token().await.expect_err("no answer");

        assert!(err.outcome_unknown(), "{err:?}");
        mock.assert_done();
    }

    #[tokio::test]
    async fn a_token_that_fails_its_lexical_rule_is_a_shape_error() {
        let mock = MockExchange::start().await;
        mock.on("POST", "/private/v1/ws-auth")
            .reply(Reply::Json(r#"{"status":0,"data":"has/slash"}"#.to_owned()));
        let client = mock.client().authenticate(MockExchange::credentials());

        let err = client
            .acquire_ws_token()
            .await
            .expect_err("not url-path safe");

        match err {
            Error::Protocol(p) => {
                assert!(matches!(p.kind(), ProtocolErrorKind::Shape { .. }));
                assert!(!format!("{p}").contains("slash"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_three_ws_auth_fixtures_decode() {
        let post = envelope::parse(
            ApiGroup::PrivateRest,
            "POST /v1/ws-auth",
            200,
            include_bytes!("../../tests/fixtures/private_post_ws-auth.json"),
        )
        .unwrap();
        let token: String = serde_json::from_str(post.raw().unwrap()).unwrap();
        assert_eq!(
            AccessToken::parse(&token).unwrap().reveal(),
            "xxxxxxxxxxxxxxxxxxxx"
        );
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
    fn a_non_string_token_is_a_shape_error_without_the_value() {
        let answer = envelope::parse(
            ApiGroup::PrivateRest,
            "POST /v1/ws-auth",
            200,
            br#"{"status":0,"data":{"token":"x"}}"#,
        )
        .unwrap();
        let err = answer.data::<String>().unwrap_err();
        match err {
            Error::Protocol(p) => {
                assert!(matches!(p.kind(), ProtocolErrorKind::Shape { .. }));
                assert!(!p.to_string().contains("token"));
            }
            other => panic!("{other:?}"),
        }
    }
}
