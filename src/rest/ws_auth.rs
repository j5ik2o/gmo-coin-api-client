//! `ws-auth`: the private WebSocket token lifecycle. Crate-private.
//!
//! These three calls are not public. The private feed owns the token from
//! issue to delete, so there is one way to use the private WebSocket and
//! the token never crosses the public API (it is a bearer credential: anyone
//! holding it reads the account's order and execution stream).

use serde::Serialize;

use super::{Call, endpoint};
use crate::client::{Authenticated, Client};
use crate::error::Result;
use crate::secret::AccessToken;

#[derive(Serialize)]
struct TokenBody<'a> {
    token: &'a str,
}

impl Client<Authenticated> {
    /// `POST /private/v1/ws-auth`. Valid for 60 minutes.
    ///
    /// Issuing a sixth token deletes the account's token closest to expiry,
    /// which may belong to another process using the same key. So the feed
    /// issues only when it holds no live token, and reuses one across
    /// reconnects.
    ///
    /// # Postconditions
    /// The token passed [`AccessToken::parse`] (URL-path safe, bounded).
    pub(crate) async fn acquire_ws_token(&self) -> Result<AccessToken> {
        let answer = self
            .private(Call::new(endpoint!(POST "/v1/ws-auth")))
            .await?;
        // TODO data is a JSON string. Decode it as a borrowed str into a
        // Zeroizing buffer, then AccessToken::parse; on failure return
        // answer.protocol(Shape { .. }) without the serde message (it would
        // quote the token).
        let _ = answer;
        todo!()
    }

    /// `PUT /private/v1/ws-auth {"token"}`: resets the expiry to 60 minutes.
    /// Idempotent, so a cancelled or repeated call is harmless.
    pub(crate) async fn extend_ws_token(&self, token: &AccessToken) -> Result<()> {
        let call = Call::new(endpoint!(PUT "/v1/ws-auth")).json(&TokenBody {
            token: token.reveal(),
        });
        self.private(call).await?.empty()
    }

    /// `DELETE /private/v1/ws-auth {"token"}`. Consumes the token.
    pub(crate) async fn delete_ws_token(&self, token: AccessToken) -> Result<()> {
        let call = Call::new(endpoint!(DELETE "/v1/ws-auth")).json(&TokenBody {
            token: token.reveal(),
        });
        self.private(call).await?.empty()
    }
}
