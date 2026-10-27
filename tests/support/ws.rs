//! The mock's WebSocket server side: handshake, unmasked server frames,
//! unmasking client frames, ping/pong, and a control handle for tests.

use std::time::{Duration, Instant};

use serde_json::Value;

/// Open connections and what they were sent. Lives in the mock's `State`.
#[derive(Default)]
pub struct Hub {
    /// Every command received, with arrival time, for pacing assertions.
    pub commands: Vec<(Instant, Api, Value)>,
    /// Connection ids per API; pushes go to every connection of that API.
    pub public_conns: Vec<u64>,
    pub private_conns: Vec<u64>,
    pub pongs: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Api {
    Public,
    Private,
}

/// Test-side control over the mock's WebSocket endpoints.
pub struct WsControl {
    // TODO Arc<Mutex<State>> plus a tokio::sync-free notifier: tests poll
    // with a short sleep loop bounded by a deadline (no extra crate).
}

impl WsControl {
    /// Waits (up to `within`) until a subscribe for `channel` (and `symbol`,
    /// when given) arrives on the current connection, and returns it.
    pub async fn subscribed(&self, api: Api, channel: &str, symbol: Option<&str>, within: Duration) -> Value {
        let _ = (api, channel, symbol, within);
        todo!()
    }

    /// Sends a text frame to every open connection of `api`.
    pub fn push(&self, api: Api, json: &str) {
        let _ = (api, json);
        todo!()
    }

    /// Closes every open connection of `api` without a close frame, the way
    /// a network drop looks to the client.
    pub fn drop_connections(&self, api: Api) {
        let _ = api;
        todo!()
    }

    /// Sends a ping and returns whether the pong came back within `within`.
    pub async fn ping(&self, api: Api, within: Duration) -> bool {
        let _ = (api, within);
        todo!()
    }

    /// Every command received so far, in arrival order.
    pub fn commands(&self) -> Vec<(Instant, Api, Value)> {
        todo!()
    }
}

/// Serves one upgraded connection.
///
/// Handshake: `Sec-WebSocket-Accept = base64(SHA-1(key + GUID))` with
/// `ring::digest::SHA1_FOR_LEGACY_USE_ONLY`. `/ws/private/v1/{token}` is
/// accepted only for a live token in `State::tokens`, else HTTP 401, which
/// lets tests exercise the client's re-acquire path. A test can also force
/// 503 to simulate maintenance.
///
/// Frames: client frames must be masked (unmasked is a test failure);
/// server frames are never masked.
pub async fn serve() {
    todo!()
}
