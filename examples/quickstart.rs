//! README の「最初の呼び出し」をそのまま動かす例。
//!
//! ```sh
//! cargo run --example quickstart
//! ```
//!
//! 本番の Public REST に接続する。API キーは要らない。

#![expect(
    clippy::print_stdout,
    reason = "例は取得した値を表示するために標準出力を使う"
)]

use gmo_coin_api_client::{Client, Symbol};

#[tokio::main]
async fn main() -> gmo_coin_api_client::Result<()> {
    let client = Client::production();
    let ticker = client.ticker(Symbol::BTC).await?;
    println!(
        "BTC 買 {} / 売 {} / 最終 {}",
        ticker.bid, ticker.ask, ticker.last
    );
    Ok(())
}
