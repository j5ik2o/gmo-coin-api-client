//! 本番の Public WebSocket に対するスモークテスト。ネットワークが必要なので
//! `cargo test --test ws_live -- --ignored` で明示的に走らせる。
//!
//! 読み取り専用で、コマンドは既定のペーサ (1 秒に 1 回) で送る。

use std::time::Duration;

use gmo_coin_api_client::ws::{PublicChannel, PublicEvent};
use gmo_coin_api_client::{Client, Symbol};

#[tokio::test]
#[ignore = "live"]
async fn the_public_ticker_feed_delivers_at_least_one_event_from_production() {
    let mut feed = Client::production().public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let ticker = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match feed.next().await.expect("the feed stays open") {
                PublicEvent::Ticker(t) => return t,
                other => println!("event before the first ticker: {other:?}"),
            }
        }
    })
    .await
    .expect("a ticker within 30 seconds");
    println!(
        "ticker BTC: bid {} ask {} last {} at {}",
        ticker.bid, ticker.ask, ticker.last, ticker.timestamp
    );
    assert_eq!(ticker.symbol, Symbol::BTC);
    assert!(ticker.ask >= ticker.bid, "the quote is not crossed");

    feed.close().await.expect("an error-free close");
}
