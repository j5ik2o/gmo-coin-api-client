//! 本番の Public WebSocket に対するスモークテスト。ネットワークが必要なので
//! `cargo test --test ws_live -- --ignored` で明示的に走らせる。
//!
//! 読み取り専用で、コマンドは既定のペーサ (1 秒に 1 回) で送る。
//!
//! `ticker` チャネルは約定があったときにしか配信されないようで、深夜の
//! BTC では数十秒空くことがある (独立した生の WebSocket クライアントでも
//! 最初の配信まで 17 秒かかった)。待ち時間はそれに合わせて長い。

use std::time::Duration;

use gmo_coin_api_client::ws::{PublicChannel, PublicEvent};
use gmo_coin_api_client::{Client, Symbol};

#[tokio::test]
#[ignore = "live"]
async fn the_public_ticker_feed_delivers_at_least_one_event_from_production() {
    let mut feed = Client::production().public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC)).unwrap();

    let started = std::time::Instant::now();
    let ticker = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            match feed.next().await.expect("the feed stays open") {
                PublicEvent::Ticker(t) => return t,
                other => println!("event before the first ticker: {other:?}"),
            }
        }
    })
    .await
    .expect("a ticker within 180 seconds");
    println!(
        "ticker BTC after {:?}: bid {} ask {} last {} at {}",
        started.elapsed(),
        ticker.bid,
        ticker.ask,
        ticker.last,
        ticker.timestamp
    );
    assert_eq!(ticker.symbol, Symbol::BTC);
    assert!(ticker.ask >= ticker.bid, "the quote is not crossed");

    feed.close().await.expect("an error-free close");
}
