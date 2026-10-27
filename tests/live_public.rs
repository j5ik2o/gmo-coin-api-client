//! 本番の Public API に対するスモークテスト。ネットワークが必要なので
//! `cargo test --test live_public -- --ignored` で明示的に走らせる。
//!
//! Public API は短い間隔の呼び出しを ERR-5003 で拒否するので、各呼び出しの
//! 間に 1.5 秒置く。

use std::time::Duration;

use gmo_coin_api_client::{BusinessDate, Client, IntradayInterval, KlineSpan, PageRequest, Symbol};

#[tokio::test]
#[ignore = "live"]
async fn public_endpoints_answer_from_production() {
    let client = Client::production();
    let pause = || tokio::time::sleep(Duration::from_millis(1500));

    let status = client.status().await.expect("status");
    println!("status: {status}");
    pause().await;

    let ticker = client.ticker(Symbol::BTC).await.expect("ticker BTC");
    println!(
        "ticker BTC: bid {} ask {} last {} volume {} at {}",
        ticker.bid, ticker.ask, ticker.last, ticker.volume, ticker.timestamp
    );
    assert_eq!(ticker.symbol, Symbol::BTC);
    pause().await;

    let book = client
        .order_book(Symbol::BTC)
        .await
        .expect("orderbooks BTC");
    println!(
        "orderbooks BTC: {} asks (best {}), {} bids (best {})",
        book.asks.len(),
        book.asks[0].price,
        book.bids.len(),
        book.bids[0].price
    );
    assert!(
        book.asks[0].price >= book.bids[0].price,
        "the book is not crossed"
    );
    pause().await;

    let trades = client
        .trades(Symbol::BTC, PageRequest::new(1, 5).unwrap())
        .await
        .expect("trades BTC");
    println!(
        "trades BTC: {} items, pagination {:?}",
        trades.items().len(),
        trades.pagination()
    );
    assert!(trades.items().len() <= 5);
    pause().await;

    let date = BusinessDate::new(2025, 10, 1).unwrap();
    let span = KlineSpan::Day {
        interval: IntradayInterval::Hour1,
        date,
    };
    let klines = client
        .klines(Symbol::BTC, span)
        .await
        .expect("klines BTC 1hour 20251001");
    println!(
        "klines BTC 1hour 20251001: {} candles, first open {} at {}",
        klines.len(),
        klines[0].open,
        klines[0].open_time
    );
    assert!(
        klines.windows(2).all(|w| w[0].open_time < w[1].open_time),
        "oldest first"
    );
    pause().await;

    let rules = client.symbols().await.expect("symbols");
    println!(
        "symbols: {} rules, e.g. {} tick {} step {}",
        rules.len(),
        rules[0].symbol,
        rules[0].tick_size,
        rules[0].size_step
    );
    assert!(rules.iter().any(|r| r.symbol == Symbol::BTC));
}
