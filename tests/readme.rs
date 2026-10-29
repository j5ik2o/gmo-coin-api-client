//! README の「注文を出す」が、送り直しの手順を写さずに、crate の
//! `Client::place_order_or_recover` に委ねていることを確かめる。
//!
//! 手順はこのメソッドの実装だけにあり、`tests/rest_private_trading.rs` の
//! `the_readme_recipe_*` は同じメソッドをモック取引所に対して動かす。
//! README に手順の写しが戻ると、このテストが落ちる。

#![expect(
    clippy::expect_used,
    reason = "テストの補助関数は #[test] 関数ではないため clippy.toml の allow-expect-in-tests が届かない。失敗はそのままテストの失敗になる"
)]

const README: &str = include_str!("../README.md");

/// 見出しの後ろにある、最初の Rust のコードブロックの中身。
fn rust_block_after(heading: &str) -> &'static str {
    let (_, section) = README.split_once(heading).expect("README に見出しがある");
    let (_, rest) = section
        .split_once("```rust")
        .expect("見出しの後に Rust のコードブロックがある");
    rest.split_once("```")
        .expect("コードブロックが閉じている")
        .0
}

#[test]
fn the_readme_order_recipe_calls_the_shared_helper_instead_of_copying_it() {
    let code = rust_block_after("### 注文を出す");

    assert!(
        code.contains("place_order_or_recover"),
        "README は crate の手順を呼ぶ: {code}"
    );
    assert!(
        !code.contains("outcome_unknown"),
        "送り直しの手順を README に写さない: {code}"
    );
}
