# gmo-coin-api-client

[GMOコイン](https://coin.z.com/) の暗号資産取引 API を Rust の async (tokio) から呼び出すクライアントライブラリです。Public と Private の REST API、Public と Private の WebSocket API に対応しています。

- API の仕様: [GMOコイン API ドキュメント](https://api.coin.z.com/docs/)
- このクレートのドキュメント: `cargo doc --open`
- 不具合の報告: [Issues](https://github.com/j5ik2o/gmo-coin-api-client/issues)

## 特徴

- REST の 32 操作と WebSocket の 7 チャネルをすべて扱えます。REST のうち WebSocket 用のアクセストークンを扱う 3 操作は、ライブラリの内部で使います。
- 誤った注文はコンパイルの時点で書けません。価格のない指値注文、価格つきの成行注文、数量ゼロの注文は型として存在しません。API キーのないクライアントには、注文などの Private API のメソッドがありません。
- API シークレット、API キー、WebSocket 用のアクセストークンは、`Debug` 出力にもエラーにも出ません。シークレットは複製できず、不要になるとメモリをゼロで消します。
- 価格と数量は、取引所が返した値を誤差なく保ちます。数値として比べることができ、表示すると受け取った文字列と同じになります。十進数のクレートには依存しません。
- WebSocket は、切断されると自動で再接続して購読を送り直します。Private WebSocket のアクセストークンは、取得・延長・削除をライブラリが行います。
- 依存クレートは推移的なものを含めて 62 個です。HTTP は hyper、WebSocket は tokio-tungstenite、TLS は rustls (暗号は ring) を使います。

## はじめ方

### 必要なもの

- Rust 1.99.1 以降 (edition 2024)
- tokio ランタイム

### 追加

```toml
[dependencies]
gmo-coin-api-client = { git = "https://github.com/j5ik2o/gmo-coin-api-client" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### 最初の呼び出し

API キーなしで、BTC の最新レートを取得します。

```rust,no_run
use gmo_coin_api_client::{Client, Symbol};

#[tokio::main]
async fn main() -> gmo_coin_api_client::Result<()> {
    let client = Client::production();
    let ticker = client.ticker(Symbol::BTC).await?;
    println!("BTC 買 {} / 売 {} / 最終 {}", ticker.bid, ticker.ask, ticker.last);
    Ok(())
}
```

## 使い方

### 注文を出す

Private API を使うには、`Client::authenticate` に認証情報を渡します。`Credentials::from_env` は環境変数 `GMO_API_KEY` と `GMO_API_SECRET` を読みます。

注文には顧客注文 ID (`ClientOrderId`) が必須です。注文が届いたか、届いて処理されたかが分からないとき (`Error::outcome_unknown` が真のとき) は、同じ注文をそのまま送り直せます。通信が途中で切れた場合だけでなく、ゲートウェイが 502 を返した場合や、取引所の答えを読めなかった場合も含みます。取引所は使用済みの顧客注文 ID を ERR-915 で拒否するので、二重には発注されません。ERR-915 を受け取ったら、顧客注文 ID で注文を照会して元の注文 ID を得ます。

この送り直しの手順は、顧客注文 ID を持つ注文系の操作 (`place_order`、`close_position`、`close_bulk`) に限ります。`transfer` (口座振替) には冪等キーがないので、結果が分からなくても送り直さず、先に `margin()` や履歴で残高を照合してください。

```rust,no_run
use gmo_coin_api_client::error::ErrorCode;
use gmo_coin_api_client::rest::{NewOrder, OrderExecution, OrderLookup};
use gmo_coin_api_client::{
    Authenticated, Batch, Client, ClientOrderId, Error, LimitTimeInForce, OrderId, OrderPrice,
    OrderSize, Side, Symbol,
};

async fn buy(client: &Client<Authenticated>) -> Result<OrderId, Box<dyn std::error::Error>> {
    let order = NewOrder {
        client_order_id: ClientOrderId::generate(),
        symbol: Symbol::BTC,
        side: Side::Buy,
        size: OrderSize::parse("0.001")?,
        execution: OrderExecution::Limit {
            price: OrderPrice::parse("9000000")?,
            time_in_force: LimitTimeInForce::Fas,
            losscut_price: None,
        },
    };
    match client.place_order(&order).await {
        Ok(id) => Ok(id),
        Err(e) if e.outcome_unknown() => match client.place_order(&order).await {
            Ok(id) => Ok(id),
            // 最初の送信は届いていた。顧客注文 ID で注文を照会する。
            Err(Error::Api(api)) if api.has(ErrorCode::CLIENT_ORDER_ID_USED) => {
                let lookup = OrderLookup::ByClientOrderIds(Batch::one(order.client_order_id.clone()));
                let orders = client.orders(&lookup).await?;
                // ERR-915 の直後でも照会が空なら、取引所の反映を待って照会し直す。
                let order = orders.first().ok_or("ERR-915 なのに注文が照会できない")?;
                Ok(order.order_id)
            }
            Err(e) => Err(e.into()),
        },
        Err(e) => Err(e.into()),
    }
}
```

### WebSocket で配信を受ける

`Client::public_feed` と `Client::private_feed` が返す `Feed` に購読したいチャネルを登録し、`next()` を呼び続けます。接続、購読コマンドの送信、ping への応答、再接続と購読の送り直しは、すべて `next()` の中で進みます。

```rust,no_run
use gmo_coin_api_client::ws::{PublicChannel, PublicEvent};
use gmo_coin_api_client::{Client, Symbol};

async fn watch() -> gmo_coin_api_client::Result<()> {
    let mut feed = Client::production().public_feed();
    feed.subscribe(PublicChannel::Ticker(Symbol::BTC))?;
    loop {
        match feed.next().await? {
            PublicEvent::Ticker(t) => println!("BTC {}", t.last),
            // 切断中の配信は失われる。必要なら REST で取り直す。
            PublicEvent::Reconnected => {}
            _ => {}
        }
    }
}
```

`next()` を使うときは、次の 6 点に注意してください。

- `next()` は少なくとも 2 分に 1 回は呼んでください。取引所は 1 分ごとに ping を送り、3 回続けて応答がないと接続を切ります。応答は `next()` の中で返します。
- `next()` は `tokio::select!` の中で打ち切っても、購読もイベントも失いません。
- 終えるときは `close().await` を呼んでください。Private の場合は、アクセストークンを削除します。`close()` を呼ばずに drop すると、トークンは最長 60 分生き続け、その間は口座の上限 5 個の 1 つを占めます。
- 購読コマンドの間隔 (1 秒に 1 回) は `Client` ごとに守り、`Client` の複製は同じ間隔を共有します。取引所の上限は IP ごとなので、1 プロセスには 1 つの `Client` を使い、すべてのフィードをそこから作ってください。
- サーバがコマンドを拒否すると `Rejected { channel, text }` が届きます。`channel` は拒否されたコマンドのチャネルです。拒否された subscribe は購読の集合から外れるので、送り直すにはもう一度 `subscribe` を呼びます。コマンドを書いてから約 1 秒が過ぎた後に、未読のフレームがないことを `next()` が確かめると、それ以後に読む拒否の `channel` は `None` になり、購読は変わりません。フィードは拒否が届いた時刻を知ることができないので、`next()` を呼ばずに別のものを `await` していた間に届いた拒否は、遅れて読んでも `channel` が付きます。ただし、呼び出し側がランタイムのスレッドを塞いだ場合 (`std::thread::sleep` や、current-thread ランタイムでの重い同期処理など) は当てはまりません。tokio が最初のポーリングで返すソケットの準備状況が古く、窓が閉じることがあり、そのとき拒否は `channel: None` で戻ります。ランタイムのスレッドを塞ぐ使い方は、このフィードでは想定していません。窓が閉じるのは、予算の残った読み込みが空のソケットを観測したときだけなので、未読のフレームが途切れない間は窓は閉じません。拒否が 1 回の読み込みに収まらないフレームの後ろに並ぶ場合も、モックの取引所を使ったテストで確かめています。未読のフレームが途切れずに帰属をあきらめた場合も、以後の拒否は `channel: None` になりますが、最後に書いた subscribe を購読済みとして残さないので、呼び出し側が何もしなくても送り直します。送り直しはチャネルごとに 1 回までで、それも匿名の拒否で返ってきたら購読を外して書き続けません（もう一度 `subscribe` を呼べば、また 1 回だけ送り直します）。外すのは最後に書いた subscribe だけなので、窓（1.05 秒）より遅れて届いた拒否は、それより前に書いた subscribe を救えません。あきらめた状態でも、フィードは拒否の窓の期限ごとに `next()` の中でソケットを一度読むので、空になれば帰属が戻ります。
- 読まれる前に次のコマンドが書かれた拒否は、その次のコマンドのものとして届きます。これは既知の問題で、<https://github.com/j5ik2o/gmo-coin-api-client/issues/14> で扱っています。

## 設計上の約束

| 項目 | このクレートの振る舞い |
|---|---|
| 呼び出し回数の制限 | REST の呼び出しは間引きません。上限を超えると ERR-5003 が返り、`Error::is_rate_limited` が真になります。WebSocket の購読コマンドは、取引所の制限に合わせて 1 秒に 1 回の間隔で送ります。 |
| 送信前の検査 | 型で表せない条件 (たとえば `cancelBefore` は現物の売りの成行注文だけ、ロスカットレートはレバレッジ銘柄だけ) は、送信前に検査して `Error::Contract` を返します。このとき取引所には何も送りません。 |
| 応答の検査 | 応答が仕様の形でないときは、既定値で埋めずに `Error::Protocol` を返します。一括キャンセルの結果に送っていない注文 ID が含まれる場合も同じです。 |
| 取引ルール | 呼値、最小注文数量、注文単位は検査しません。取引所が検査します。 |
| 暗号化 | 本番の接続先には必ず TLS を使います。暗号化しない `http` と `ws` は、ループバックの IP アドレスにだけ接続できます。テストでモックサーバーに接続するためです。 |
| アクセストークン | WebSocket 用のアクセストークンを扱う API (`ws-auth`) は公開していません。Private の `Feed` が、取得、再接続での再利用、45 分での延長、終了時の削除を行います。 |

### 秘密情報について残っている制約

クレートが自分で持つバッファ (API シークレット、リクエスト本文、アクセストークン) はゼロで消します。ただし、次の複製は依存ライブラリの内部に残り、消せません。

- hyper のヘッダ (`API-KEY` と `API-SIGN`)
- hyper の書き込みバッファ (トークンを含む `ws-auth` の本文)
- tokio-tungstenite が保持する Private WebSocket の URL (トークンを含む)

## テスト

テストは、リポジトリ内のモックサーバーを相手に動きます。モックサーバーは署名を検証し、取引所と同じ形のエラーを返します。WebSocket の配信、切断、メンテナンスも再現します。

```sh
cargo test
```

本番の Public API と Public WebSocket に接続する確認は、通常のテストから外しています。

```sh
cargo test --test live_public -- --ignored --nocapture
cargo test --test ws_live -- --ignored --nocapture
```

Private API は本番では確かめていません。公式ドキュメントの応答例とモックサーバーで確かめています。

`tests/fixtures/` の応答例は、公式ドキュメントから `tools/` のスクリプトで取り出したものです。

## 変更を出す前に

変更を出すときは、次の 4 つがすべて通ることを手元で確かめてください。同じ検査を CI も走らせます。

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps --locked
```

lint の水準は `Cargo.toml` の `[lints]` が決めています。警告を黙らせるときは、`#[expect(..., reason = "...")]` で対象を小さく絞り、理由を書いてください。

## ライセンス

まだ決まっていません。`Cargo.toml` の `license` は暫定の値で、`LICENSE` ファイルは置いていません。
