# AGENTS.md

このリポジトリで作業する人のためのルール。Claude Code は `CLAUDE.md` からこのファイルを読む。

## コミットメッセージ

[Conventional Commits](https://www.conventionalcommits.org/ja/v1.0.0/) の形にする。件名は英語で、命令形の短い文にする。

```
<type>(<scope>): <summary>
```

### type

| type | 使う場面 |
|---|---|
| `feat` | 利用者から見える機能の追加 |
| `fix` | 不具合の修正 |
| `perf` | 性能の改善 |
| `refactor` | 外から見た振る舞いを変えない構造の変更 |
| `docs` | 文書だけの変更 |
| `test` | テストだけの変更 |
| `build` | ビルドや依存の変更 |
| `ci` | CI とリリースのワークフローの変更 |
| `chore` | 上に当てはまらない雑務 (依存の更新など) |
| `revert` | 以前のコミットの取り消し |
| `style` | 意味を変えない整形 |

### scope

変更した範囲を 1 語で書く。省略してもよい。例は `ws`、`rest`、`transport`、`error`、`release`、`deps`、`docs` である。

```
fix(ws): keep the reconnect delay within the cap
ci: add the release workflows
chore(deps): update serde to 1.0.229
```

### 破壊的変更

型の名前、公開メソッドの引数、公開型の variant を互換性のない形で変えるときは、次のどちらかで示す。

- type か scope の後ろに `!` を付ける (`feat(rest)!: rename OrderLookup`)
- 本文の footer に `BREAKING CHANGE: <変更の説明>` を書く

### 版の上げ幅との関係

`lib-bump-version` ワークフローは `.github/semver-level.py` で、前のタグからのコミットを見て上げ幅を決める。対応は次のとおり。

- `feat` と `revert` があれば minor
- それ以外の type だけなら patch
- `!` か `BREAKING CHANGE:` があれば major

0.x の版でもこの規則をそのまま使う。`0.1.0` から `feat` で上げると `0.2.0`、`fix` だけなら `0.1.1`、破壊的変更なら `1.0.0` になる。

## プルリクエスト

タイトルもコミットメッセージと同じ `<type>(<scope>): <summary>` の形にする。説明には変更の理由と、確かめたコマンドを書く。

## 変更を出す前に

次のコマンドを手元で通す。

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps --locked
python3 -m unittest discover -s .github -p 'test_*.py' -v
actionlint -shellcheck=''
cargo deny check advisories licenses
```

## リリース

`Cargo.toml` の `publish = false` は、加藤さんの公開承認があるまで外さない。この設定がある間は `lib-release` ワークフローの `cargo publish` は失敗する。

リリースは次の順で進める。

1. Actions の画面から `lib-bump-version` を実行する。このワークフローは版を上げる枝 (`chore/release-vX.Y.Z`) とコミット (`chore: release vX.Y.Z`) を作り、リリースノートを本文にした PR を開く。main への push も、タグの作成も、GitHub Release の作成もしない。
2. PR をマージする。`GITHUB_TOKEN` で作った PR では CI が自動で走らないので、PR を閉じて開き直すか、空のコミットを足すなどして CI を走らせる。
3. マージのあと、加藤さんが main に `vX.Y.Z` タグを push する。`lib-release` はそのタグの push で起動する。
4. `lib-release` は Environment `crates-io` の承認を待ってから `cargo publish` を実行する。

最初の 0.1.0 は版を上げる PR を作らない。加藤さんが今の main に `v0.1.0` を push する。`lib-bump-version` は `v*` タグがないときに、この手順を促すメッセージを出して止まる。

公開は加藤さんが GitHub の Environment (`crates-io`) で承認する。`lib-release` のジョブにこの Environment を付けてあり、Environment に必須レビュアーを設定すると、承認が済むまでジョブは実行を始めない。

公開に使う `CARGO_REGISTRY_TOKEN` は、リポジトリの secret ではなく Environment `crates-io` の secret に置く。登録は加藤さんが自分で行う。
