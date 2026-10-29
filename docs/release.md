# リリース手順

この文書は、0.1.0 以降の版上げと crates.io への公開の手順をまとめたものです。関係するワークフローは `lib-bump-version`、`CI`、`lib-release` の 3 つです。

## 版上げの PR は人が開く

### 決めたこと

版上げの PR は、ワークフローではなく人が開きます。`lib-bump-version` は版を上げた枝とリリースノートを作るところまでを担当し、PR は人が開きます。人が開いた PR では `pull_request` の `opened` が起こるので、合意した CI がそのまま実行されます。

### なぜこの方法にしたか

CI の起こし方を決めるには、まず「版上げの PR を誰が開くか」を決める必要があります。GitHub は、リポジトリの `GITHUB_TOKEN` が起こしたイベントでは新しいワークフローを起動しません。例外は `workflow_dispatch` と `repository_dispatch` だけです（[Triggering a workflow](https://docs.github.com/en/actions/how-tos/writing-workflows/choosing-when-your-workflow-runs/triggering-a-workflow)）。つまり `GITHUB_TOKEN` で PR を開くと、その PR では CI は実行されません。どの方法を選ぶにしても、PR を開く人を変えるか、人が別の操作で CI を起こすかのどちらかになります。

もう 1 つ制約があります。いまリポジトリ設定「Allow GitHub Actions to create and approve pull requests」は無効です。この設定が無効の間は、`GITHUB_TOKEN` は PR を作れません。`lib-bump-version` が `gh pr create` を呼んでも失敗します。したがって、PR を閉じて開き直す方法と `workflow_dispatch` で CI を起こす方法は、この設定を有効にすることを前提にしています。

タグの push と公開の承認は、もともと加藤さんが行います。版上げには人の操作が入るので、PR を開くのも人に任せます。長く残る秘密情報やリポジトリ設定を増やさずに済む方法として、人に PR を開いてもらいます。人が開いた PR は、ほかの PR と同じように CI が実行されます。

### 比べた方法

| 方法 | 追加の設定 | 秘密情報 | 人がすること | 採否 |
|---|---|---|---|---|
| 人が PR を開く | なし | なし | PR を開く（本文は実行のアーティファクトから使える） | 採用 |
| GitHub App のトークンで PR を開く | App の登録とインストール | App の秘密鍵 | なし | 自動化したくなったら検討 |
| Actions に PR を作らせ、人が閉じて開き直す | 「Allow GitHub Actions to create and approve pull requests」を有効にする | なし | 閉じて開き直す | 不採用 |
| `workflow_dispatch` で CI を起こす | 上の設定を有効にする（PR を作るため） | なし | 実行を押す | 不採用 |
| 人が空のコミットを追加する | 上の設定を有効にする（PR を作るため） | なし | 空のコミットを追加する | 不採用 |

- GitHub App のトークンを使う方法では、PR を開くのは App になり、CI も自動で実行されます。いまの方法と比べると、秘密鍵の保管と App の管理が増えます。自動化を優先するときに移します。
- Actions に PR を作らせる方法、`workflow_dispatch`、空のコミットは、どれも上の設定の有効化が前提です。設定を有効にすると、Actions は PR の承認もできるようになります。そのため、この方法を採る場合は、main の保護ルールで「Actions 以外の承認を 1 件必須」にします。この文書を書いた時点で main には保護ルールも ruleset もないので、設定と保護ルールを同時に用意する必要があります。今回はどちらもしていません。

## 手順

### 1. 版上げのワークフローを実行する

Actions の画面で `lib-bump-version` を選び、`Run workflow` を押します。`Use workflow from` は `main` にします。

- `level` は `auto` のままで構いません。版を決め打ちするときは `version` に `X.Y.Z` を入れます。
- 前の版のタグ（`v0.1.0` など）がないと、このワークフローは理由を出して止まります。最初の版は加藤さんが手でタグを push します。
- 実行が終わると、枝 `chore/release-vX.Y.Z` が push されます。リリースノートは実行の Summary と、実行のアーティファクト `release-notes` に出ます。main への push、タグの作成、GitHub Release の作成はしません。

### 2. 人が PR を開く

コマンドで開く場合は、実行のアーティファクトをダウンロードしてから PR を開きます。実行の Summary には、実行の ID を埋めたこのコマンドが出ます。そのまま使って構いません。

```sh
gh run download <実行の ID> -n release-notes -D release-notes-dir
gh pr create --base main --head chore/release-vX.Y.Z \
  --title "chore: release vX.Y.Z" \
  --body-file release-notes-dir/release-notes.md
```

画面で開く場合は、ブランチの一覧に出る「Compare & pull request」から開き、タイトルを `chore: release vX.Y.Z`、本文を Summary のリリースノートにします。

タイトルはコミットメッセージと同じ `<type>(<scope>): <summary>` の形にします。詳しくは [AGENTS.md](../AGENTS.md) を見てください。

### 3. CI を確かめる

PR を開くと、CI の `check`・`release-control`・`audit` と GitGuardian が実行されます。

- `check` は `cargo fmt --check`、`cargo clippy -D warnings`、`cargo test`、`cargo doc` を実行します。
- `release-control` は `.github` の Python のテストと actionlint を実行します。
- `audit` は `cargo deny` で advisories と licenses を確かめます。

CI が実行されなかったときは、PR を閉じて開き直します。人が操作した `reopened` で CI が実行されます。

### 4. PR をマージする

加藤さんの承認を得てから、PR を main にマージします。マージの方法はこれまでどおりです。

### 5. タグを push して公開する

加藤さんが main に `vX.Y.Z` タグを push します。`lib-release` はそのタグの push で起動し、Environment `crates-io` の承認を待ってから `cargo publish` を実行します。

## 「Allow GitHub Actions to create and approve pull requests」の扱い

この手順では、この設定を有効にしません。無効のままでよいので、加藤さんの承認を待って有効にする必要はありません。

有効にする方法を採る場合は、この文書の「比べた方法」に書いたとおり、main の保護ルールで「Actions 以外の承認を 1 件必須」にします。設定だけを有効にして、Actions が自分の開いた PR を承認できる状態にはしません。

## 補足: 保護ルールはまだない

この文書を書いた時点で、main には保護ルールも ruleset もありません。「CI が成功しないとマージできない」は、いまは運用の約束です。保護ルールを入れるときの必須チェックは、CI の `check`・`release-control`・`audit` の 3 つです。
