#!/usr/bin/env python3
"""main-ci-failure の失敗の順序に使う ci-run-key を取り出す。

公開リポジトリでは第三者がコメントできる。本文やコメントの印だけを見ると、
第三者が大きな値を書いて失敗 issue を開いたままにできる。そこで、印を読む
対象を、github-actions[bot] が書いた本文とコメントだけに限る。ログイン名だけで
なく user.type が Bot であることも確かめる。

使い方:
  gh api "repos/OWNER/REPO/issues/N" > issue.json
  gh api --paginate --slurp "repos/OWNER/REPO/issues/N/comments" > comments.json
  failure-run-key.py issue.json comments.json
"""

import json
import re
import sys

BOT_LOGIN = "github-actions[bot]"
BOT_TYPE = "Bot"
KEY = re.compile(r"ci-run-key: ([0-9]+)")


def is_bot(user):
    return (
        isinstance(user, dict)
        and user.get("login") == BOT_LOGIN
        and user.get("type") == BOT_TYPE
    )


def keys_in(text):
    return [int(value) for value in KEY.findall(text or "")]


def keys_from_issue(issue):
    return keys_in(issue.get("body")) if is_bot(issue.get("user")) else []


def keys_from_comment_pages(pages):
    keys = []
    for page in pages:
        for comment in page:
            if is_bot(comment.get("user")):
                keys.extend(keys_in(comment.get("body")))
    return keys


def latest_key(issue, comment_pages):
    keys = keys_from_issue(issue) + keys_from_comment_pages(comment_pages)
    return max(keys) if keys else 0


def main(argv):
    if len(argv) != 3:
        sys.stderr.write("usage: failure-run-key.py ISSUE_JSON COMMENTS_JSON\n")
        return 2
    with open(argv[1], encoding="utf-8") as issue_file:
        issue = json.load(issue_file)
    with open(argv[2], encoding="utf-8") as comments_file:
        pages = json.load(comments_file)
    if not isinstance(pages, list) or (pages and not isinstance(pages[0], list)):
        pages = [pages]
    print(latest_key(issue, pages))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
