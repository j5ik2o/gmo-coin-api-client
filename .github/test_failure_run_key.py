import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("failure-run-key.py")
spec = importlib.util.spec_from_file_location("failure_run_key", SCRIPT)
run_key = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_key)

BOT = {"login": "github-actions[bot]", "type": "Bot"}


def comment(login, kind, body):
    return {"user": {"login": login, "type": kind}, "body": body}


class FailureRunKeyTest(unittest.TestCase):
    def test_reads_the_issue_body_and_comments_from_the_bot(self):
        issue = comment("github-actions[bot]", "Bot", "ci-run-key: 7")
        pages = [[comment("github-actions[bot]", "Bot", "ci-run-key: 900")]]
        self.assertEqual(run_key.latest_key(issue, pages), 900)

    def test_ignores_a_comment_with_the_marker_from_a_person(self):
        issue = comment("github-actions[bot]", "Bot", "ci-run-key: 7")
        pages = [
            [
                comment("mallory", "User", "ci-run-key: 999999"),
                comment("github-actions[bot]", "Bot", "ci-run-key: 9"),
            ]
        ]
        self.assertEqual(run_key.latest_key(issue, pages), 9)

    def test_ignores_other_bots_and_a_claim_to_be_the_ci_bot(self):
        pages = [
            [
                comment("renovate[bot]", "Bot", "ci-run-key: 888888"),
                comment("github-actions[bot]", "User", "ci-run-key: 777777"),
                comment("github-actions[bot]", "Bot", "ci-run-key: 5"),
            ]
        ]
        self.assertEqual(run_key.latest_key(comment("mallory", "User", "ci-run-key: 999999"), pages), 5)

    def test_ignores_a_body_from_a_person(self):
        issue = comment("mallory", "User", "ci-run-key: 999999")
        self.assertEqual(run_key.latest_key(issue, [[]]), 0)

    def test_no_keys_means_zero(self):
        self.assertEqual(run_key.latest_key(comment("github-actions[bot]", "Bot", "no key"), [[]]), 0)

    def test_cli_reads_the_files(self):
        with tempfile.TemporaryDirectory() as directory:
            issue_path = Path(directory) / "issue.json"
            comments_path = Path(directory) / "comments.json"
            issue_path.write_text(
                json.dumps(comment("github-actions[bot]", "Bot", "ci-run-key: 7")), encoding="utf-8"
            )
            comments_path.write_text(
                json.dumps(
                    [
                        [
                            comment("mallory", "User", "ci-run-key: 999999"),
                            comment("github-actions[bot]", "Bot", "ci-run-key: 42"),
                        ]
                    ]
                ),
                encoding="utf-8",
            )
            result = subprocess.run(
                [sys.executable, str(SCRIPT), str(issue_path), str(comments_path)],
                capture_output=True,
                text=True,
            )
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "42\n", ""))


if __name__ == "__main__":
    unittest.main()
