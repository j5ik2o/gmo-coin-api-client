from pathlib import Path
import subprocess
import sys
import unittest

SCRIPT = Path(__file__).with_name("next-semver.py")


def next_semver(tag, level):
    return subprocess.run([sys.executable, str(SCRIPT), level], input=f"{tag}\n", text=True, capture_output=True)


class NextSemverTest(unittest.TestCase):
    def test_major_from_prerelease_tag_returns_core_version(self):
        for tag, expected in (
            ("v4.0.0-alpha.1", "4.0.0\n"),
            ("v4.0.0-alpha.0", "4.0.0\n"),
            ("v5.1.2-rc.3", "5.1.2\n"),
            ("v14.0.0-alpha.1", "14.0.0\n"),
            ("v10.0.0-alpha.1", "10.0.0\n"),
            ("v14.12.10-rc.1", "14.12.10\n"),
        ):
            with self.subTest(tag=tag):
                result = next_semver(tag, "major")
                self.assertEqual((result.returncode, result.stdout, result.stderr), (0, expected, ""))

    def test_non_major_from_prerelease_tag_fails_without_output(self):
        for level in ("patch", "minor"):
            with self.subTest(level=level):
                result = next_semver("v4.0.0-alpha.1", level)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("pre-release", result.stderr)

    def test_each_level_from_release_tag(self):
        for tag, level, expected in (
            ("v3.0.6", "major", "4.0.0\n"),
            ("v3.0.6", "minor", "3.1.0\n"),
            ("v3.0.6", "patch", "3.0.7\n"),
            ("v10.0.0", "major", "11.0.0\n"),
            ("v10.0.0", "minor", "10.1.0\n"),
            ("v10.0.0", "patch", "10.0.1\n"),
            ("v14.12.10", "patch", "14.12.11\n"),
        ):
            with self.subTest(tag=tag, level=level):
                result = next_semver(tag, level)
                self.assertEqual((result.returncode, result.stdout, result.stderr), (0, expected, ""))


if __name__ == "__main__":
    unittest.main()
