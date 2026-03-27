import re
import tempfile
import unittest
from pathlib import Path

from email_backup import (
    BackupError,
    Config,
    DEFAULT_IGNORE_MAILBOX_REGEX,
    DEFAULT_MAILDIR_PATH,
    DEFAULT_STATE_DIR,
    build_manifest,
    compare_manifests,
    load_manifest,
    write_checksum,
    write_json,
)


def manifest_record(
    *,
    key: str,
    message_ts: int = 0,
    first_seen_ts: int | None = 0,
    occurrences: int = 1,
    mailboxes: list[str] | None = None,
    content_hashes: list[str] | None = None,
) -> dict:
    return {
        "key": key,
        "message_id": "<msg@example.com>",
        "subject": "subject",
        "message_ts": message_ts,
        "first_seen_ts": first_seen_ts,
        "occurrences": occurrences,
        "mailboxes": mailboxes or ["INBOX"],
        "sample_paths": ["cur/msg"],
        "content_hashes": content_hashes or ["hash-a"],
        "size_bytes": 123,
        "latest_file_mtime": 0,
    }


class EmailBackupTests(unittest.TestCase):
    def test_config_defaults_use_tool_local_subdirs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "email-backup.toml"
            config_path.write_text(
                '\n'.join(
                    [
                        'restic_repo = "/tmp/restic-repo"',
                        'mbsync_command = ["/bin/echo", "mbsync"]',
                        'restic_command = ["/bin/echo", "restic"]',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            config = Config.load(config_path)

            self.assertEqual(config.maildir_path, (config_path.parent / DEFAULT_MAILDIR_PATH).resolve())
            self.assertEqual(config.state_dir, (config_path.parent / DEFAULT_STATE_DIR).resolve())

    def test_build_manifest_rejects_missing_maildir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BackupError):
                build_manifest(Path(temp_dir) / "missing-maildir", DEFAULT_IGNORE_MAILBOX_REGEX)

    def test_default_ignore_regex_matches_dot_maildir_folders(self) -> None:
        pattern = re.compile(DEFAULT_IGNORE_MAILBOX_REGEX)

        self.assertIsNotNone(pattern.search(".Trash"))
        self.assertIsNotNone(pattern.search(".Spam.sub"))
        self.assertIsNotNone(pattern.search("INBOX/Trash"))
        self.assertIsNone(pattern.search("Archive"))

    def test_compare_alerts_on_mailbox_or_copy_loss(self) -> None:
        baseline = {
            "records": {
                "mid:<a>": manifest_record(
                    key="mid:<a>",
                    occurrences=2,
                    mailboxes=["INBOX", ".Archive"],
                )
            }
        }
        current = {
            "records": {
                "mid:<a>": manifest_record(
                    key="mid:<a>",
                    occurrences=1,
                    mailboxes=["INBOX"],
                )
            }
        }

        report = compare_manifests(baseline, current, immutability_days=1, sample_limit=20)

        self.assertEqual(report["summary"]["status"], "alert")
        self.assertEqual(report["summary"]["placement_changed_stable_count"], 1)
        self.assertEqual(report["summary"]["missing_stable_count"], 0)
        self.assertEqual(report["summary"]["mutated_stable_count"], 0)

    def test_compare_uses_first_seen_for_stability(self) -> None:
        baseline = {
            "records": {
                "mid:<future>": manifest_record(
                    key="mid:<future>",
                    message_ts=4102444800,
                    first_seen_ts=0,
                )
            }
        }
        current = {"records": {}}

        report = compare_manifests(baseline, current, immutability_days=1, sample_limit=20)

        self.assertEqual(report["summary"]["status"], "alert")
        self.assertEqual(report["summary"]["missing_stable_count"], 1)

    def test_load_manifest_rejects_checksum_mismatch(self) -> None:
        manifest = {
            "schema_version": 2,
            "generated_at": "2026-01-01T00:00:00+00:00",
            "maildir": "/tmp/maildir",
            "ignore_mailbox_regex": DEFAULT_IGNORE_MAILBOX_REGEX,
            "stats": {
                "files_scanned": 1,
                "files_ignored_by_mailbox": 0,
                "scan_errors": 0,
                "unique_messages": 1,
            },
            "records": {"mid:<a>": manifest_record(key="mid:<a>")},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "baseline.json"
            write_json(path, manifest)
            write_checksum(path)
            path.write_text('{"tampered": true}\n', encoding="utf-8")

            with self.assertRaises(BackupError):
                load_manifest(path)


if __name__ == "__main__":
    unittest.main()
