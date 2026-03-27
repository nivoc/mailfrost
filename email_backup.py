#!/usr/bin/env python3

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email import policy
from email.parser import BytesHeaderParser
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, TextIO


DEFAULT_IGNORE_MAILBOX_REGEX = r"(^|[/.])(Trash|Junk|Spam|Drafts)([/.]|$)"
DEFAULT_MBSYNC_COMMAND = ["mbsync", "-c", "./mbsyncrc", "mail-backup"]
DEFAULT_RESTIC_COMMAND = ["restic"]
DEFAULT_RESTIC_BACKUP_ARGS = ["--tag", "mail-backup"]
DEFAULT_RESTIC_CHECK_ARGS = ["--read-data-subset=5%"]
DEFAULT_MAILDIR_PATH = "./data/maildir"
DEFAULT_STATE_DIR = "./data/state"
DEFAULT_REPORT_SAMPLE_LIMIT = 20
DEFAULT_IMMUTABILITY_DAYS = 30
DEFAULT_RESTIC_CHECK_INTERVAL_DAYS = 7
SAMPLE_PATH_LIMIT = 5
ALERT_EXIT_CODE = 2
CURRENT_MANIFEST_SCHEMA_VERSION = 2


class BackupError(Exception):
    pass


class ConfigError(BackupError):
    pass


class CommandFailed(BackupError):
    def __init__(self, command: list[str], exit_code: int) -> None:
        self.command = command
        self.exit_code = exit_code
        super().__init__(f"Command failed with exit code {exit_code}: {shlex.join(command)}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def timestamp_local() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def expand_string(value: str) -> str:
    return os.path.expandvars(os.path.expanduser(value))


def resolve_path(base_dir: Path, raw_value: str) -> Path:
    expanded = Path(expand_string(raw_value))
    if not expanded.is_absolute():
        expanded = (base_dir / expanded).resolve()
    return expanded


def resolve_repo_spec(base_dir: Path, raw_value: str) -> str:
    expanded = expand_string(raw_value)
    if expanded.startswith(("/", ".", "~")) or ":" not in expanded:
        return str(resolve_path(base_dir, expanded))
    return expanded


def resolve_command(base_dir: Path, raw_value: Any, default: list[str]) -> list[str]:
    if raw_value is None:
        return list(default)
    if not isinstance(raw_value, list) or not raw_value:
        raise ConfigError("Command entries must be non-empty arrays of strings")
    if not all(isinstance(item, str) and item for item in raw_value):
        raise ConfigError("Command entries must contain only non-empty strings")

    command = [expand_string(item) for item in raw_value]
    executable = command[0]
    if "/" in executable:
        command[0] = str(resolve_path(base_dir, executable))
    return command


def resolve_string_list(raw_value: Any, default: list[str]) -> list[str]:
    if raw_value is None:
        return list(default)
    if not isinstance(raw_value, list) or not all(isinstance(item, str) for item in raw_value):
        raise ConfigError("Argument lists must be arrays of strings")
    return [expand_string(item) for item in raw_value]


def require_string(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a non-empty string")
    return value.strip()


def optional_string(data: dict[str, Any], key: str, default: str) -> str:
    value = data.get(key, default)
    if not isinstance(value, str):
        raise ConfigError(f"{key} must be a string")
    return value


def optional_int(data: dict[str, Any], key: str, default: int, minimum: int = 0) -> int:
    value = data.get(key, default)
    if not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{key} must be an integer >= {minimum}")
    return value


def optional_bool(data: dict[str, Any], key: str, default: bool) -> bool:
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{key} must be a boolean")
    return value


def resolve_env(raw_value: Any) -> dict[str, str]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ConfigError("env must be a table of string values")

    resolved: dict[str, str] = {}
    for key, value in raw_value.items():
        if not isinstance(key, str) or not key:
            raise ConfigError("env keys must be non-empty strings")
        if not isinstance(value, (str, int, float, bool)):
            raise ConfigError(f"env.{key} must be a scalar value")
        resolved[key] = expand_string(str(value))
    return resolved


@dataclass(frozen=True)
class Config:
    config_path: Path
    maildir_path: Path
    restic_repo: str
    state_dir: Path
    immutability_days: int = DEFAULT_IMMUTABILITY_DAYS
    report_sample_limit: int = DEFAULT_REPORT_SAMPLE_LIMIT
    restic_check_interval_days: int = DEFAULT_RESTIC_CHECK_INTERVAL_DAYS
    restic_include_state_dir: bool = True
    ignore_mailbox_regex: str = DEFAULT_IGNORE_MAILBOX_REGEX
    mbsync_command: list[str] = field(default_factory=lambda: list(DEFAULT_MBSYNC_COMMAND))
    restic_command: list[str] = field(default_factory=lambda: list(DEFAULT_RESTIC_COMMAND))
    restic_backup_args: list[str] = field(default_factory=lambda: list(DEFAULT_RESTIC_BACKUP_ARGS))
    restic_check_args: list[str] = field(default_factory=lambda: list(DEFAULT_RESTIC_CHECK_ARGS))
    alert_command: str = ""
    env: dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, config_path: Path) -> "Config":
        resolved_config_path = config_path.resolve()
        if not resolved_config_path.is_file():
            raise ConfigError(f"Configuration file not found: {resolved_config_path}")

        with resolved_config_path.open("rb") as handle:
            raw = tomllib.load(handle)

        if not isinstance(raw, dict):
            raise ConfigError("Configuration file must contain a TOML table")

        allowed_keys = {
            "maildir_path",
            "restic_repo",
            "state_dir",
            "immutability_days",
            "report_sample_limit",
            "restic_check_interval_days",
            "restic_include_state_dir",
            "ignore_mailbox_regex",
            "mbsync_command",
            "restic_command",
            "restic_backup_args",
            "restic_check_args",
            "alert_command",
            "env",
        }
        unknown_keys = sorted(set(raw) - allowed_keys)
        if unknown_keys:
            raise ConfigError(f"Unknown configuration keys: {', '.join(unknown_keys)}")

        base_dir = resolved_config_path.parent
        maildir_path = resolve_path(base_dir, optional_string(raw, "maildir_path", DEFAULT_MAILDIR_PATH))
        state_dir = resolve_path(base_dir, optional_string(raw, "state_dir", DEFAULT_STATE_DIR))
        restic_repo = resolve_repo_spec(base_dir, require_string(raw, "restic_repo"))
        ignore_mailbox_regex = optional_string(raw, "ignore_mailbox_regex", DEFAULT_IGNORE_MAILBOX_REGEX)

        try:
            re.compile(ignore_mailbox_regex)
        except re.error as exc:
            raise ConfigError(f"ignore_mailbox_regex is invalid: {exc}") from exc

        config = cls(
            config_path=resolved_config_path,
            maildir_path=maildir_path,
            restic_repo=restic_repo,
            state_dir=state_dir,
            immutability_days=optional_int(raw, "immutability_days", DEFAULT_IMMUTABILITY_DAYS, minimum=1),
            report_sample_limit=optional_int(raw, "report_sample_limit", DEFAULT_REPORT_SAMPLE_LIMIT, minimum=1),
            restic_check_interval_days=optional_int(
                raw,
                "restic_check_interval_days",
                DEFAULT_RESTIC_CHECK_INTERVAL_DAYS,
                minimum=0,
            ),
            restic_include_state_dir=optional_bool(raw, "restic_include_state_dir", True),
            ignore_mailbox_regex=ignore_mailbox_regex,
            mbsync_command=resolve_command(base_dir, raw.get("mbsync_command"), DEFAULT_MBSYNC_COMMAND),
            restic_command=resolve_command(base_dir, raw.get("restic_command"), DEFAULT_RESTIC_COMMAND),
            restic_backup_args=resolve_string_list(raw.get("restic_backup_args"), DEFAULT_RESTIC_BACKUP_ARGS),
            restic_check_args=resolve_string_list(raw.get("restic_check_args"), DEFAULT_RESTIC_CHECK_ARGS),
            alert_command=optional_string(raw, "alert_command", "").strip(),
            env=resolve_env(raw.get("env")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.maildir_path.exists() and not self.maildir_path.is_dir():
            raise ConfigError(f"maildir_path is not a directory: {self.maildir_path}")
        if self.state_dir.exists() and not self.state_dir.is_dir():
            raise ConfigError(f"state_dir is not a directory: {self.state_dir}")
        if not self.mbsync_command:
            raise ConfigError("mbsync_command must not be empty")
        if not self.restic_command:
            raise ConfigError("restic_command must not be empty")
        if not self.restic_repo:
            raise ConfigError("restic_repo must not be empty")
        self._validate_command(self.mbsync_command)
        self._validate_command(self.restic_command)

    def _validate_command(self, command: list[str]) -> None:
        executable = command[0]
        if "/" in executable:
            if not Path(executable).is_file():
                raise ConfigError(f"Command not found: {executable}")
            return
        if shutil.which(executable) is None:
            raise ConfigError(f"Command not found in PATH: {executable}")


@dataclass
class StatePaths:
    state_dir: Path
    log_dir: Path
    manifest_dir: Path
    report_dir: Path
    alert_log: Path
    restic_check_stamp: Path
    lock_file: Path

    @classmethod
    def from_state_dir(cls, state_dir: Path) -> "StatePaths":
        return cls(
            state_dir=state_dir,
            log_dir=state_dir / "logs",
            manifest_dir=state_dir / "manifests",
            report_dir=state_dir / "reports",
            alert_log=state_dir / "alerts.log",
            restic_check_stamp=state_dir / "restic-check.last_ok",
            lock_file=state_dir / ".lock",
        )

    def create(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class Runtime:
    config: Config
    paths: StatePaths
    run_id: str
    run_log_path: Path
    run_log_handle: TextIO
    lock_handle: TextIO

    @classmethod
    def start(cls, config: Config) -> "Runtime":
        paths = StatePaths.from_state_dir(config.state_dir)
        paths.create()

        lock_handle = paths.lock_file.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_handle.close()
            raise BackupError(f"Another backup run appears to be active: {paths.lock_file}") from exc

        run_id = utc_now().strftime("%Y%m%dT%H%M%SZ")
        lock_handle.seek(0)
        lock_handle.truncate()
        lock_handle.write(json.dumps({"pid": os.getpid(), "started_at": run_id}) + "\n")
        lock_handle.flush()

        run_log_path = paths.log_dir / f"run-{run_id}.log"
        run_log_handle = run_log_path.open("a", encoding="utf-8")
        runtime = cls(
            config=config,
            paths=paths,
            run_id=run_id,
            run_log_path=run_log_path,
            run_log_handle=run_log_handle,
            lock_handle=lock_handle,
        )
        runtime.log("INFO", f"Run id: {runtime.run_id}")
        runtime.log("INFO", f"Config file: {config.config_path}")
        runtime.log("INFO", f"State dir: {config.state_dir}")
        return runtime

    def close(self) -> None:
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.lock_handle.close()
            self.run_log_handle.close()

    def log(self, level: str, message: str) -> None:
        line = f"{timestamp_local()} [{level}] {message}"
        print(line, flush=True)
        self.run_log_handle.write(line + "\n")
        self.run_log_handle.flush()

    def log_raw(self, text: str) -> None:
        sys.stdout.write(text)
        sys.stdout.flush()
        self.run_log_handle.write(text)
        self.run_log_handle.flush()

    def command_env(self, extra_env: dict[str, str] | None = None) -> dict[str, str]:
        env = os.environ.copy()
        env.update(self.config.env)
        if extra_env:
            env.update(extra_env)
        return env

    def run_command(self, command: list[str], extra_env: dict[str, str] | None = None) -> None:
        self.log("INFO", f"Running command: {shlex.join(command)}")
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=self.command_env(extra_env),
                cwd=str(self.config.config_path.parent),
            )
        except FileNotFoundError as exc:
            raise BackupError(f"Required command not found: {command[0]}") from exc

        assert process.stdout is not None
        with process.stdout:
            for chunk in process.stdout:
                self.log_raw(chunk)

        exit_code = process.wait()
        if exit_code != 0:
            raise CommandFailed(command, exit_code)

    def send_alert(self, status: str, subject: str, body: str) -> None:
        alert_line = f"{timestamp_local()} [{status}] {subject}"
        with self.paths.alert_log.open("a", encoding="utf-8") as handle:
            handle.write(alert_line + "\n")

        if not self.config.alert_command:
            return

        alert_env = self.command_env(
            {
                "EMAIL_BACKUP_STATUS": status,
                "EMAIL_BACKUP_ALERT_SUBJECT": subject,
                "EMAIL_BACKUP_ALERT_BODY": body,
            }
        )
        result = subprocess.run(
            ["/bin/bash", "-c", self.config.alert_command],
            input=body,
            text=True,
            env=alert_env,
            capture_output=True,
        )
        if result.returncode != 0:
            self.log(
                "WARN",
                f"Alert command failed with exit code {result.returncode}: {self.config.alert_command}",
            )
            if result.stdout:
                self.log_raw(result.stdout)
            if result.stderr:
                self.log_raw(result.stderr)


def normalize_message_id(value: Any) -> str | None:
    if not value:
        return None
    compact = " ".join(str(value).split()).strip()
    if not compact:
        return None
    if compact.startswith("<") and compact.endswith(">"):
        compact = compact[1:-1].strip()
    compact = compact.lower()
    if not compact:
        return None
    return f"<{compact}>"


def clean_text(value: Any) -> str:
    if not value:
        return ""
    return " ".join(str(value).split())


def parse_message_timestamp(header_value: Any, fallback_epoch: float) -> int:
    if not header_value:
        return int(fallback_epoch)
    try:
        parsed = parsedate_to_datetime(str(header_value))
    except Exception:
        return int(fallback_epoch)
    if parsed is None:
        return int(fallback_epoch)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def iter_maildir_files(maildir_root: Path):
    for root, dirs, files in os.walk(maildir_root):
        dirs.sort()
        files.sort()
        base = os.path.basename(root)

        if base == "tmp":
            dirs[:] = []
            continue

        if base not in {"cur", "new"}:
            continue

        dirs[:] = []
        for filename in files:
            yield Path(root) / filename


def mailbox_name(maildir_root: Path, message_path: Path) -> str:
    mailbox_root = message_path.parent.parent
    relative = mailbox_root.relative_to(maildir_root)
    if str(relative) == ".":
        return "INBOX"
    return relative.as_posix()


def relative_message_path(maildir_root: Path, message_path: Path) -> str:
    return message_path.relative_to(maildir_root).as_posix()


def new_record(
    key: str,
    message_id: str | None,
    subject: str,
    message_ts: int,
    first_seen_ts: int,
    content_hash: str,
    mailbox: str,
    rel_path: str,
    size_bytes: int,
    file_mtime: float,
) -> dict[str, Any]:
    return {
        "key": key,
        "message_id": message_id,
        "subject": subject,
        "message_ts": message_ts,
        "first_seen_ts": first_seen_ts,
        "occurrences": 1,
        "mailboxes": {mailbox},
        "sample_paths": [rel_path],
        "content_hashes": {content_hash},
        "size_bytes": size_bytes,
        "latest_file_mtime": int(file_mtime),
    }


def update_record(
    record: dict[str, Any],
    message_id: str | None,
    subject: str,
    message_ts: int,
    content_hash: str,
    mailbox: str,
    rel_path: str,
    size_bytes: int,
    file_mtime: float,
) -> None:
    record["occurrences"] += 1
    record["mailboxes"].add(mailbox)
    record["content_hashes"].add(content_hash)
    record["size_bytes"] = max(record["size_bytes"], size_bytes)
    record["latest_file_mtime"] = max(record["latest_file_mtime"], int(file_mtime))

    if record["message_ts"] is None or message_ts < record["message_ts"]:
        record["message_ts"] = message_ts
    if message_id and not record["message_id"]:
        record["message_id"] = message_id
    if subject and not record["subject"]:
        record["subject"] = subject
    if rel_path not in record["sample_paths"] and len(record["sample_paths"]) < SAMPLE_PATH_LIMIT:
        record["sample_paths"].append(rel_path)


def freeze_manifest_records(records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    frozen: dict[str, dict[str, Any]] = {}
    for key, record in records.items():
        frozen[key] = {
            "key": record["key"],
            "message_id": record["message_id"],
            "subject": record["subject"],
            "message_ts": record["message_ts"],
            "first_seen_ts": record["first_seen_ts"],
            "occurrences": record["occurrences"],
            "mailboxes": sorted(record["mailboxes"]),
            "sample_paths": list(record["sample_paths"]),
            "content_hashes": sorted(record["content_hashes"]),
            "size_bytes": record["size_bytes"],
            "latest_file_mtime": record["latest_file_mtime"],
        }
    return frozen


def build_manifest(
    maildir_root: Path,
    ignore_mailbox_regex: str,
    observed_at_epoch: int | None = None,
) -> dict[str, Any]:
    if not maildir_root.is_dir():
        raise BackupError(f"maildir_path does not exist or is not a directory: {maildir_root}")

    if observed_at_epoch is None:
        observed_at_epoch = int(time.time())

    ignore_pattern = re.compile(ignore_mailbox_regex) if ignore_mailbox_regex else None
    parser = BytesHeaderParser(policy=policy.default)
    records: dict[str, dict[str, Any]] = {}
    stats = {
        "files_scanned": 0,
        "files_ignored_by_mailbox": 0,
        "scan_errors": 0,
    }
    scan_errors: list[str] = []

    for message_path in iter_maildir_files(maildir_root):
        mailbox = mailbox_name(maildir_root, message_path)
        if ignore_pattern and ignore_pattern.search(mailbox):
            stats["files_ignored_by_mailbox"] += 1
            continue

        rel_path = relative_message_path(maildir_root, message_path)
        try:
            file_stat = message_path.stat()
            raw_message = message_path.read_bytes()
            headers = parser.parsebytes(raw_message, headersonly=True)
        except Exception as exc:
            stats["scan_errors"] += 1
            scan_errors.append(f"{rel_path}: {exc}")
            continue

        message_hash = hashlib.sha256(raw_message).hexdigest()
        message_id = normalize_message_id(headers.get("Message-ID"))
        subject = clean_text(headers.get("Subject"))
        message_ts = parse_message_timestamp(headers.get("Date"), file_stat.st_mtime)
        logical_key = f"mid:{message_id}" if message_id else f"sha256:{message_hash}"

        existing = records.get(logical_key)
        if existing is None:
            records[logical_key] = new_record(
                key=logical_key,
                message_id=message_id,
                subject=subject,
                message_ts=message_ts,
                first_seen_ts=min(message_ts, observed_at_epoch),
                content_hash=message_hash,
                mailbox=mailbox,
                rel_path=rel_path,
                size_bytes=file_stat.st_size,
                file_mtime=file_stat.st_mtime,
            )
        else:
            update_record(
                record=existing,
                message_id=message_id,
                subject=subject,
                message_ts=message_ts,
                content_hash=message_hash,
                mailbox=mailbox,
                rel_path=rel_path,
                size_bytes=file_stat.st_size,
                file_mtime=file_stat.st_mtime,
            )

        stats["files_scanned"] += 1

    if scan_errors:
        sample = "; ".join(scan_errors[:5])
        raise BackupError(
            f"Could not read {len(scan_errors)} Maildir files while building the manifest. Sample: {sample}"
        )

    return {
        "schema_version": CURRENT_MANIFEST_SCHEMA_VERSION,
        "generated_at": utc_now().isoformat(),
        "maildir": str(maildir_root),
        "ignore_mailbox_regex": ignore_mailbox_regex,
        "stats": {
            **stats,
            "unique_messages": len(records),
        },
        "records": freeze_manifest_records(records),
    }


def load_manifest(path: Path) -> dict[str, Any]:
    validate_manifest_checksum(path)
    with path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    validate_manifest(path, manifest)
    return manifest


def manifest_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def checksum_path(path: Path) -> Path:
    return path.with_name(path.name + ".sha256")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_checksum(path: Path) -> None:
    checksum_file = checksum_path(path)
    with checksum_file.open("w", encoding="utf-8") as handle:
        handle.write(file_sha256(path) + "\n")


def validate_manifest_checksum(path: Path) -> None:
    checksum_file = checksum_path(path)
    if not checksum_file.is_file():
        return

    expected = checksum_file.read_text(encoding="utf-8").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise BackupError(f"Invalid checksum file for manifest: {checksum_file}")

    actual = file_sha256(path)
    if actual != expected:
        raise BackupError(f"Manifest checksum mismatch: {path}")


def validate_manifest(path: Path, manifest: Any) -> None:
    if not isinstance(manifest, dict):
        raise BackupError(f"Manifest is not a JSON object: {path}")

    schema_version = manifest_int(manifest.get("schema_version"))
    if schema_version not in {1, CURRENT_MANIFEST_SCHEMA_VERSION}:
        raise BackupError(f"Unsupported manifest schema_version in {path}: {manifest.get('schema_version')}")

    records = manifest.get("records")
    if not isinstance(records, dict):
        raise BackupError(f"Manifest records must be an object: {path}")

    for logical_key, record in records.items():
        if not isinstance(logical_key, str):
            raise BackupError(f"Manifest record key is not a string in {path}")
        if not isinstance(record, dict):
            raise BackupError(f"Manifest record is not an object for {logical_key} in {path}")
        if record.get("key") != logical_key:
            raise BackupError(f"Manifest record key mismatch for {logical_key} in {path}")

        if record.get("message_id") is not None and not isinstance(record.get("message_id"), str):
            raise BackupError(f"Manifest message_id must be null or string for {logical_key} in {path}")
        if record.get("subject") is not None and not isinstance(record.get("subject"), str):
            raise BackupError(f"Manifest subject must be null or string for {logical_key} in {path}")
        if record.get("message_ts") is not None and manifest_int(record.get("message_ts")) is None:
            raise BackupError(f"Manifest message_ts must be null or integer for {logical_key} in {path}")
        if record.get("first_seen_ts") is not None and manifest_int(record.get("first_seen_ts")) is None:
            raise BackupError(f"Manifest first_seen_ts must be null or integer for {logical_key} in {path}")

        occurrences = manifest_int(record.get("occurrences"))
        if occurrences is None or occurrences < 1:
            raise BackupError(f"Manifest occurrences must be >= 1 for {logical_key} in {path}")

        size_bytes = manifest_int(record.get("size_bytes"))
        if size_bytes is None or size_bytes < 0:
            raise BackupError(f"Manifest size_bytes must be >= 0 for {logical_key} in {path}")

        latest_file_mtime = manifest_int(record.get("latest_file_mtime"))
        if latest_file_mtime is None or latest_file_mtime < 0:
            raise BackupError(f"Manifest latest_file_mtime must be >= 0 for {logical_key} in {path}")

        for field_name in ("mailboxes", "sample_paths", "content_hashes"):
            value = record.get(field_name)
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                raise BackupError(f"Manifest {field_name} must be a list of strings for {logical_key} in {path}")


def carried_first_seen_ts(previous_record: dict[str, Any], default_epoch: int) -> int:
    first_seen_ts = manifest_int(previous_record.get("first_seen_ts"))
    if first_seen_ts is not None:
        return first_seen_ts

    message_ts = manifest_int(previous_record.get("message_ts"))
    if message_ts is None:
        return default_epoch
    return min(message_ts, default_epoch)


def merge_manifest_history(previous_manifest: dict[str, Any], current_manifest: dict[str, Any]) -> dict[str, Any]:
    previous_records = previous_manifest.get("records", {})
    current_records = current_manifest.get("records", {})

    for logical_key, current_record in current_records.items():
        previous_record = previous_records.get(logical_key)
        if previous_record is None:
            continue

        default_epoch = int(current_record["first_seen_ts"])
        current_record["first_seen_ts"] = carried_first_seen_ts(previous_record, default_epoch)

    current_manifest["schema_version"] = CURRENT_MANIFEST_SCHEMA_VERSION
    return current_manifest


def stable_record(record: dict[str, Any], cutoff_epoch: int) -> bool:
    first_seen_ts = manifest_int(record.get("first_seen_ts"))
    if first_seen_ts is not None:
        return first_seen_ts <= cutoff_epoch

    message_ts = manifest_int(record.get("message_ts"))
    if message_ts is None:
        return False
    return int(message_ts) <= cutoff_epoch


def format_epoch(epoch: int | None) -> str:
    if epoch is None:
        return "unknown"
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime("%Y-%m-%d")


def sample_missing_item(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": record["key"],
        "message_id": record.get("message_id"),
        "subject": record.get("subject"),
        "message_date": format_epoch(record.get("message_ts")),
        "mailboxes": record.get("mailboxes", []),
        "sample_paths": record.get("sample_paths", []),
        "content_hashes": record.get("content_hashes", []),
        "occurrences": record.get("occurrences", 0),
    }


def sample_mutated_item(baseline_record: dict[str, Any], current_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": baseline_record["key"],
        "message_id": baseline_record.get("message_id"),
        "subject": baseline_record.get("subject") or current_record.get("subject"),
        "message_date": format_epoch(baseline_record.get("message_ts")),
        "mailboxes": current_record.get("mailboxes", []),
        "baseline_hashes": baseline_record.get("content_hashes", []),
        "current_hashes": current_record.get("content_hashes", []),
        "sample_paths": current_record.get("sample_paths", []),
        "occurrences": current_record.get("occurrences", 0),
    }


def sample_placement_change_item(
    baseline_record: dict[str, Any],
    current_record: dict[str, Any],
) -> dict[str, Any]:
    return {
        "key": baseline_record["key"],
        "message_id": baseline_record.get("message_id"),
        "subject": baseline_record.get("subject") or current_record.get("subject"),
        "message_date": format_epoch(baseline_record.get("message_ts")),
        "baseline_mailboxes": baseline_record.get("mailboxes", []),
        "current_mailboxes": current_record.get("mailboxes", []),
        "baseline_occurrences": baseline_record.get("occurrences", 0),
        "current_occurrences": current_record.get("occurrences", 0),
        "baseline_paths": baseline_record.get("sample_paths", []),
        "current_paths": current_record.get("sample_paths", []),
    }


def compare_manifests(
    baseline_manifest: dict[str, Any],
    current_manifest: dict[str, Any],
    immutability_days: int,
    sample_limit: int,
) -> dict[str, Any]:
    baseline_records = baseline_manifest.get("records", {})
    current_records = current_manifest.get("records", {})
    cutoff_epoch = int(time.time() - immutability_days * 86400)

    missing = []
    mutated = []
    placement_changes = []

    for key, baseline_record in baseline_records.items():
        if not stable_record(baseline_record, cutoff_epoch):
            continue
        if key not in current_records:
            missing.append(sample_missing_item(baseline_record))

    for key in sorted(set(baseline_records) & set(current_records)):
        baseline_record = baseline_records[key]
        current_record = current_records[key]
        if not stable_record(baseline_record, cutoff_epoch):
            continue
        if sorted(baseline_record.get("content_hashes", [])) != sorted(current_record.get("content_hashes", [])):
            mutated.append(sample_mutated_item(baseline_record, current_record))
        if (
            sorted(baseline_record.get("mailboxes", [])) != sorted(current_record.get("mailboxes", []))
            or int(baseline_record.get("occurrences", 0)) != int(current_record.get("occurrences", 0))
        ):
            placement_changes.append(sample_placement_change_item(baseline_record, current_record))

    if missing or mutated or placement_changes:
        status = "alert"
    else:
        status = "ok"

    return {
        "generated_at": utc_now().isoformat(),
        "summary": {
            "status": status,
            "immutability_days": immutability_days,
            "baseline_unique_messages": len(baseline_records),
            "current_unique_messages": len(current_records),
            "missing_stable_count": len(missing),
            "mutated_stable_count": len(mutated),
            "placement_changed_stable_count": len(placement_changes),
        },
        "samples": {
            "missing_stable_messages": missing[:sample_limit],
            "mutated_stable_messages": mutated[:sample_limit],
            "placement_changed_stable_messages": placement_changes[:sample_limit],
        },
        "details": {
            "missing_stable_messages": missing,
            "mutated_stable_messages": mutated,
            "placement_changed_stable_messages": placement_changes,
        },
    }


def baseline_init_report(immutability_days: int) -> dict[str, Any]:
    return {
        "generated_at": utc_now().isoformat(),
        "summary": {
            "status": "baseline-init",
            "immutability_days": immutability_days,
            "baseline_unique_messages": 0,
            "current_unique_messages": 0,
            "missing_stable_count": 0,
            "mutated_stable_count": 0,
            "placement_changed_stable_count": 0,
        },
        "samples": {
            "missing_stable_messages": [],
            "mutated_stable_messages": [],
            "placement_changed_stable_messages": [],
        },
        "details": {
            "missing_stable_messages": [],
            "mutated_stable_messages": [],
            "placement_changed_stable_messages": [],
        },
    }


def render_samples(label: str, items: list[dict[str, Any]]) -> list[str]:
    if not items:
        return []
    lines = [label]
    for item in items:
        subject = item.get("subject") or "(no subject)"
        message_id = item.get("message_id") or item["key"]
        mailboxes = ", ".join(item.get("mailboxes", [])) or "-"
        sample_paths = ", ".join(item.get("sample_paths", [])) or "-"
        lines.append(
            f"- {item['message_date']} | {subject} | {message_id} | mailboxes={mailboxes} | paths={sample_paths}"
        )
    return lines


def render_placement_samples(label: str, items: list[dict[str, Any]]) -> list[str]:
    if not items:
        return []

    lines = [label]
    for item in items:
        subject = item.get("subject") or "(no subject)"
        message_id = item.get("message_id") or item["key"]
        baseline_mailboxes = ", ".join(item.get("baseline_mailboxes", [])) or "-"
        current_mailboxes = ", ".join(item.get("current_mailboxes", [])) or "-"
        lines.append(
            (
                f"- {item['message_date']} | {subject} | {message_id} | "
                f"baseline_mailboxes={baseline_mailboxes} | current_mailboxes={current_mailboxes} | "
                f"baseline_occurrences={item.get('baseline_occurrences', 0)} | "
                f"current_occurrences={item.get('current_occurrences', 0)}"
            )
        )
    return lines


def render_report_text(report: dict[str, Any], sample_limit: int) -> str:
    summary = report["summary"]
    samples = report["samples"]

    lines = [
        f"Status: {summary['status'].upper()}",
        f"Stable messages missing: {summary['missing_stable_count']}",
        f"Stable messages mutated: {summary['mutated_stable_count']}",
        f"Stable messages with mailbox/count changes: {summary['placement_changed_stable_count']}",
        f"Immutability window (days): {summary['immutability_days']}",
        f"Baseline unique messages: {summary['baseline_unique_messages']}",
        f"Current unique messages: {summary['current_unique_messages']}",
    ]

    if summary["status"] == "baseline-init":
        lines.append("")
        lines.append("No previous baseline exists. The current Maildir state will become the baseline after a successful backup.")

    if samples["missing_stable_messages"]:
        lines.append("")
        lines.extend(render_samples("Missing samples:", samples["missing_stable_messages"]))

    if samples["mutated_stable_messages"]:
        lines.append("")
        lines.extend(render_samples("Mutated samples:", samples["mutated_stable_messages"]))

    if samples["placement_changed_stable_messages"]:
        lines.append("")
        lines.extend(
            render_placement_samples(
                "Mailbox/count change samples:",
                samples["placement_changed_stable_messages"],
            )
        )

    truncated = False
    if len(report["details"]["missing_stable_messages"]) > sample_limit:
        lines.append(
            f"  ... {len(report['details']['missing_stable_messages']) - sample_limit} more missing messages not shown"
        )
        truncated = True

    if len(report["details"]["mutated_stable_messages"]) > sample_limit:
        lines.append(
            f"  ... {len(report['details']['mutated_stable_messages']) - sample_limit} more mutated messages not shown"
        )
        truncated = True

    if len(report["details"]["placement_changed_stable_messages"]) > sample_limit:
        lines.append(
            (
                f"  ... {len(report['details']['placement_changed_stable_messages']) - sample_limit} "
                "more mailbox/count changes not shown"
            )
        )
        truncated = True

    if truncated:
        lines.append("")
        lines.append("Full details in the JSON report (see 'details' key).")

    return "\n".join(lines) + "\n"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_text(path: Path, content: str) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write(content)


def write_manifest_file(path: Path, payload: dict[str, Any]) -> None:
    write_json(path, payload)
    write_checksum(path)


def is_file_due_by_days(path: Path, interval_days: int) -> bool:
    if not path.is_file():
        return True
    age_seconds = max(0.0, time.time() - path.stat().st_mtime)
    return age_seconds >= interval_days * 86400


class EmailBackupApp:
    def __init__(self, config: Config, runtime: Runtime) -> None:
        self.config = config
        self.runtime = runtime

    @property
    def baseline_manifest(self) -> Path:
        return self.runtime.paths.manifest_dir / "baseline.json"

    @property
    def latest_manifest(self) -> Path:
        return self.runtime.paths.manifest_dir / "latest.json"

    def current_manifest_path(self) -> Path:
        return self.runtime.paths.manifest_dir / f"manifest-{self.runtime.run_id}.json"

    def report_json_path(self) -> Path:
        return self.runtime.paths.report_dir / f"report-{self.runtime.run_id}.json"

    def report_text_path(self) -> Path:
        return self.runtime.paths.report_dir / f"report-{self.runtime.run_id}.txt"

    def run(self) -> int:
        current_manifest_path = self.current_manifest_path()
        report_json_path = self.report_json_path()
        report_text_path = self.report_text_path()

        self.run_mbsync()
        current_manifest = build_manifest(self.config.maildir_path, self.config.ignore_mailbox_regex)
        write_json(current_manifest_path, current_manifest)

        if self.baseline_manifest.is_file():
            self.runtime.log("INFO", "Comparing current Maildir state against baseline")
            baseline_manifest = load_manifest(self.baseline_manifest)
            report = compare_manifests(
                baseline_manifest=baseline_manifest,
                current_manifest=current_manifest,
                immutability_days=self.config.immutability_days,
                sample_limit=self.config.report_sample_limit,
            )
        else:
            self.runtime.log("INFO", "No baseline found, this run will initialize it after a successful backup")
            report = baseline_init_report(self.config.immutability_days)
            report["summary"]["current_unique_messages"] = current_manifest["stats"]["unique_messages"]

        report_text = render_report_text(report, self.config.report_sample_limit)
        write_json(report_json_path, report)
        write_text(report_text_path, report_text)

        self.run_restic_backup([current_manifest_path, report_json_path, report_text_path])
        self.maybe_run_restic_check()

        status = str(report["summary"]["status"])
        if status == "alert":
            self.handle_integrity_alert(report, report_text_path, current_manifest_path)
            self.runtime.log_raw("\n" + report_text + "\n")
            return ALERT_EXIT_CODE

        if status == "baseline-init":
            self.runtime.log("INFO", "Initializing baseline after first successful backup")
        else:
            self.runtime.log("INFO", "Audit completed without stable-message anomalies")

        self.promote_baseline(current_manifest_path, preserve_history=True)
        self.runtime.log("INFO", f"Baseline updated: {self.baseline_manifest}")
        self.runtime.log("INFO", f"Audit report: {report_json_path}")
        self.runtime.log("INFO", f"Audit summary: {report_text_path}")
        self.runtime.log_raw("\n" + report_text + "\n")
        return 0

    def rebaseline(self) -> int:
        current_manifest_path = self.current_manifest_path()
        manifest = build_manifest(self.config.maildir_path, self.config.ignore_mailbox_regex)
        write_json(current_manifest_path, manifest)
        self.run_restic_backup([current_manifest_path])
        self.maybe_run_restic_check()
        self.promote_baseline(current_manifest_path, preserve_history=False)
        self.runtime.log("INFO", "Current Maildir state accepted as the new baseline after a successful backup")
        return 0

    def run_mbsync(self) -> None:
        self.runtime.run_command(self.config.mbsync_command)

    def run_restic_backup(self, extra_paths: list[Path]) -> None:
        paths = [str(self.config.maildir_path)]
        if self.config.restic_include_state_dir:
            paths.append(str(self.config.state_dir))
        else:
            paths.extend(str(path) for path in extra_paths)

        command = [
            *self.config.restic_command,
            "-r",
            self.config.restic_repo,
            "backup",
            *self.config.restic_backup_args,
            *paths,
        ]
        self.runtime.run_command(command)

    def maybe_run_restic_check(self) -> None:
        interval_days = self.config.restic_check_interval_days
        if interval_days == 0:
            self.runtime.log("INFO", "Skipping restic check because restic_check_interval_days=0")
            return

        if not is_file_due_by_days(self.runtime.paths.restic_check_stamp, interval_days):
            self.runtime.log("INFO", "Skipping restic check because it is not due yet")
            return

        command = [
            *self.config.restic_command,
            "-r",
            self.config.restic_repo,
            "check",
            *self.config.restic_check_args,
        ]
        self.runtime.run_command(command)
        self.runtime.paths.restic_check_stamp.touch()

    def promote_baseline(self, source_manifest: Path, preserve_history: bool) -> None:
        promoted_manifest = load_manifest(source_manifest)
        if preserve_history and self.baseline_manifest.is_file():
            previous_manifest = load_manifest(self.baseline_manifest)
            promoted_manifest = merge_manifest_history(previous_manifest, promoted_manifest)

        write_manifest_file(self.latest_manifest, promoted_manifest)
        write_manifest_file(self.baseline_manifest, promoted_manifest)
        self.runtime.log("INFO", f"Baseline updated: {self.baseline_manifest}")

    def store_latest_only(self, source_manifest: Path) -> None:
        latest_manifest = load_manifest(source_manifest)
        write_manifest_file(self.latest_manifest, latest_manifest)
        self.runtime.log("INFO", "Latest manifest updated without promoting baseline")

    def handle_integrity_alert(
        self,
        report: dict[str, Any],
        report_text_path: Path,
        current_manifest_path: Path,
    ) -> None:
        summary = report["summary"]
        self.runtime.log("ERROR", "Stable-message anomalies detected")
        self.runtime.log("ERROR", f"Missing stable messages: {summary['missing_stable_count']}")
        self.runtime.log("ERROR", f"Mutated stable messages: {summary['mutated_stable_count']}")
        self.runtime.log(
            "ERROR",
            f"Stable messages with mailbox/count changes: {summary['placement_changed_stable_count']}",
        )
        self.runtime.send_alert(
            "ALERT",
            "Email backup integrity alert",
            report_text_path.read_text(encoding="utf-8"),
        )
        self.store_latest_only(current_manifest_path)
        self.runtime.log("INFO", f"Audit report: {self.report_json_path()}")
        self.runtime.log("INFO", f"Audit summary: {report_text_path}")


def notify_runtime_failure(runtime: Runtime, exc: Exception) -> None:
    message = str(exc)
    runtime.log("ERROR", message)
    body = "\n".join(
        [
            "Status: ERROR",
            f"Error: {message}",
            f"Run log: {runtime.run_log_path}",
        ]
    )
    runtime.send_alert("ERROR", "Email backup run failed", body)


def parse_args() -> argparse.Namespace:
    default_config = Path(__file__).resolve().parent / "email-backup.toml"
    parser = argparse.ArgumentParser(
        description=(
            "Back up a Maildir with restic and alert when stable messages disappear, mutate, move, "
            "or lose copies."
        )
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=("run", "rebaseline"),
        default="run",
        help="run the backup flow or accept the current Maildir state as the new baseline",
    )
    parser.add_argument(
        "--config",
        default=str(default_config),
        help="Path to the TOML config file (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = Config.load(Path(args.config))
    except BackupError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    runtime: Runtime | None = None
    try:
        runtime = Runtime.start(config)
        app = EmailBackupApp(config, runtime)
        if args.command == "rebaseline":
            return app.rebaseline()
        return app.run()
    except Exception as exc:
        if runtime is not None:
            notify_runtime_failure(runtime, exc)
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if runtime is not None:
            runtime.close()


if __name__ == "__main__":
    raise SystemExit(main())
