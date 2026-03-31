# Mail Backup

`mail-backup` is a standalone Go tool for Maildir-based mail backup and integrity auditing.

It does four things in one run:

- syncs mail with `mbsync`
- scans the local Maildir into a manifest
- alerts if stable mail disappears, mutates, changes mailbox placement, or loses copies
- backs up the Maildir and tool state with `kopia`

Unlike `ical-backup`, this tool maintains a trusted baseline for old mail. If a stable message vanishes or changes unexpectedly, the run exits with an alert instead of silently accepting the new state.

## Requirements

- Go 1.26.1 or newer
- `mbsync`
- `kopia`
- one IMAP account that `mbsync` can mirror into a local Maildir

For stronger guarantees, keep the Kopia repository off-host and use storage-side immutability where possible.

## Setup

Run the interactive setup wizard:

```bash
go run ./cmd/mail-backup setup
```

The wizard will:

1. prompt for IMAP host, username, password, and port
2. generate the internal `mbsync` config file
3. prompt for Kopia repository type and credentials
4. optionally create or connect the Kopia repository
5. write `.env`
6. create local working directories

You can rerun `setup` at any time to refresh values.

## Usage

Build the binary:

```bash
make build
./mail-backup
```

Run a normal backup:

```bash
./mail-backup backup
```

Accept the current Maildir as the new baseline:

```bash
./mail-backup rebaseline
```

Restore a snapshot into a staging directory:

```bash
./mail-backup restore
```

Restore a specific snapshot:

```bash
./mail-backup restore -snapshot <id>
```

Use custom config and/or env paths:

```bash
./mail-backup --config /path/to/config --env /path/to/.env backup
```

## Configuration

The tool uses the same split configuration model as `ical-backup`:

1. `.env` for secrets and Kopia repository settings
2. `config.advanced` for tracked operational defaults
3. optional local `config` for overrides

### `.env`

Created by `setup`. Typical values:

```dotenv
KOPIA_CONFIG_PATH=./data/kopia/repository.config
KOPIA_PASSWORD=generated-password
KOPIA_REPO_TYPE=filesystem
KOPIA_REPO_PATH=./data/kopia/repo

IMAP_HOST=imap.example.com
IMAP_PORT=993
IMAP_USERNAME=user@example.com
IMAP_PASSWORD=app-password
```

For S3 repositories, `.env` also contains:

```dotenv
KOPIA_REPO_TYPE=s3
KOPIA_S3_BUCKET=your-bucket
KOPIA_S3_ENDPOINT=s3.<region>.wasabisys.com
KOPIA_S3_PREFIX=mail-backup
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

### `config.advanced` and `config`

These files contain non-secret operational settings:

- `MAILDIR_PATH`
- `STATE_DIR`
- `REPORT_SAMPLE_LIMIT`
- `IMMUTABILITY_DAYS`
- `IGNORE_MAILBOX_REGEX`
- `KOPIA_MAINTENANCE_INTERVAL_DAYS`
- `KOPIA_INCLUDE_STATE_DIR`
- `MBSYNC_CONFIG_PATH`
- `MBSYNC_COMMAND`
- `KOPIA_COMMAND`
- `KOPIA_SNAPSHOT_ARGS`
- `ALERT_COMMAND`

## How It Works

The backup flow:

- acquires a lock in `state_dir/.lock`
- regenerates the `mbsync` config from `.env`
- runs `mbsync`
- builds the current Maildir manifest
- compares it to `state_dir/manifests/baseline.json`
- writes JSON and text reports under `state_dir/reports/`
- snapshots the Maildir with `kopia`
- snapshots the state directory too unless disabled
- runs periodic `kopia maintenance`
- promotes the new baseline only after a clean backup with no integrity alert

State stored under `state_dir`:

- `logs/`
- `manifests/baseline.json`
- `manifests/latest.json`
- `manifests/*.sha256`
- `reports/*.json`
- `reports/*.txt`
- `alerts.log`

## Exit Codes

- `0`: success
- `1`: config error, scan failure, command failure, or restore/setup failure
- `2`: integrity alert

## Alerting

If `ALERT_COMMAND` is set, it runs with `bash -c` semantics and receives the alert body on stdin.

These environment variables are provided:

- `MAIL_BACKUP_STATUS`
- `MAIL_BACKUP_ALERT_SUBJECT`
- `MAIL_BACKUP_ALERT_BODY`

Example mail alert:

```bash
ALERT_COMMAND='mail -s "$MAIL_BACKUP_ALERT_SUBJECT" you@example.com'
```

## Restore

`restore` is a local filesystem restore from Kopia snapshots.

- default target: `./restored/<snapshot-id>`
- `-target` overrides the destination
- `-force` is required for restoring directly into the configured `MAILDIR_PATH`

The tool does not upload mail back to IMAP or perform server-side replay.

## Operational Notes

- The tool ignores `Trash`, `Junk`, `Spam`, and `Drafts` style folders by default.
- Identity is based on normalized `Message-ID`, with content-hash fallback when `Message-ID` is missing.
- Stability is based on `first_seen_ts` carried forward in trusted manifests.
- The scan fails closed if some Maildir files cannot be read.
- Baseline and latest manifests use local SHA-256 sidecars so accidental edits are detected before they are trusted again.
- `mbsync` still uses a config file internally, but this tool generates it automatically from `.env` and `MAILDIR_PATH`.
- The generated `mbsync` config is pull-only and uses `Expunge Near`, so deletions on the server are removed locally and can be detected by the audit.

## When To Rebaseline

Use `rebaseline` only for intentional historical changes such as:

- mailbox migration
- deliberate archival re-import
- one-time cleanup of broken duplicates

Do not rebaseline just to hide an unexplained alert.
