# Mailfrost

Note: this is still a personal tool. Use it at your own risk. It has only been tested against Fastmail so far.

`Mailfrost` is a standalone Go tool for Maildir integrity tracking, confidence, and backup.

In normal use it does four things in one run:

- syncs mail with `mbsync`
- scans the local Maildir into a manifest
- tracks and hashes stable mail so you can detect when old messages disappear, mutate, move unexpectedly, or lose copies
- backs up the Maildir and tool state with `kopia`

Unlike `ical-backup`, this tool maintains a trusted baseline for old mail. The main purpose is confidence in your mailbox over time, especially in the face of faulty mail clients, sync agents, or accidental modifications. If stable mail changes unexpectedly, the run exits with an alert instead of silently accepting the new state.

## Requirements

- Go 1.26.1 or newer
- `mbsync`
- `kopia`
- one IMAP account that `mbsync` can mirror into a local Maildir

For stronger guarantees, keep the Kopia repository off-host and use storage-side immutability where possible.

## Quick Start

Build the binary:

```bash
make build
```

Run the interactive setup wizard:

```bash
./mailfrost setup
```

Then run the normal backup flow:

```bash
./mailfrost backup
```

That is the intended setup path.

`setup` will:

1. verify that `mbsync` and `kopia` are installed
2. prompt for IMAP host, username, password, and port
3. generate the internal `mbsync` config file
4. prompt for Kopia repository type and credentials
5. optionally create or connect the Kopia repository
6. set the Maildir compression policy in Kopia to `zstd`
7. write `.env`
8. create local working directories
9. print a short Kopia repository summary

You can rerun `setup` at any time to refresh values.

## Common Commands

Normal backup:

```bash
./mailfrost backup
```

Recover the managed IMAP mailboxes from a snapshot:

```bash
./mailfrost recover
```

Resume a failed recovery push without clearing mailboxes again:

```bash
./mailfrost recover-resume
```

Restore a snapshot into a local directory:

```bash
./mailfrost restore
```

Accept the current Maildir as the new baseline:

```bash
./mailfrost rebaseline
```

Show the tool version:

```bash
./mailfrost version
```

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
- prints a short Kopia repository summary
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

## Configuration

Most users do not need to edit config files manually. `setup` is the normal path.

The tool uses this split configuration model:

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
KOPIA_S3_PREFIX=mailfrost
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

## Restore

`restore` is a local filesystem restore from Kopia snapshots.

- default target: `./restored/<snapshot-id>`
- `-target` overrides the destination
- `-force` is required for restoring directly into the configured `MAILDIR_PATH`

The tool does not upload mail back to IMAP or perform server-side replay.

## Recover

`recover` is the destructive server-side recovery flow.

- it restores the selected Kopia snapshot into a staging Maildir under `state_dir/recoveries/`
- it can copy currently managed remote mail into a server-side safety mailbox tree before destructive recovery
- it rewrites the managed IMAP mailboxes from that staging Maildir using a dedicated temporary `mbsync` config
- it discovers snapshots by the Mailfrost purpose tag plus the configured IMAP account, so recovery works from a different local directory on another machine
- managed mailboxes are the mailboxes not matched by `IGNORE_MAILBOX_REGEX`
- it uses isolated temporary `mbsync` `SyncState`, separate from normal backup sync state
- interactive mode asks whether to create the safety copy and warns that it may temporarily require about 2x mailbox space
- non-interactive mode keeps the safety copy enabled by default
- interactive mode requires typed confirmation of the IMAP login user
- non-interactive mode requires both `-yes` and `-confirm-user <imap-username>`

Examples:

```bash
./mailfrost recover -snapshot <id>
./mailfrost recover -snapshot <id> -yes -confirm-user user@example.com
./mailfrost recover-resume
./mailfrost recover-resume -run 20260401T141455Z
```

Important behavior:

- before deleting anything, the tool copies current managed remote mail under a mailbox root like `Recovery-Safety-20260331T171900`
- in interactive mode you can decline the safety copy, but then newer mail in managed mailboxes has no automatic rescue path
- this deletes mail from the managed IMAP mailboxes before re-uploading the snapshot
- ignored folders such as `Trash`, `Junk`, `Spam`, and `Drafts` are left alone by default
- if the recovery `mbsync` push times out, rerun `recover-resume` to continue that push without clearing mailboxes again
- after a successful recovery, run `backup` and then `rebaseline` if the recovered state is now intentional truth

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
