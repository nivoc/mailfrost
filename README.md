# Email Backup Monitor

This repository contains a single Python program, `email_backup.py`, for a Maildir-based backup workflow:

- sync mail with `mbsync`
- build a Maildir manifest
- compare the current Maildir to the last known-good baseline
- back up the Maildir with `restic`
- alert if old messages disappear, change, move mailboxes, or lose copies

The core idea is simple: once a message has been present long enough to be considered stable, the tool expects that message to remain unchanged. If a mail client, sync bug, or server-side mistake deletes, rewrites, moves, or silently drops copies of old mail, the run exits non-zero and produces an alert report.

## Requirements

- Python 3.11 or newer
- `mbsync`
- `restic`
- a working Maildir sync setup
- a `restic` repository that you can already write to

For stronger guarantees, use an off-host `restic` repository and storage-side immutability or retention controls where possible.

## Setup

1. Copy the example config:

   ```bash
   cp email-backup.toml.example email-backup.toml
   ```

2. Edit `email-backup.toml`:

   - set `maildir_path`
   - set `restic_repo`
   - set `env.RESTIC_PASSWORD_FILE` or other required `restic` credentials
   - adjust `immutability_days`
   - optionally add an `alert_command`

3. Make sure your `restic` repo exists. If not, initialize it once:

   ```bash
   RESTIC_PASSWORD_FILE=~/.config/restic/mail-backup-password restic -r /path/to/repo init
   ```

4. Run the first backup:

   ```bash
   ./email_backup.py
   ```

The first successful run creates the baseline. From then on, each run compares the current Maildir to that baseline before promoting a new one.

## How It Works

- `run`
  - runs `mbsync`
  - scans the Maildir
  - compares the scan to `state_dir/manifests/baseline.json`
  - runs `restic backup`
  - optionally runs periodic `restic check`
  - promotes the new manifest to the baseline only after a successful backup

- `rebaseline`
  - accepts the current Maildir state as the new baseline
  - runs a `restic backup` first so the accepted state is recoverable
  - use this only after an intentional bulk change, migration, or cleanup that you trust

The script stores state in `state_dir`:

- `logs/`: one run log per execution
- `manifests/baseline.json`: the last known-good manifest
- `manifests/latest.json`: the most recent manifest, even after alerts
- `manifests/*.sha256`: local checksum sidecars for baseline/latest manifests
- `reports/`: JSON and text reports for each run
- `alerts.log`: a local history of alerts and failures

## Usage

Run the normal backup flow:

```bash
./email_backup.py
```

Use a custom config path:

```bash
./email_backup.py --config /path/to/email-backup.toml
```

Accept the current state as the new known-good baseline:

```bash
./email_backup.py rebaseline
```

## Exit Codes

- `0`: success
- `1`: operational failure, config error, scan failure, or command failure
- `2`: integrity alert, meaning stable messages disappeared or changed

## Alerting

If `alert_command` is set, it is executed with `bash -c` semantics. The report body is passed on stdin.

These environment variables are provided to the alert command:

- `EMAIL_BACKUP_STATUS`
- `EMAIL_BACKUP_ALERT_SUBJECT`
- `EMAIL_BACKUP_ALERT_BODY`

Example mail alert:

```toml
alert_command = 'mail -s "$EMAIL_BACKUP_ALERT_SUBJECT" you@example.com'
```

Example Telegram alert:

```toml
alert_command = 'curl -fsS -X POST "https://api.telegram.org/bot<token>/sendMessage" -d chat_id="<chat-id>" --data-urlencode text@"-"'
```

## Scheduling

### Cron

Run every hour:

```cron
0 * * * * cd /Users/you/path/to/mail-bck && /usr/bin/env python3 ./email_backup.py >> /tmp/email-backup-cron.log 2>&1
```

### launchd on macOS

If you prefer `launchd`, create a plist that runs:

```bash
/usr/bin/env python3 /Users/you/path/to/mail-bck/email_backup.py
```

Point it at the same working directory so the default config file is found.

## Operational Notes

- The tool intentionally ignores `Trash`, `Junk`, `Spam`, and `Drafts` style folders by default, including common Maildir names such as `.Trash` and `.Spam`.
- Message flags and Maildir filenames can change without being treated as corruption. The audit keys primarily on `Message-ID`, with a content-hash fallback when no `Message-ID` exists.
- Stability is based on when a message was first seen successfully by the tool when that history is available; older manifests without that field fall back to the message `Date` header.
- If the scan cannot read some Maildir files, the run fails closed instead of silently producing a partial audit.
- Message-ID collisions with different bodies are reported as warnings because they indicate ambiguous identity, but not always corruption.
- The baseline and latest manifests get local SHA-256 sidecars so accidental edits or corruption are detected before they are trusted again.

## Recommended First Run Checklist

1. Confirm `mbsync -a` already behaves the way you want.
2. Confirm the `restic` repo is initialized and reachable.
3. Run `./email_backup.py`.
4. Inspect the run log and the generated report in `state_dir/reports/`.
5. Trigger a test alert by temporarily breaking the `alert_command`, or by editing a synthetic test Maildir copy before running in a safe environment.

## When To Rebaseline

Use `rebaseline` only when you intentionally changed historical mail and want the tool to trust that new state going forward, for example:

- mailbox migration
- deliberate archival re-import
- one-time cleanup of broken duplicate messages

Do not rebaseline just to make an alert disappear unless you understand exactly why the alert happened.
