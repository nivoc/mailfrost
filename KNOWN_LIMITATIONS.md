# Known Limitations

These are real limits of the current approach that depend more on the operating environment than on application code.

## Baseline Integrity Is Only Locally Checked

`baseline.json` and `latest.json` have local SHA-256 sidecars. That catches accidental edits and many corruption cases before the manifest is trusted again.

Anything that can rewrite both the manifest and its `.sha256` file can still forge the local trust anchor. For stronger guarantees, keep the `kopia` repository off-host and use storage-side immutability, retention locks, or another external trust boundary.

## Maildir Scans See A Live Filesystem

The tool locks only itself. It cannot stop another mail client, sync tool, or server-side mirror from modifying the Maildir while the scan is running.

If you need a stronger guarantee than "best effort on a live Maildir", run backups from an isolated sync host, quiesce other mail access during the scan window, or scan a filesystem snapshot.

## Alert Delivery Is Single-Channel

If `alert_command` fails, the failure is logged and the process still exits non-zero when the underlying run failed or integrity checks fired. There is no second independent notification path built into the tool.

For production use, monitor the job's exit status in `cron`, `launchd`, `systemd`, or your scheduler of choice, and treat alert-command failures as an operational problem.

## Restore Is Filesystem-Only

The tool can restore a Maildir snapshot from Kopia into a local directory, but it does not push messages back to IMAP or replay them to the server.

If you need server-side recovery, restore locally first and then use a separate, explicit process to re-import or resync the recovered mail.

## Recover Is Destructive For Managed IMAP Mailboxes

`recover` is intentionally not a smart bidirectional sync. It clears the managed IMAP mailboxes and then repopulates them from the restored snapshot through a dedicated temporary `mbsync` push configuration with isolated `SyncState`.

Before clearing those mailboxes, the tool now copies current managed remote mail into a safety mailbox tree on the server. That gives you a rescue point for mail that arrived after the chosen snapshot, but a failed recovery can still leave the managed server state only partially rebuilt until you rerun recovery or restore from another snapshot.

## State Retention Is Not Automatic

Logs, manifests, reports, and checksum sidecars accumulate over time. This repository does not impose a built-in pruning policy because the right retention window depends on how much audit history you want to keep.
