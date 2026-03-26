# Known Limitations

These are real limits of the current approach that are either not cleanly fixable inside this script, or depend on your operating environment more than on Python code.

## Baseline Integrity Is Only Locally Checked

`baseline.json` and `latest.json` now have local SHA-256 sidecars. That catches accidental edits and many corruption cases before the manifest is trusted again.

It is not tamper-proof. Anything that can rewrite both the manifest and its `.sha256` file can still forge the local trust anchor. For stronger guarantees, keep the `restic` repository off-host and use storage-side immutability, retention locks, or another external trust boundary.

## Maildir Scans See A Live Filesystem

The script locks only itself. It cannot stop another mail client, sync tool, or server-side mirror from modifying the Maildir while the scan is running.

If you need a stronger guarantee than "best effort on a live Maildir", run backups from an isolated sync host, quiesce other mail access during the scan window, or scan a filesystem snapshot.

## Alert Delivery Is Single-Channel

If `alert_command` fails, the failure is logged and the process still exits non-zero when the underlying run failed or integrity checks fired. There is no second independent notification path built into the tool.

For production use, monitor the job's exit status in `cron`, `launchd`, `systemd`, or your scheduler of choice, and treat alert-command failures as an operational problem.

## State Retention Is Not Automatic

Logs, manifests, reports, and checksum sidecars accumulate over time. This repository does not impose a built-in pruning policy because the right retention window depends on how much audit history you want to keep.

If you run the job frequently, add an external retention policy for `state_dir`, for example with `tmpfiles.d`, `logrotate`, or a scheduled cleanup script.
