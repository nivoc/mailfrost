#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR"

if ! command -v restic >/dev/null 2>&1; then
  echo "restic is not installed or not in PATH" >&2
  exit 1
fi

mkdir -p \
  data/maildir/INBOX/cur \
  data/maildir/INBOX/new \
  data/maildir/INBOX/tmp \
  data/state \
  data/restic \
  data/restic-repo

PASSWORD_FILE="data/restic/password"
REPO_DIR="data/restic-repo"

if [ ! -f "$PASSWORD_FILE" ]; then
  umask 077
  openssl rand -base64 32 > "$PASSWORD_FILE"
  chmod 600 "$PASSWORD_FILE"
  echo "Created $PASSWORD_FILE"
else
  chmod 600 "$PASSWORD_FILE"
  echo "Keeping existing $PASSWORD_FILE"
fi

if [ ! -f "$REPO_DIR/config" ]; then
  RESTIC_PASSWORD_FILE="$PASSWORD_FILE" restic -r "$REPO_DIR" init
else
  echo "Keeping existing restic repo in $REPO_DIR"
fi

echo
echo "Local setup is ready."
echo "Next:"
echo "1. Check mbsyncrc"
echo "2. Run: mbsync -c ./mbsyncrc mail-backup"
echo "3. Run: ./email_backup.py"
