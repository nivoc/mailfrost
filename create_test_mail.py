#!/usr/bin/env python3

from __future__ import annotations

import argparse
import imaplib
import random
import secrets
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def build_message(username: str, sent_at: datetime, token: str) -> EmailMessage:
    domain = username.split("@", 1)[1] if "@" in username else "example.invalid"
    message = EmailMessage()
    message["From"] = username
    message["To"] = username
    message["Subject"] = f"mailfrost synthetic old mail {token}"
    message["Date"] = sent_at.strftime("%a, %d %b %Y %H:%M:%S +0000")
    message["Message-ID"] = f"<mailfrost-{token}@{domain}>"
    message.set_content(
        "\n".join(
            [
                "Synthetic old test message for Mailfrost.",
                "Safe to delete after testing.",
                f"Token: {token}",
            ]
        )
    )
    return message


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Append a synthetic old test mail to an IMAP mailbox using credentials from .env."
    )
    parser.add_argument("--env", default=".env", help="Path to the .env file")
    parser.add_argument("--mailbox", default="INBOX", help="Target mailbox on the IMAP server")
    parser.add_argument(
        "--min-days-ago",
        type=int,
        default=60,
        help="Minimum age in days for the synthetic message",
    )
    parser.add_argument(
        "--max-days-ago",
        type=int,
        default=3650,
        help="Maximum age in days for the synthetic message",
    )
    args = parser.parse_args()

    if args.min_days_ago < 1 or args.max_days_ago < args.min_days_ago:
        raise SystemExit("invalid day range")

    env_path = Path(args.env).resolve()
    env = parse_env_file(env_path)
    host = env.get("IMAP_HOST", "")
    port = int(env.get("IMAP_PORT", "993"))
    username = env.get("IMAP_USERNAME", "")
    password = env.get("IMAP_PASSWORD", "")
    if not host or not username or not password:
        raise SystemExit("IMAP_HOST, IMAP_USERNAME, and IMAP_PASSWORD must be set in .env")

    days_ago = random.randint(args.min_days_ago, args.max_days_ago)
    sent_at = datetime.now(timezone.utc) - timedelta(days=days_ago, hours=random.randint(0, 23), minutes=random.randint(0, 59))
    token = secrets.token_hex(6)
    message = build_message(username, sent_at, token)

    connection = imaplib.IMAP4_SSL(host, port)
    try:
        connection.login(username, password)
        status, data = connection.append(args.mailbox, "", sent_at, message.as_bytes())
        if status != "OK":
            raise SystemExit(f"append failed: {status} {data!r}")
    finally:
        try:
            connection.logout()
        except Exception:
            pass

    print(f"mailbox={args.mailbox}")
    print(f"days_ago={days_ago}")
    print(f"subject={message['Subject']}")
    print(f"message_id={message['Message-ID']}")
    print(f"internal_date={sent_at.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
