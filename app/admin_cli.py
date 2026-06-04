from __future__ import annotations

import argparse
import os
import httpx

from dotenv import load_dotenv

from app.auth import hash_password
from app.llm_client import smoke_test
from app.tenant_store import init_db, create_user, get_user_by_email


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Admin CLI for control DB")
    sub = parser.add_subparsers(dest="cmd", required=True)

    init_cmd = sub.add_parser("init-db", help="Initialize control DB schema")

    su_cmd = sub.add_parser("create-superuser", help="Create a superuser account")
    su_cmd.add_argument("--email", required=True)
    su_cmd.add_argument("--password", required=True)

    sub.add_parser("test-openrouter", help="Test the configured LLM provider connection")

    webhook_cmd = sub.add_parser("set-telegram-webhook", help="Configure Telegram webhook URL")
    webhook_cmd.add_argument("--url", required=True, help="Full webhook URL, e.g. https://app.up.railway.app/telegram/webhook")

    args = parser.parse_args()

    if args.cmd == "init-db":
        init_db()
        print("Control DB initialized.")
        return 0

    if args.cmd == "create-superuser":
        init_db()
        existing = get_user_by_email(args.email)
        if existing:
            print("User already exists.")
            return 1
        pwd_hash = hash_password(args.password)
        create_user(args.email, pwd_hash, role="superuser", dsn_id=None)
        print("Superuser created.")
        return 0

    if args.cmd == "test-openrouter":
        result = smoke_test()
        print(f"Provider: {result['provider']}")
        print(f"Model: {result['model']}")
        print(f"Status: {result['status']}")
        return 0

    if args.cmd == "set-telegram-webhook":
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            print("TELEGRAM_BOT_TOKEN is not set.")
            return 1
        resp = httpx.post(
            f"https://api.telegram.org/bot{token}/setWebhook",
            json={"url": args.url, "drop_pending_updates": True},
            timeout=20,
        )
        data = resp.json()
        if not data.get("ok"):
            print(f"Telegram webhook failed: {data}")
            return 1
        print(f"Telegram webhook set to {args.url}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
