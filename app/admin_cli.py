from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from app.auth import hash_password
from app.tenant_store import init_db, create_user, get_user_by_email


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Admin CLI for control DB")
    sub = parser.add_subparsers(dest="cmd", required=True)

    init_cmd = sub.add_parser("init-db", help="Initialize control DB schema")

    su_cmd = sub.add_parser("create-superuser", help="Create a superuser account")
    su_cmd.add_argument("--email", required=True)
    su_cmd.add_argument("--password", required=True)

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

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
