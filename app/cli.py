from __future__ import annotations

import argparse
import json
import os
from typing import Any

from dotenv import load_dotenv

from app.llm_planner import question_to_sql
from app.db import run_select, DatabaseError
from app.verbalizer import verbalize_answer


def _default_restaurant() -> str:
    return os.getenv("DEFAULT_RESTAURANT", "Gamba")


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Ask the Restaurant BI Agent from the terminal.")
    parser.add_argument("question", help="Question in English, e.g. 'gross sales last 7 days'")
    parser.add_argument("--restaurant", default=None, help="Restaurant name (defaults to DEFAULT_RESTAURANT)")
    parser.add_argument("--no-preview", action="store_true", help="Disable preview limit")
    parser.add_argument("--include-sql", action="store_true", help="Print SQL and params")
    parser.add_argument("--include-data", action="store_true", help="Print raw rows")
    args = parser.parse_args()

    restaurant = args.restaurant or _default_restaurant()

    plan = question_to_sql(args.question, restaurant=restaurant)
    try:
        rows = run_select(
            plan.sql,
            params={"restaurant": restaurant},
            preview=not args.no_preview,
            statement_timeout_ms=int(os.getenv("STATEMENT_TIMEOUT_MS_ASK", "30000")),
        )
    except DatabaseError as e:
        print(f"DB error: {e}")
        if args.include_sql:
            print("\nSQL:\n", plan.sql)
        return 1

    print(verbalize_answer(args.question, plan, rows))

    if args.include_sql:
        print("\nSQL:")
        print(plan.sql)
        print("\nParams:")
        print(json.dumps({"restaurant": restaurant}, indent=2))

    if args.include_data:
        print("\nRows:")
        print(json.dumps(rows, indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
