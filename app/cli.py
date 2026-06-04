from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from dotenv import load_dotenv

from app.analyst import detect_language
from app.db import DatabaseError
from app.vera import answer_with_vera, MAX_MEMORY_EXCHANGES, VeraResponse


def _default_restaurant() -> str:
    return os.getenv("DEFAULT_RESTAURANT", "Gamba")


def _ask(
    question: str,
    restaurant: str,
    preview: bool,
    include_sql: bool,
    language: str,
    history: list[dict],
) -> tuple[VeraResponse | None, list]:
    """
    Run one question through the full pipeline.
    Returns (answer_text | None, rows).
    """
    try:
        response = answer_with_vera(
            question,
            restaurants=[restaurant],
            dsn=None,
            history=history,
            restaurant_knowledge={},
            language=language,
            preview=preview,
        )
    except DatabaseError as e:
        print(f"[database error] {e}", file=sys.stderr)
        return None, []
    except Exception as e:
        print(f"[vera error] {e}", file=sys.stderr)
        return None, []

    if include_sql and response.sql:
        print(f"\n--- SQL ---\n{response.sql}\n----------\n")
    return response, response.rows


def _show_chart(chart_bytes: Optional[bytes]) -> None:
    """Generate a chart from rows and open it if on macOS."""
    if not chart_bytes:
        return
    path = "/tmp/bi_chart.png"
    with open(path, "wb") as f:
        f.write(chart_bytes)
    print(f"\n[Chart saved → {path}]")
    # Auto-open on macOS
    if sys.platform == "darwin":
        os.system(f"open '{path}'")


def _chat_mode(restaurant: str, include_sql: bool, language: str, show_charts: bool) -> int:
    """Interactive REPL — maintains conversation history across questions."""
    print(f"Restaurant BI Agent — chatting with '{restaurant}' [{language}]")
    print("Type your question, or 'exit' to quit.\n")

    history: list[dict] = []

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not question:
            continue
        if question.lower() in ("exit", "quit", "q", "bye"):
            break

        response, rows = _ask(
            question, restaurant,
            preview=True,
            include_sql=include_sql,
            language=language or detect_language(question),
            history=history,
        )
        if response:
            if response.table:
                print(f"\n{response.table}")
            print(f"\nVera: {response.message}\n")
            if show_charts:
                _show_chart(response.chart_bytes)
            history.append({"role": "user", "content": question})
            history.append({"role": "assistant", "content": response.message})
            if len(history) > MAX_MEMORY_EXCHANGES * 2:
                history = history[-MAX_MEMORY_EXCHANGES * 2:]

    return 0


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Restaurant BI Agent CLI")
    parser.add_argument("question", nargs="?", help="Question to ask (omit for --chat mode)")
    parser.add_argument("--restaurant", default=None, help="Restaurant name (default: DEFAULT_RESTAURANT env var)")
    parser.add_argument("--language", default=None, choices=["en", "es"], help="Response language (default: auto-detect from question)")
    parser.add_argument("--chat", action="store_true", help="Interactive chat mode with conversation history")
    parser.add_argument("--no-preview", action="store_true", help="Fetch all rows (no preview limit)")
    parser.add_argument("--include-sql", action="store_true", help="Print the generated SQL")
    parser.add_argument("--include-data", action="store_true", help="Print raw rows as JSON")
    parser.add_argument("--chart", action="store_true", help="Generate and open a chart when applicable")
    args = parser.parse_args()

    restaurant = args.restaurant or _default_restaurant()

    if args.chat or not args.question:
        return _chat_mode(
            restaurant,
            include_sql=args.include_sql,
            language=args.language,  # None = auto-detect per question
            show_charts=args.chart,
        )

    lang = args.language or detect_language(args.question)
    response, rows = _ask(
        args.question, restaurant,
        preview=not args.no_preview,
        include_sql=args.include_sql,
        language=lang,
        history=[],
    )
    if response is None:
        return 1

    if response.table:
        print(response.table)
        print()
    print(response.message)

    if args.chart:
        _show_chart(response.chart_bytes)

    if args.include_data:
        print("\nRows:")
        print(json.dumps(rows, indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
