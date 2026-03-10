from __future__ import annotations

import argparse
import json
import os
import sys

from dotenv import load_dotenv

from app.analyst import generate_answer
from app.charting import make_chart
from app.db import run_select, DatabaseError
from app.llm_planner import question_to_sql


def _default_restaurant() -> str:
    return os.getenv("DEFAULT_RESTAURANT", "Gamba")


def _ask(
    question: str,
    restaurant: str,
    preview: bool,
    include_sql: bool,
    language: str,
    history: list[dict],
) -> tuple[str | None, list]:
    """
    Run one question through the full pipeline.
    Returns (answer_text | None, rows).
    """
    try:
        plan = question_to_sql(question, restaurant=restaurant, history=history)
    except Exception as e:
        print(f"[planner error] {e}", file=sys.stderr)
        return None, []

    if include_sql:
        print(f"\n--- SQL ---\n{plan.sql}\n----------\n")

    try:
        rows = run_select(
            plan.sql,
            params={"restaurant": restaurant},
            preview=preview,
            statement_timeout_ms=int(os.getenv("STATEMENT_TIMEOUT_MS_ASK", "30000")),
        )
    except DatabaseError as e:
        print(f"[database error] {e}", file=sys.stderr)
        if include_sql:
            print(f"SQL was:\n{plan.sql}")
        return None, []

    answer = generate_answer(question, rows, language=language, history=history)
    return answer, rows


def _show_chart(rows: list, question: str) -> None:
    """Generate a chart from rows and open it if on macOS."""
    chart_bytes = make_chart(rows, question)
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

        answer, rows = _ask(
            question, restaurant,
            preview=True,
            include_sql=include_sql,
            language=language,
            history=history,
        )
        if answer:
            print(f"\nAnalyst: {answer}\n")
            if show_charts:
                _show_chart(rows, question)
            history.append({"role": "user", "content": question})
            history.append({"role": "assistant", "content": answer})
            if len(history) > 20:
                history = history[-20:]

    return 0


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Restaurant BI Agent CLI")
    parser.add_argument("question", nargs="?", help="Question to ask (omit for --chat mode)")
    parser.add_argument("--restaurant", default=None, help="Restaurant name (default: DEFAULT_RESTAURANT env var)")
    parser.add_argument("--language", default="en", choices=["en", "es"], help="Response language (default: en)")
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
            language=args.language,
            show_charts=args.chart,
        )

    answer, rows = _ask(
        args.question, restaurant,
        preview=not args.no_preview,
        include_sql=args.include_sql,
        language=args.language,
        history=[],
    )
    if answer is None:
        return 1

    print(answer)

    if args.chart:
        _show_chart(rows, args.question)

    if args.include_data:
        print("\nRows:")
        print(json.dumps(rows, indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
