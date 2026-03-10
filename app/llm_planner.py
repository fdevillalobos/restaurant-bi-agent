from __future__ import annotations

import json
import os
import re
from datetime import date
from typing import Optional

from openai import OpenAI
from pydantic import BaseModel

from app.schema_context import schema_prompt
from app.sql_safety import validate_select_only, UnsafeSQL


class SQLPlan(BaseModel):
    sql: str
    notes: Optional[str] = None


# Keep LLMQuery as an alias so telegram_bot.py / cli.py imports don't break
LLMQuery = SQLPlan


def _escape_percent(sql: str) -> str:
    """Escape bare % for psycopg pyformat while keeping %(name)s intact."""
    return re.sub(r"%(?!\()", "%%", sql)


def question_to_sql(
    question: str,
    restaurant: str,
    history: list[dict] | None = None,
) -> SQLPlan:
    """
    Translate a natural-language question into a SQL SELECT query.

    Args:
        question:   The user's question in any language.
        restaurant: The restaurant name (used as the %(restaurant)s param value).
        history:    Optional list of prior {role, content} messages for follow-up context.

    Returns:
        SQLPlan with .sql (safe, ready to execute) and .notes.
    """
    client = OpenAI()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    today = date.today().isoformat()

    system = schema_prompt(today=today, restaurant=restaurant)

    messages: list[dict] = [{"role": "system", "content": system}]
    if history:
        # Pass last 10 messages (5 exchanges) for follow-up context
        messages.extend(history[-10:])
    messages.append({"role": "user", "content": question})

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = resp.choices[0].message.content or ""
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON: {e}\nRaw: {content[:400]}") from e

    # Normalize alternate key names
    if "sql" not in data or not data["sql"]:
        for alt in ("query", "sql_query", "SQL", "statement"):
            if alt in data and data[alt]:
                data["sql"] = data[alt]
                break

    if not data.get("sql"):
        raise ValueError(f"LLM response is missing a 'sql' key. Raw: {content[:400]}")

    # Safety checks (non-negotiable)
    validate_select_only(data["sql"])
    if "%(restaurant)s" not in data["sql"]:
        raise UnsafeSQL("SQL must include %(restaurant)s for restaurant scoping.")

    sql = _escape_percent(data["sql"].strip())
    return SQLPlan(sql=sql, notes=data.get("notes"))
