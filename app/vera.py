from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from app.analyst import detect_language, format_table
from app.charting import make_chart
from app.db import run_select
from app.llm_client import chat_completion
from app.schema_context import vera_context_prompt
from app.sql_safety import validate_select_only, UnsafeSQL


MAX_VERA_QUERIES = 3
MAX_MEMORY_EXCHANGES = 30


class VeraQuery(BaseModel):
    purpose: str = ""
    sql: str


class VeraChartSpec(BaseModel):
    type: Optional[str] = None
    title: Optional[str] = None
    x: Optional[str] = None
    y: Optional[str] = None
    label: Optional[str] = None
    caption: Optional[str] = None


class VeraPlan(BaseModel):
    action: Literal["clarify", "query"]
    clarifying_question: Optional[str] = None
    queries: List[VeraQuery] = Field(default_factory=list)
    chart: Optional[VeraChartSpec] = None
    knowledge_question: Optional[str] = None


class VeraFinal(BaseModel):
    answer: str
    recommendations: List[str] = Field(default_factory=list)
    suggested_next_questions: List[str] = Field(default_factory=list)
    knowledge_to_save: Optional[str] = None


@dataclass
class QueryResult:
    purpose: str
    sql: str
    rows: List[Dict[str, Any]]
    table: str


@dataclass
class VeraResponse:
    action: Literal["clarify", "answer"]
    message: str
    rows: List[Dict[str, Any]]
    table: str
    chart_bytes: Optional[bytes]
    chart_caption: Optional[str]
    sql: Optional[str]
    executed_queries: List[QueryResult]
    knowledge_to_save: Optional[str] = None


def _json_loads(content: str) -> dict[str, Any]:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, flags=re.DOTALL)
        if match:
            return json.loads(match.group(0))
        raise


def _compact_history(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return history[-(MAX_MEMORY_EXCHANGES * 2):]


def _truncate_text(text: str, limit: int = 2500) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _rows_payload(rows: List[Dict[str, Any]], max_rows: int = 120) -> dict[str, Any]:
    sample = rows[:max_rows]
    return {
        "row_count": len(rows),
        "rows": sample,
        "truncated": len(rows) > len(sample),
    }


def _plan_user_payload(
    question: str,
    restaurants: List[str],
    language: str,
    history: List[Dict[str, Any]],
    restaurant_knowledge: Dict[str, str],
) -> str:
    return json.dumps(
        {
            "question": question,
            "language": language,
            "selected_restaurants": restaurants,
            "conversation_memory": _compact_history(history),
            "restaurant_knowledge": restaurant_knowledge,
        },
        ensure_ascii=False,
        default=str,
    )


def _final_user_payload(
    question: str,
    restaurants: List[str],
    language: str,
    history: List[Dict[str, Any]],
    restaurant_knowledge: Dict[str, str],
    results: List[QueryResult],
    chart_spec: Optional[VeraChartSpec],
) -> str:
    return json.dumps(
        {
            "question": question,
            "language": language,
            "selected_restaurants": restaurants,
            "conversation_memory": _compact_history(history),
            "restaurant_knowledge": restaurant_knowledge,
            "query_results": [
                {
                    "purpose": r.purpose,
                    "sql": r.sql,
                    "data": _rows_payload(r.rows),
                    "table_shown_to_user": bool(r.table),
                }
                for r in results
            ],
            "requested_chart": chart_spec.model_dump() if chart_spec else None,
        },
        ensure_ascii=False,
        default=str,
    )


def _require_restaurant_scope(sql: str) -> None:
    if "%(restaurant)s" not in sql and "%(restaurants)s" not in sql:
        raise UnsafeSQL("SQL must include %(restaurant)s or %(restaurants)s for restaurant scoping.")


def _apply_restaurant_scope(sql: str, restaurants: List[str]) -> tuple[str, dict]:
    if not restaurants:
        return sql, {}
    if len(restaurants) == 1:
        return sql, {"restaurant": restaurants[0]}
    updated = re.sub(r"=\s*LOWER\(%\(restaurant\)s\)", "= ANY(%(restaurants)s)", sql)
    updated = re.sub(r"=\s*%\(restaurant\)s", "= ANY(%(restaurants)s)", updated)
    return updated, {"restaurants": restaurants, "restaurant": restaurants[0]}


def plan_with_vera(
    question: str,
    restaurants: List[str],
    history: List[Dict[str, Any]],
    restaurant_knowledge: Dict[str, str],
    language: Optional[str] = None,
) -> VeraPlan:
    lang = language or detect_language(question)
    messages = [
        {"role": "system", "content": vera_context_prompt(mode="plan")},
        {"role": "user", "content": _plan_user_payload(question, restaurants, lang, history, restaurant_knowledge)},
    ]
    resp = chat_completion(
        messages,
        temperature=float(os.getenv("VERA_PLAN_TEMPERATURE", "0.0")),
        response_format={"type": "json_object"},
    )
    data = _json_loads(resp.choices[0].message.content or "{}")
    plan = VeraPlan.model_validate(data)

    if plan.action == "query":
        plan.queries = plan.queries[:MAX_VERA_QUERIES]
        if not plan.queries:
            raise ValueError("Vera returned query action without queries.")
        for q in plan.queries:
            validate_select_only(q.sql)
            _require_restaurant_scope(q.sql)
    elif not plan.clarifying_question:
        raise ValueError("Vera returned clarify action without a clarifying_question.")
    return plan


def final_with_vera(
    question: str,
    restaurants: List[str],
    history: List[Dict[str, Any]],
    restaurant_knowledge: Dict[str, str],
    results: List[QueryResult],
    chart_spec: Optional[VeraChartSpec],
    language: Optional[str] = None,
) -> VeraFinal:
    lang = language or detect_language(question)
    messages = [
        {"role": "system", "content": vera_context_prompt(mode="final")},
        {"role": "user", "content": _final_user_payload(question, restaurants, lang, history, restaurant_knowledge, results, chart_spec)},
    ]
    resp = chat_completion(
        messages,
        temperature=float(os.getenv("VERA_FINAL_TEMPERATURE", "0.3")),
        response_format={"type": "json_object"},
    )
    data = _json_loads(resp.choices[0].message.content or "{}")
    return VeraFinal.model_validate(data)


def format_vera_final(final: VeraFinal) -> str:
    parts = [_truncate_text(final.answer.strip())]
    if final.recommendations:
        parts.append("Recommended next steps:\n" + "\n".join(f"- {r}" for r in final.recommendations[:3]))
    if final.suggested_next_questions:
        parts.append("Questions worth asking next:\n" + "\n".join(f"- {q}" for q in final.suggested_next_questions[:3]))
    return "\n\n".join(p for p in parts if p.strip())


def answer_with_vera(
    question: str,
    restaurants: List[str],
    dsn: Optional[str],
    history: List[Dict[str, Any]],
    restaurant_knowledge: Dict[str, str],
    *,
    language: Optional[str] = None,
    preview: bool = True,
) -> VeraResponse:
    plan = plan_with_vera(question, restaurants, history, restaurant_knowledge, language=language)
    if plan.action == "clarify":
        return VeraResponse(
            action="clarify",
            message=plan.clarifying_question or "What decision are you trying to make with this analysis?",
            rows=[],
            table="",
            chart_bytes=None,
            chart_caption=None,
            sql=None,
            executed_queries=[],
        )

    executed: List[QueryResult] = []
    for query in plan.queries[:MAX_VERA_QUERIES]:
        sql, params = _apply_restaurant_scope(query.sql, restaurants)
        rows = run_select(
            sql,
            params=params,
            preview=preview,
            statement_timeout_ms=int(os.getenv("STATEMENT_TIMEOUT_MS_ASK", "30000")),
            dsn=dsn,
        )
        executed.append(QueryResult(query.purpose, sql, rows, format_table(rows)))

    final = final_with_vera(
        question,
        restaurants,
        history,
        restaurant_knowledge,
        executed,
        plan.chart,
        language=language,
    )
    primary = executed[0] if executed else QueryResult("", "", [], "")
    chart_bytes = make_chart(primary.rows, question, plan.chart) if (plan.chart and primary.rows) else None
    message = format_vera_final(final)
    if plan.knowledge_question:
        message = f"{message}\n\nRestaurant context question:\n- {plan.knowledge_question}"

    return VeraResponse(
        action="answer",
        message=message,
        rows=primary.rows,
        table=primary.table,
        chart_bytes=chart_bytes,
        chart_caption=(plan.chart.caption or plan.chart.title) if plan.chart else None,
        sql="\n\n".join(r.sql for r in executed if r.sql),
        executed_queries=executed,
        knowledge_to_save=final.knowledge_to_save,
    )


__all__ = [
    "MAX_MEMORY_EXCHANGES",
    "MAX_VERA_QUERIES",
    "QueryResult",
    "VeraChartSpec",
    "VeraFinal",
    "VeraPlan",
    "VeraQuery",
    "VeraResponse",
    "answer_with_vera",
    "final_with_vera",
    "format_vera_final",
    "plan_with_vera",
]
