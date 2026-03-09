from __future__ import annotations

import json
import os
import re
from typing import Literal, Optional, Set, Any

from openai import OpenAI
from pydantic import BaseModel, ValidationError
import sqlglot
from sqlglot import expressions as exp

from app.schema_context import schema_prompt
from app.sql_safety import validate_select_only, UnsafeSQL


class LLMQuery(BaseModel):
    sql: str
    expected_result: Optional[Literal["scalar", "time_series", "breakdown", "table"]] = "table"
    notes: Optional[str] = None


def _extract_tables(sql: str) -> Set[str]:
    try:
        tree = _parse_sql(sql)
    except Exception:
        return set()
    tables = set()
    for t in tree.find_all(exp.Table):
        if t.name:
            tables.add(t.name)
    return tables


_PARAM_RE = re.compile(r"%\([a-zA-Z_][a-zA-Z0-9_]*\)s")
_PARAM_TOKEN = "__PARAM__TOKEN__"


def _sanitize_params(sql: str) -> str:
    return _PARAM_RE.sub(f"'{_PARAM_TOKEN}'", sql)


def _restore_params(sql: str) -> str:
    return sql.replace(f"'{_PARAM_TOKEN}'", "%(restaurant)s")


def _escape_percent(sql: str) -> str:
    # Escape bare % for psycopg pyformat (keep %(name)s intact)
    return re.sub(r"%(?!\()", "%%", sql)


def _parse_sql(sql: str) -> exp.Expression:
    return sqlglot.parse_one(_sanitize_params(sql), read="postgres")


def _strip_code_fence(text: str) -> str:
    # Remove ```json / ```sql fences if present
    if text.strip().startswith("```"):
        lines = text.strip().splitlines()
        if len(lines) >= 2 and lines[0].startswith("```"):
            # drop first and last fence line
            return "\n".join(lines[1:-1]).strip()
    return text.strip()


def _extract_json_object(text: str) -> Optional[dict]:
    text = _strip_code_fence(text)
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
    except Exception:
        pass

    # try to find the first {...} block
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        snippet = text[start : end + 1]
        try:
            data = json.loads(snippet)
            if isinstance(data, dict):
                return data
        except Exception:
            return None
    return None


def _extract_sql(text: str) -> Optional[str]:
    text = _strip_code_fence(text)
    # try to find a SELECT or WITH statement
    m = re.search(r"(WITH\\s+.+?SELECT\\s+|SELECT\\s+)", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    sql = text[m.start():].strip()
    return sql


def _requires_sales_filter(sql: str) -> bool:
    s = sql.lower()
    return " sales " in f" {s} " or "sales." in s


def _has_closed_filter(sql: str) -> bool:
    s = sql.lower().replace('"', "")
    return "sale_state" in s and "'closed'" in s


def _has_restaurant_param(sql: str) -> bool:
    return "%(restaurant)s" in sql


def _split_and(expr: exp.Expression) -> list[exp.Expression]:
    parts: list[exp.Expression] = []
    if isinstance(expr, exp.And):
        parts.extend(_split_and(expr.left))
        parts.extend(_split_and(expr.right))
    else:
        parts.append(expr)
    return parts


def _mentions_column(expr: exp.Expression, table: str, column: str) -> bool:
    for c in expr.find_all(exp.Column):
        if c.name == column and (c.table == table or c.table is None and table is None):
            return True
    return False


def _replace_column(tree: exp.Expression, table: str, old: str, new: str) -> exp.Expression:
    for col in tree.find_all(exp.Column):
        if col.name == old and col.table == table:
            col.set("this", exp.Identifier(this=new))
    return tree


def _apply_last_completed_week(sql: str, table: str, column: str) -> str:
    try:
        tree = _parse_sql(sql)
    except Exception:
        return sql

    where = tree.args.get("where")
    new_pred = exp.and_(
        exp.GTE(
            this=exp.Column(this=exp.Identifier(this=column), table=exp.Identifier(this=table)),
            expression=exp.Sub(
                this=exp.Anonymous(this="date_trunc", expressions=[exp.Literal.string("week"), exp.Anonymous(this="now")]),
                expression=exp.Interval(this=exp.Literal.string("7 days")),
            ),
        ),
        exp.LT(
            this=exp.Column(this=exp.Identifier(this=column), table=exp.Identifier(this=table)),
            expression=exp.Anonymous(this="date_trunc", expressions=[exp.Literal.string("week"), exp.Anonymous(this="now")]),
        ),
    )

    if where is None:
        tree.set("where", exp.Where(this=new_pred))
        return _restore_params(tree.sql(dialect="postgres"))

    parts = _split_and(where.this)
    filtered = [
        p for p in parts
        if not _mentions_column(p, table, column)
    ]
    filtered.append(new_pred)
    combined = filtered[0]
    for p in filtered[1:]:
        combined = exp.and_(combined, p)
    tree.set("where", exp.Where(this=combined))
    return _restore_params(tree.sql(dialect="postgres"))


def _inject_items_not_canceled(sql: str) -> str:
    # Insert "items.canceled IS NOT TRUE" into WHERE before GROUP/ORDER/LIMIT
    if "items.canceled" in sql.lower():
        return sql
    lower = sql.lower()
    insert = " items.canceled IS NOT TRUE "
    if " where " in lower:
        # insert AND before group/order/limit
        for token in [" group by ", " order by ", " limit "]:
            idx = lower.find(token)
            if idx != -1:
                return sql[:idx] + " AND" + insert + sql[idx:]
        return sql.rstrip(";") + " AND" + insert + ";"
    else:
        # add WHERE before group/order/limit
        for token in [" group by ", " order by ", " limit "]:
            idx = lower.find(token)
            if idx != -1:
                return sql[:idx] + " WHERE" + insert + sql[idx:]
    return sql.rstrip(";") + " WHERE" + insert + ";"


def _apply_last_weekday(sql: str, table: str, column: str, weekday_index: int) -> str:
    """
    weekday_index: 0=Mon ... 6=Sun
    Returns most recent weekday (including current week). If today is before that weekday,
    shift to previous week.
    """
    # Use ISO week day (Mon=1..Sun=7). Last weekday = date_trunc('week') + (idx) days,
    # minus 7 days if today's ISO DOW < target.
    target_day = weekday_index + 1
    pred_sql = (
        f"DATE({table}.{column}) = ("
        f"DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '{weekday_index} days' - "
        f"(CASE WHEN EXTRACT(ISODOW FROM CURRENT_DATE) < {target_day} "
        f"THEN INTERVAL '7 days' ELSE INTERVAL '0 days' END)"
        f")"
    )

    try:
        tree = _parse_sql(sql)
        where = tree.args.get("where")
        parts = _split_and(where.this) if where else []
        filtered = [p for p in parts if not _mentions_column(p, table, column)]
        pred_tree = sqlglot.parse_one(f"SELECT * FROM t WHERE {pred_sql}", read="postgres")
        pred_expr = pred_tree.args["where"].this
        filtered.append(pred_expr)
        combined = filtered[0] if filtered else pred_expr
        for p in filtered[1:]:
            combined = exp.and_(combined, p)
        tree.set("where", exp.Where(this=combined))
        return _restore_params(tree.sql(dialect="postgres"))
    except Exception:
        # fallback: inject predicate into SQL string
        return _inject_where_pred(sql, pred_sql)


def _inject_where_pred(sql: str, pred: str) -> str:
    lower = sql.lower()
    if " where " in lower:
        for token in [" group by ", " order by ", " limit "]:
            idx = lower.find(token)
            if idx != -1:
                return sql[:idx] + " AND " + pred + sql[idx:]
        return sql.rstrip(";") + " AND " + pred + ";"
    else:
        for token in [" group by ", " order by ", " limit "]:
            idx = lower.find(token)
            if idx != -1:
                return sql[:idx] + " WHERE " + pred + sql[idx:]
        return sql.rstrip(";") + " WHERE " + pred + ";"


def question_to_sql(question: str, restaurant: str) -> LLMQuery:
    client = OpenAI()
    system_prompt = schema_prompt()
    user_prompt = (
        "Generate SQL to answer the user question.\n"
        "Return JSON only. Use %(restaurant)s as the restaurant param.\n"
        f"Question: {question}\n"
    )

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0,
    )

    content = resp.choices[0].message.content or ""

    data = _extract_json_object(content)
    if data is None:
        sql = _extract_sql(content)
        if not sql:
            snippet = content.strip().replace("\n", " ")[:200]
            raise ValueError(f"LLM response was not JSON or SQL. Snippet: {snippet}")
        plan = LLMQuery(sql=sql, expected_result="table", notes="extracted_sql")
    else:
        # normalize common alternate key names
        if "sql" not in data:
            if "query" in data:
                data["sql"] = data["query"]
            elif "sql_query" in data:
                data["sql"] = data["sql_query"]

        # normalize expected_result if malformed
        if "expected_result" in data:
            if not isinstance(data["expected_result"], str):
                data["expected_result"] = "table"
            else:
                allowed = {"scalar", "time_series", "breakdown", "table"}
                if data["expected_result"] not in allowed:
                    data["expected_result"] = "table"

        try:
            plan = LLMQuery(**data)
        except ValidationError as e:
            snippet = content.strip().replace("\n", " ")[:200]
            raise ValueError(f"LLM JSON failed validation: {e}. Snippet: {snippet}") from e

    # Basic SQL safety
    validate_select_only(plan.sql)
    if not _has_restaurant_param(plan.sql):
        raise UnsafeSQL("SQL must include %(restaurant)s parameter for restaurant scoping.")
    if _requires_sales_filter(plan.sql) and not _has_closed_filter(plan.sql):
        raise UnsafeSQL("SQL must enforce sales.sale_state = 'CLOSED'.")

    # Time window normalization for "last completed week" / "last week"
    q = question.lower()
    last_week_phrases = (
        "last completed week",
        "last complete week",
        "last full week",
        "last week",
        "semana pasada",
        "semana anterior",
        "última semana",
        "ultima semana",
        "semana previa",
    )
    if any(p in q for p in last_week_phrases):
        tables = _extract_tables(plan.sql)
        if "sales" in tables:
            plan = LLMQuery(
                sql=_apply_last_completed_week(plan.sql, "sales", "created_at"),
                expected_result=plan.expected_result,
                notes=(plan.notes or "") + " | normalized:last_week",
            )
        elif "payments" in tables and "sales" not in tables:
            plan = LLMQuery(
                sql=_apply_last_completed_week(plan.sql, "payments", "created_at"),
                expected_result=plan.expected_result,
                notes=(plan.notes or "") + " | normalized:last_week",
            )

    # Time window normalization for "last <weekday>"
    weekday_map = {
        "last monday": 0,
        "last tuesday": 1,
        "last wednesday": 2,
        "last thursday": 3,
        "last friday": 4,
        "last saturday": 5,
        "last sunday": 6,
        "lunes pasado": 0,
        "martes pasado": 1,
        "miércoles pasado": 2,
        "miercoles pasado": 2,
        "jueves pasado": 3,
        "viernes pasado": 4,
        "sábado pasado": 5,
        "sabado pasado": 5,
        "domingo pasado": 6,
    }
    for phrase, idx in weekday_map.items():
        if phrase in q:
            tables = _extract_tables(plan.sql)
            if "sales" in tables:
                plan = LLMQuery(
                    sql=_apply_last_weekday(plan.sql, "sales", "created_at", idx),
                    expected_result=plan.expected_result,
                    notes=(plan.notes or "") + " | normalized:last_weekday",
                )
            elif "payments" in tables and "sales" not in tables:
                plan = LLMQuery(
                    sql=_apply_last_weekday(plan.sql, "payments", "created_at", idx),
                    expected_result=plan.expected_result,
                    notes=(plan.notes or "") + " | normalized:last_weekday",
                )
            break

    # Never use closed_at; replace with created_at when sales table is present
    try:
        tree = _parse_sql(plan.sql)
        tree = _replace_column(tree, "sales", "closed_at", "created_at")
        plan = LLMQuery(
            sql=_restore_params(tree.sql(dialect="postgres")),
            expected_result=plan.expected_result,
            notes=(plan.notes or "") + " | normalized:created_at",
        )
    except Exception:
        pass

    # If query filters on products.name, ensure item revenue (items.price * items.quantity)
    try:
        tree = _parse_sql(plan.sql)
        has_product_name = any(
            isinstance(expr, exp.EQ)
            and _mentions_column(expr, "products", "name")
            for expr in tree.find_all(exp.EQ)
        )
        if has_product_name:
            # replace SUM(sales.total) with SUM(items.price * items.quantity)
            for s in tree.find_all(exp.Select):
                for i, sel in enumerate(s.expressions):
                    if isinstance(sel, exp.Alias):
                        target = sel.this
                        if isinstance(target, exp.Sum) and _mentions_column(target, "sales", "total"):
                            new_expr = exp.Sum(
                                this=exp.Mul(
                                    this=exp.Column(this=exp.Identifier(this="price"), table=exp.Identifier(this="items")),
                                    expression=exp.Column(this=exp.Identifier(this="quantity"), table=exp.Identifier(this="items")),
                                )
                            )
                            s.expressions[i] = exp.Alias(this=new_expr, alias=sel.alias)
                    elif isinstance(sel, exp.Sum) and _mentions_column(sel, "sales", "total"):
                        s.expressions[i] = exp.Sum(
                            this=exp.Mul(
                                this=exp.Column(this=exp.Identifier(this="price"), table=exp.Identifier(this="items")),
                                expression=exp.Column(this=exp.Identifier(this="quantity"), table=exp.Identifier(this="items")),
                            )
                        )

            # ensure items.canceled IS NOT TRUE
            where = tree.args.get("where")
            canceled_pred = exp.IsNot(
                this=exp.Column(this=exp.Identifier(this="canceled"), table=exp.Identifier(this="items")),
                expression=exp.Boolean(this=True),
            )
            if where is None:
                tree.set("where", exp.Where(this=canceled_pred))
            else:
                parts = _split_and(where.this)
                if not any(_mentions_column(p, "items", "canceled") for p in parts):
                    parts.append(canceled_pred)
                    combined = parts[0]
                    for p in parts[1:]:
                        combined = exp.and_(combined, p)
                    tree.set("where", exp.Where(this=combined))

            # make product name match case-insensitive if using equality
            for eq in tree.find_all(exp.EQ):
                if _mentions_column(eq, "products", "name"):
                    right = eq.expression
                    left = eq.this
                    eq.replace(
                        exp.ILike(
                            this=left,
                            expression=right,
                        )
                    )

            # coalesce sum to 0 for product revenue
            for s in tree.find_all(exp.Select):
                for i, sel in enumerate(s.expressions):
                    if isinstance(sel, exp.Alias):
                        target = sel.this
                        if isinstance(target, exp.Sum):
                            s.expressions[i] = exp.Alias(
                                this=exp.Coalesce(this=target, expressions=[exp.Literal.number(0)]),
                                alias=sel.alias,
                            )
                    elif isinstance(sel, exp.Sum):
                        s.expressions[i] = exp.Coalesce(this=sel, expressions=[exp.Literal.number(0)])

            plan = LLMQuery(
                sql=_restore_params(tree.sql(dialect="postgres")),
                expected_result=plan.expected_result,
                notes=(plan.notes or "") + " | normalized:product_revenue",
            )
    except Exception:
        pass

    # If items table is used, ensure canceled items are excluded
    try:
        sql_lower = plan.sql.lower()
        if "items" in sql_lower and "items.canceled" not in sql_lower:
            tree = _parse_sql(plan.sql)
            where = tree.args.get("where")
            canceled_pred = exp.IsNot(
                this=exp.Column(this=exp.Identifier(this="canceled"), table=exp.Identifier(this="items")),
                expression=exp.Boolean(this=True),
            )
            if where is None:
                tree.set("where", exp.Where(this=canceled_pred))
            else:
                parts = _split_and(where.this)
                if not any(_mentions_column(p, "items", "canceled") for p in parts):
                    parts.append(canceled_pred)
                    combined = parts[0]
                    for p in parts[1:]:
                        combined = exp.and_(combined, p)
                    tree.set("where", exp.Where(this=combined))
            plan = LLMQuery(
                sql=_restore_params(tree.sql(dialect="postgres")),
                expected_result=plan.expected_result,
                notes=(plan.notes or "") + " | normalized:items_not_canceled",
            )
    except Exception:
        # If parsing fails, fall back to safe string injection.
        pass

    # Final fallback for items.canceled if still missing
    try:
        if "items" in plan.sql.lower() and "items.canceled" not in plan.sql.lower():
            plan = LLMQuery(
                sql=_inject_items_not_canceled(plan.sql),
                expected_result=plan.expected_result,
                notes=(plan.notes or "") + " | normalized:items_not_canceled",
            )
    except Exception:
        pass

    final_sql = _escape_percent(plan.sql.strip())
    return LLMQuery(
        sql=final_sql,
        expected_result=plan.expected_result,
        notes=plan.notes,
    )
