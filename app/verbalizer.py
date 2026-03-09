# app/verbalizer.py
from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional


def _to_date_str(x: Any) -> str:
    """Normalize a period value (datetime/date/str) to YYYY-MM-DD string."""
    if isinstance(x, datetime):
        return x.strftime("%Y-%m-%d")
    if isinstance(x, date):
        return x.strftime("%Y-%m-%d")

    s = str(x)
    try:
        return datetime.fromisoformat(s.replace("Z", "")).strftime("%Y-%m-%d")
    except Exception:
        return s[:10] if len(s) >= 10 else s


def _fmt_number(x: Any) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


def _find_dimension_key(rows: List[Dict[str, Any]]) -> Optional[str]:
    """Find first non-(period,value) column to use as label in rankings."""
    if not rows:
        return None
    for k in rows[0].keys():
        if k not in ("period", "value"):
            return k
    return None


def _find_value_key(row: Dict[str, Any], exclude: set[str]) -> Optional[str]:
    for k, v in row.items():
        if k in exclude:
            continue
        if isinstance(v, (int, float)):
            return k
        try:
            float(v)
            return k
        except Exception:
            continue
    # fallback: any remaining key
    for k in row.keys():
        if k not in exclude:
            return k
    return None


def verbalize_answer(question: str, plan: Any, rows: List[Dict[str, Any]], language: str = "en") -> str:
    if not rows:
        if language == "es":
            return f"No pude encontrar datos para: {question}"
        return f"I couldn’t find any data matching: {question}"

    expected = getattr(plan, "expected_result", None)

    # ----------------------------
    # Scalar mode (single value)
    # ----------------------------
    if expected == "scalar":
        r0 = rows[0]
        if "value" in r0:
            if language == "es":
                return f"La respuesta es {_fmt_number(r0['value'])}."
            return f"The answer is {_fmt_number(r0['value'])}."
        # fallback to first column
        if r0:
            k = list(r0.keys())[0]
            if language == "es":
                return f"La respuesta es {_fmt_number(r0[k])}."
            return f"The answer is {_fmt_number(r0[k])}."

    comparison_dates = getattr(plan, "comparison_dates", None)

    # ----------------------------
    # Comparison mode (2 specific days)
    # ----------------------------
    if comparison_dates and len(comparison_dates) == 2 and "period" in rows[0] and "value" in rows[0]:
        # Map returned rows by YYYY-MM-DD
        by_day: Dict[str, float] = {}
        for r in rows:
            d = _to_date_str(r["period"])
            by_day[d] = float(r["value"])

        d1 = str(comparison_dates[0])
        d2 = str(comparison_dates[1])

        v1 = by_day.get(d1)
        v2 = by_day.get(d2)

        # Fallback: if not found exactly, use chronological first/last
        if v1 is None or v2 is None:
            rows_sorted = sorted(rows, key=lambda r: _to_date_str(r["period"]))
            if len(rows_sorted) >= 2:
                d1 = _to_date_str(rows_sorted[0]["period"])
                d2 = _to_date_str(rows_sorted[-1]["period"])
                v1 = float(rows_sorted[0]["value"])
                v2 = float(rows_sorted[-1]["value"])

        if v1 is None or v2 is None:
            # If still missing, just show what we have
            lines = [f"• {_to_date_str(r['period'])}: {_fmt_number(r['value'])}" for r in rows]
            return (
                (f"Aquí está lo que encontré para **{question}**:\n\n" if language == "es" else f"Here’s what I found for **{question}**:\n\n")
                + "\n".join(lines)
            )

        diff = v2 - v1
        pct = (diff / v1) * 100 if v1 != 0 else 0.0
        sign = "+" if pct >= 0 else ""
        direction = "increase" if diff > 0 else "decrease" if diff < 0 else "no change"
        direction_es = "aumento" if diff > 0 else "disminución" if diff < 0 else "sin cambios"

        label_new = "Current period"
        label_old = "Previous period"

        # nicer labels based on question text
        q_lower = str(question).lower()
        if "yesterday" in q_lower and "last week" in q_lower:
            label_new = "Yesterday"
            label_old = "Same weekday last week"
        elif "this week" in q_lower and "last week" in q_lower:
            label_new = "This week"
            label_old = "Last week"
        elif "this month" in q_lower and "last month" in q_lower:
            label_new = "This month"
            label_old = "Last month"

        if language == "es":
            return (
                f"Para **{question}**:\n"
                f"- {label_new} ({d2}): {_fmt_number(v2)}\n"
                f"- {label_old} ({d1}): {_fmt_number(v1)}\n"
                f"Cambio: {_fmt_number(diff)} ({sign}{pct:.1f}%) {direction_es}"
            )
        return (
            f"For **{question}**:\n"
            f"- {label_new} ({d2}): {_fmt_number(v2)}\n"
            f"- {label_old} ({d1}): {_fmt_number(v1)}\n"
            f"Change: {_fmt_number(diff)} ({sign}{pct:.1f}%) {direction}"
        )

    # ----------------------------
    # Time series mode (supports period/value or week/gross_sales, etc.)
    # ----------------------------
    if isinstance(rows[0], dict) and ("period" in rows[0] or "week" in rows[0]):
        time_key = "period" if "period" in rows[0] else "week"
        # choose value key
        value_key = None
        for k in rows[0].keys():
            if k != time_key:
                value_key = k
                break
        value_label = "value" if value_key is None else value_key

        lines = []
        for r in rows:
            day = _to_date_str(r.get(time_key))
            val = _fmt_number(r.get(value_key)) if value_key else _fmt_number(r.get("value"))
            if language == "es":
                lines.append(f"- {day} -> {value_label} = {val}")
            else:
                lines.append(f"- {day} -> {value_label.replace('_',' ').title()} = {val}")

        if language == "es":
            header = f"Resultados para {question}:\n"
        else:
            header = f"Results for {question}:\n"

        return header + "\n".join(lines)

    # ----------------------------
    # Ranking / breakdown mode
    # ----------------------------
    # Ranking / breakdown mode
    dim_key = _find_dimension_key(rows)

    # Trend rows: product + recent_rev + prior_rev + delta (+ pct_change)
    if dim_key == "product" and "delta" in rows[0] and "recent_rev" in rows[0] and "prior_rev" in rows[0]:
        lines = []
        for r in rows[:20]:
            product = str(r.get("product"))
            recent = _fmt_number(r.get("recent_rev"))
            prior = _fmt_number(r.get("prior_rev"))
            delta = _fmt_number(r.get("delta"))
            pct = r.get("pct_change")
            pct_str = "n/a" if pct is None else f"{float(pct) * 100:+.1f}%"
            lines.append(f"• {product}: Δ {delta} ({pct_str}) — recent {recent}, prior {prior}")

        return (
            (f"Aquí están los productos con mayor aumento para **{question}**:\n\n" if language == "es" else f"Here are the products with the biggest increase for **{question}**:\n\n")
            + "\n".join(lines)
            + ("\n\n¿Quieres que los ordene por % de cambio en vez de aumento absoluto?" if language == "es" else "\n\nWant me to rank by % change instead of absolute increase?")
        )

    # Standard ranking: label + value (supports custom value key)
    if dim_key:
        value_key = "value" if "value" in rows[0] else _find_value_key(rows[0], {dim_key, "period"})
        if value_key:
            lines = []
            label_name = value_key.replace("_", " ").title()
            for r in rows[:20]:
                label = str(r.get(dim_key))
                val = _fmt_number(r.get(value_key))
                if language == "es":
                    lines.append(f"- {label} -> {label_name} = {val}")
                else:
                    lines.append(f"- {label} -> {label_name} = {val}")

            return (
                (f"Aquí está el desglose para {question}:\n" if language == "es" else f"Here’s the breakdown for {question}:\n")
                + "\n".join(lines)
            )

    # ----------------------------
    # If single-row single-column, answer directly
    # ----------------------------
    if len(rows) == 1 and isinstance(rows[0], dict) and len(rows[0]) == 1:
        key = list(rows[0].keys())[0]
        val = rows[0][key]
        if language == "es":
            return f"Para {question}, el valor es {_fmt_number(val)}."
        return f"For {question}, the value is {_fmt_number(val)}."

    # ----------------------------
    # Generic fallback
    # ----------------------------
    if rows and isinstance(rows[0], dict):
        lines = []
        for r in rows[:20]:
            parts = [f"{k}={_fmt_number(v)}" for k, v in r.items()]
            lines.append("- " + ", ".join(parts))
        header = (f"Encontré {len(rows)} resultados para {question}:\n" if language == "es" else f"I found {len(rows)} results for {question}:\n")
        return header + "\n".join(lines)

    return (
        (f"Encontré {len(rows)} resultados para {question}." if language == "es" else f"I found {len(rows)} results for {question}.")
    )
