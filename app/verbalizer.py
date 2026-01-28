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


def verbalize_answer(question: str, plan: Any, rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return f"I couldn’t find any data matching: {question}"

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
                f"Here’s what I found for **{question}**:\n\n"
                + "\n".join(lines)
            )

        diff = v2 - v1
        pct = (diff / v1) * 100 if v1 != 0 else 0.0
        sign = "+" if pct >= 0 else ""
        direction = "increase" if diff > 0 else "decrease" if diff < 0 else "no change"

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

        return (
            f"For **{question}**:\n"
            f"- {label_new} ({d2}): {_fmt_number(v2)}\n"
            f"- {label_old} ({d1}): {_fmt_number(v1)}\n"
            f"Change: {_fmt_number(diff)} ({sign}{pct:.1f}%) {direction}"
        )

    # ----------------------------
    # Time series mode
    # ----------------------------
    if "period" in rows[0] and "value" in rows[0]:
        lines = []
        for r in rows:
            day = _to_date_str(r.get("period"))
            val = _fmt_number(r.get("value"))
            lines.append(f"• {day}: {val}")

        return (
            f"Here’s what I found for **{question}**:\n\n"
            + "\n".join(lines)
            + "\n\nLet me know if you want to explore this further."
        )

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
            f"Here are the products with the biggest increase for **{question}**:\n\n"
            + "\n".join(lines)
            + "\n\nWant me to rank by % change instead of absolute increase?"
        )

    # Standard ranking: label + value
    if dim_key and "value" in rows[0]:
        lines = []
        for r in rows[:20]:
            label = str(r.get(dim_key))
            val = _fmt_number(r.get("value"))
            lines.append(f"• {label}: {val}")

        return (
            f"Here’s the breakdown for **{question}**:\n\n"
            + "\n".join(lines)
            + "\n\nWant me to add a date filter or compare periods?"
        )

    # ----------------------------
    # Generic fallback
    # ----------------------------
    return (
        f"I found {len(rows)} results for **{question}**.\n"
        f"Sample:\n{rows[:5]}"
    )
