from datetime import date, timedelta
from typing import Optional

from openai import OpenAI, OpenAIError
from pydantic import ValidationError

from app.query_plan import QueryPlan, Dimension
from app.fallback_planner import fallback_plan
import json
import re

client = OpenAI()


# ----------------------------
# Date helpers
# ----------------------------

def today_utc() -> date:
    return date.today()


def week_start(d: date) -> date:
    # ISO week: Monday = 0
    return d - timedelta(days=d.weekday())


# ----------------------------
# Main planner
# ----------------------------

def question_to_plan(question: str) -> QueryPlan:
    """
    Converts a natural language question into a validated QueryPlan.
    Deterministic rules override the LLM for all time logic.
    """

    q = question.lower()
    today = today_utc()

    # ----------------------------
    # 1. Deterministic time handling
    # ----------------------------

    date_range: Optional[dict] = None
    time_grain: str = "none"
    limit: int = 0

    # --- Last N days ---
    if "last 7 days" in q:
        date_range = {
            "start": str(today - timedelta(days=7)),
            "end": str(today),
        }
        time_grain = "day"
        limit = 7

    elif "last 14 days" in q:
        date_range = {
            "start": str(today - timedelta(days=14)),
            "end": str(today),
        }
        time_grain = "day"
        limit = 14

    elif "last 30 days" in q:
        date_range = {
            "start": str(today - timedelta(days=30)),
            "end": str(today),
        }
        time_grain = "day"
        limit = 30

    elif "yesterday" in q and "last week" in q:
        yesterday = today - timedelta(days=1)
        last_week_same_day = yesterday - timedelta(days=7)

        return QueryPlan(
            metric="gross_sales",
            base_table="sales",
            restaurant="Gamba",
            date_range={"start": str(last_week_same_day), "end": str(yesterday)},
            time_grain="day",
            comparison_dates=[str(last_week_same_day), str(yesterday)],
            dimensions=[],
            limit=0,
        )

    # --- This week vs last week ---
    elif "this week" in q and "last week" in q:
        this_week_start = week_start(today)
        last_week_start = this_week_start - timedelta(days=7)

        date_range = {
            "start": str(last_week_start),
            "end": str(today),
        }
        time_grain = "week"
        limit = 0  # weekly comparison, no limit

    elif (
        ("inc`reased" in q or "increase" in q or "grew" in q or "growth" in q)
        and ("product" in q or "products" in q)
        and ("last 2 weeks" in q or "last two weeks" in q)
        and ("previous 2 weeks" in q or "previous two weeks" in q or "before that" in q)
        and ("complete weeks" in q or "only complete weeks" in q)
    ):
        # deterministic "complete weeks" trend query
        # Default top N
        n = 5
        m = re.search(r"\b(\d+)\b", q)
        if m:
            n = int(m.group(1))

        return QueryPlan(
            metric="item_revenue",
            base_table="items",
            restaurant="Gamba",
            date_range=None,  # computed in SQL using date_trunc('week', now())
            time_grain="none",
            dimensions=[Dimension(table="products", column="name", alias="product")],
            limit=n,
            trend_complete_weeks=True,
            trend_recent_weeks=2,
            trend_prior_weeks=2,
            trend_rank_by=("pct_change" if ("percent" in q or "%" in q or "rate" in q or "fastest" in q) else "delta"),
        )

    elif "this month" in q and "last month" in q:
        first_this_month = today.replace(day=1)
        first_last_month = (first_this_month - timedelta(days=1)).replace(day=1)

        # If user explicitly asks "till same day number", do MTD vs prior MTD
        if "same day number" in q or "till the same day" in q or "to the same day" in q:
            # Example: today Jan 27 -> compare Jan 1-27 vs Dec 1-27
            day_of_month = today.day
            end_this = today
            end_last = first_last_month + timedelta(days=day_of_month - 1)

            return QueryPlan(
                metric="gross_sales",
                base_table="sales",
                restaurant="Gamba",
                date_range=None,  # we'll use explicit ranges below
                time_grain="month",
                comparison_dates=[str(first_last_month), str(first_this_month)],
                comparison_ends=[str(end_last), str(end_this)],
                dimensions=[],
                limit=0,
            )

        # Default: this month-to-date vs full last month (or keep your existing logic)
        end_this = today
        return QueryPlan(
            metric="gross_sales",
            base_table="sales",
            restaurant="Gamba",
            date_range={"start": str(first_last_month), "end": str(end_this)},
            time_grain="month",
            comparison_dates=[str(first_last_month), str(first_this_month)],
            dimensions=[],
            limit=0,
        )

    # ----------------------------
    # 2. Metric & base table
    # ----------------------------

    # Default assumptions
    metric = "gross_sales"
    base_table = "sales"

    if "revenue" in q or "sales" in q:
        metric = "gross_sales"
        base_table = "sales"

    # ----------------------------
    # 3. Dimensions
    # ----------------------------

    dimensions = []

    # If user asks for "top/best products by revenue", force items-based revenue
    if ("top" in q or "best" in q) and ("product" in q or "products" in q) and ("revenue" in q or "sales" in q):
        # Default window if not specified
        start = today - timedelta(days=30)
        end = today

        # Try to parse "top N"
        n = 5
        m = re.search(r"\btop\s+(\d+)\b", q)
        if m:
            n = int(m.group(1))

        return QueryPlan(
            metric="item_revenue",
            base_table="items",
            restaurant="Gamba",
            date_range={"start": str(start), "end": str(end)},
            time_grain="none",
            dimensions=[Dimension(table="products", column="name", alias="product")],
            limit=n,
        )

    # ----------------------------
    # 4. If deterministic path worked, build plan directly
    # ----------------------------

    if date_range is not None:
        return QueryPlan(
            metric=metric,
            base_table=base_table,
            restaurant="Gamba",  # canonical name; SQL normalizes case
            date_range=date_range,
            time_grain=time_grain,
            dimensions=dimensions,
            limit=limit,
        )

    # ----------------------------
    # 5. Otherwise use LLM planner
    # ----------------------------

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a BI query planner for a restaurant analytics system.\n"
                        "Return ONLY valid JSON matching the QueryPlan schema.\n"
                        "Do NOT invent dates like 'this week' or 'last week'.\n"
                        "Dates must be explicit ISO dates.\n"
                    ),
                },
                {
                    "role": "user",
                    "content": question,
                },
            ],
        )

        data = resp.choices[0].message.content

        obj = json.loads(data)

        # Some models wrap the payload
        if isinstance(obj, dict) and "queryPlan" in obj and isinstance(obj["queryPlan"], dict):
            obj = obj["queryPlan"]

        # Force restaurant (don’t trust the LLM)
        obj["restaurant"] = "Gamba"

        plan = QueryPlan.model_validate(obj)
        return plan

    except (OpenAIError, ValidationError, ValueError) as e:
        # ----------------------------
        # 6. Hard fallback (guaranteed)
        # ----------------------------
        return fallback_plan(question, restaurant="Gamba")
