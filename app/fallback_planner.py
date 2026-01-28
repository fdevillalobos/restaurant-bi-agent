from __future__ import annotations

import re
from datetime import date, timedelta
from app.query_plan import QueryPlan, DateRange, Dimension

def fallback_plan(question: str, restaurant: str) -> QueryPlan:
    q = question.lower().strip()

    # Defaults
    plan = QueryPlan(
        metric="gross_sales",
        base_table="sales",
        restaurant=restaurant,
        time_grain="none",
        dimensions=[],
        date_range=None,
        limit=None,
    )

    # Time phrases (very basic)
    m = re.search(r"last\s+(\d+)\s+days", q)
    if m:
        days = int(m.group(1))
        end = date.today()
        start = end - timedelta(days=days)
        plan.date_range = DateRange(start=str(start), end=str(end))
        plan.time_grain = "day"

    if "last week" in q:
        end = date.today()
        start = end - timedelta(days=7)
        plan.date_range = DateRange(start=str(start), end=str(end))
        plan.time_grain = "day"

    if "last month" in q:
        end = date.today()
        start = end - timedelta(days=30)
        plan.date_range = DateRange(start=str(start), end=str(end))
        plan.time_grain = "day"

    # Routing
    if "top" in q and ("product" in q or "products" in q):
        plan.metric = "item_revenue"
        plan.base_table = "items"
        plan.dimensions = [Dimension(table="products", column="name", alias="product")]

        # For ranking, aggregate over whole period unless explicitly asked
        if not any(k in q for k in ["by day", "per day", "daily", "by week", "by month"]):
            plan.time_grain = "none"

        m2 = re.search(r"top\s+(\d+)", q)
        plan.limit = int(m2.group(1)) if m2 else 10

    if "payment" in q and ("method" in q or "methods" in q):
        plan.metric = "payment_total"
        plan.base_table = "payments"
        plan.dimensions = [Dimension(table="payment_methods", column="name", alias="payment_method")]
        plan.limit = None

    if "expense" in q and ("category" in q or "categories" in q):
        plan.metric = "expense_total"
        plan.base_table = "expenses"
        plan.dimensions = [Dimension(table="expense_categories", column="name", alias="expense_category")]
        plan.limit = None

    if "covers" in q or "guests" in q:
        plan.metric = "covers"
        plan.base_table = "sales"

    return plan
