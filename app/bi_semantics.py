from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal, List, Dict

from app.schema_pack import SCHEMA


MetricType = Literal[
    "gross_sales",
    "item_revenue",
    "covers",
    "expense_total",
    "payment_total",
]


@dataclass(frozen=True)
class MetricDef:
    key: MetricType
    description: str
    base_table: str
    expression_sql: str  # SQL aggregation expression
    # Additional default filters beyond canceled/restaurant
    extra_filters: List[str]


class BISemantics:
    """
    Canonical business logic. The agent should always prefer these
    definitions unless the user explicitly overrides.
    """

    def __init__(self) -> None:
        self.metrics: Dict[MetricType, MetricDef] = {
            "gross_sales": MetricDef(
                key="gross_sales",
                description="Total sales based on sales.total (restaurant-scoped).",
                base_table="sales",
                expression_sql="SUM(sales.total)",
                extra_filters=[
                    # We will refine this in Module 3 once you confirm the exact sale_state values.
                    # For now, keep it permissive to avoid excluding valid revenue.
                    # Example later: "sales.sale_state = 'paid'"
                ],
            ),
            "item_revenue": MetricDef(
                key="item_revenue",
                description="Line-item revenue based on items.price * items.quantity (excluding canceled items).",
                base_table="items",
                expression_sql="SUM(items.price * items.quantity)",
                extra_filters=[],
            ),
            "covers": MetricDef(
                key="covers",
                description="Total covers/guests based on sales.num_customers.",
                base_table="sales",
                expression_sql="SUM(sales.num_customers)",
                extra_filters=[],
            ),
            "expense_total": MetricDef(
                key="expense_total",
                description="Total operational expenses based on expenses.amount (excluding canceled expenses).",
                base_table="expenses",
                expression_sql="SUM(expenses.amount)",
                extra_filters=[],
            ),
            "payment_total": MetricDef(
                key="payment_total",
                description="Total payments collected based on payments.amount (excluding canceled payments).",
                base_table="payments",
                expression_sql="SUM(payments.amount)",
                extra_filters=[],
            ),
        }

    def metric(self, key: MetricType) -> MetricDef:
        return self.metrics[key]

    def build_default_where(
        self,
        table: str,
        restaurant_param: str = ":restaurant",
        extra_filters: Optional[List[str]] = None,
    ) -> List[str]:
        clauses = []
        clauses.extend(SCHEMA.default_filters_sql(table, restaurant_param=restaurant_param))
        if extra_filters:
            clauses.extend(extra_filters)
        return clauses


SEMANTICS = BISemantics()
