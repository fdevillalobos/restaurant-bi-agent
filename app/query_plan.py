from __future__ import annotations

from typing import List, Optional, Literal
from pydantic import BaseModel, Field


MetricKey = Literal[
    "gross_sales",
    "item_revenue",
    "covers",
    "expense_total",
    "payment_total",
]

TimeGrain = Literal[
    "day",
    "week",
    "month",
    "none",
]


class DateRange(BaseModel):
    start: Optional[str] = Field(None, description="YYYY-MM-DD")
    end: Optional[str] = Field(None, description="YYYY-MM-DD")


class Dimension(BaseModel):
    table: str
    column: str
    alias: Optional[str] = None


class QueryPlan(BaseModel):
    """
    Structured analytical intent.
    This is what the LLM will produce later.
    """

    metric: MetricKey
    base_table: str

    restaurant: str

    date_range: Optional[DateRange] = None
    comparison_dates: Optional[list[str]] = None  # period starts
    comparison_ends: Optional[list[str]] = None   # period ends (same length as comparison_dates)

    # Trend spec (for deterministic growth queries)
    trend_complete_weeks: Optional[bool] = None
    trend_recent_weeks: Optional[int] = None
    trend_prior_weeks: Optional[int] = None
    trend_rank_by: Optional[Literal["delta", "pct_change"]] = None
    time_grain: TimeGrain = "none"

    dimensions: List[Dimension] = []
    limit: Optional[int] = None
