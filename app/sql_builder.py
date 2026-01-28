# app/sql_builder.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple
from collections import deque, defaultdict

from app.query_plan import QueryPlan
from app.schema_pack import SCHEMA, Join
from app.bi_semantics import SEMANTICS


@dataclass(frozen=True)
class BuiltQuery:
    sql: str
    params: Dict[str, Any]


# -------------------------
# Time field resolution
# -------------------------

def resolve_time_field(base_table: str) -> Tuple[str, str]:
    """
    items has no created_at in your DB, so for items-based analytics
    we use sales.created_at via items.sale_id -> sales.uuid.
    """
    if base_table == "items":
        return ("sales", "created_at")

    spec = SCHEMA.get_table(base_table)
    if not spec.default_date_column:
        raise ValueError(f"No default date column configured for base table '{base_table}'")
    return (base_table, spec.default_date_column)


# -------------------------
# Minimal join planning via BFS paths
# -------------------------

def _build_adjacency() -> Dict[str, List[Tuple[str, Join]]]:
    adj: Dict[str, List[Tuple[str, Join]]] = defaultdict(list)
    for j in SCHEMA.joins:
        adj[j.left_table].append((j.right_table, j))
        adj[j.right_table].append((j.left_table, j))
    return adj


def _find_path_joins(start: str, target: str) -> List[Join]:
    if start == target:
        return []

    adj = _build_adjacency()
    q = deque([start])
    prev: Dict[str, str | None] = {start: None}
    prev_join: Dict[str, Join] = {}

    while q:
        cur = q.popleft()
        for nxt, join_obj in adj.get(cur, []):
            if nxt in prev:
                continue
            prev[nxt] = cur
            prev_join[nxt] = join_obj
            if nxt == target:
                q.clear()
                break
            q.append(nxt)

    if target not in prev:
        raise ValueError(f"No join path from '{start}' to '{target}'")

    path: List[Join] = []
    node = target
    while prev[node] is not None:
        path.append(prev_join[node])
        node = prev[node]  # type: ignore[assignment]
    path.reverse()
    return path


def _emit_join_clauses(base: str, joins_needed: List[Join]) -> List[str]:
    present: Set[str] = {base}
    clauses: List[str] = []

    remaining = joins_needed[:]
    for _ in range(30):
        progressed = False
        next_remaining: List[Join] = []

        for j in remaining:
            if j.left_table in present and j.right_table not in present:
                clauses.append(
                    f"{j.join_type} JOIN {j.right_table} "
                    f"ON {j.left_table}.{j.left_key} = {j.right_table}.{j.right_key}"
                )
                present.add(j.right_table)
                progressed = True
            elif j.right_table in present and j.left_table not in present:
                clauses.append(
                    f"{j.join_type} JOIN {j.left_table} "
                    f"ON {j.left_table}.{j.left_key} = {j.right_table}.{j.right_key}"
                )
                present.add(j.left_table)
                progressed = True
            else:
                if j.left_table in present and j.right_table in present:
                    progressed = True
                else:
                    next_remaining.append(j)

        remaining = next_remaining
        if not remaining:
            break
        if not progressed:
            break

    if remaining:
        missing = sorted({j.left_table for j in remaining} | {j.right_table for j in remaining})
        raise ValueError(f"Unable to emit join clauses; remaining tables: {missing}")

    return clauses


# -------------------------
# Main builder
# -------------------------

def build_sql(plan: QueryPlan) -> BuiltQuery:
    # Trend complete-weeks query (special fast path)
    if plan.trend_complete_weeks and plan.base_table == "items":
        if not plan.limit:
            limit = 5
        else:
            limit = int(plan.limit)

        recent_weeks = int(plan.trend_recent_weeks or 2)
        prior_weeks = int(plan.trend_prior_weeks or 2)
        total_weeks = recent_weeks + prior_weeks

        # Rank by delta by default
        rank_by = plan.trend_rank_by or "delta"
        order_expr = "delta DESC" if rank_by == "delta" else "pct_change DESC NULLS LAST"

        sql = f"""
    WITH bounds AS (
    SELECT date_trunc('week', now())::timestamp AS week_start_current
    ),
    s AS (
    SELECT s.uuid, s.created_at
    FROM sales s
    CROSS JOIN bounds b
    WHERE LOWER(s.restaurant) = LOWER(%(restaurant)s)
        AND s.sale_state = 'CLOSED'
        AND s.created_at >= (b.week_start_current - interval '{total_weeks * 7} days')
        AND s.created_at <  b.week_start_current
    ),
    agg AS (
    SELECT
        i.product_id,
        SUM(CASE WHEN s.created_at >= ((SELECT week_start_current FROM bounds) - interval '{recent_weeks * 7} days')
                THEN i.price * i.quantity ELSE 0 END) AS recent_rev,
        SUM(CASE WHEN s.created_at <  ((SELECT week_start_current FROM bounds) - interval '{recent_weeks * 7} days')
                THEN i.price * i.quantity ELSE 0 END) AS prior_rev
    FROM s
    JOIN items i ON i.sale_id = s.uuid
    WHERE i.canceled IS NOT TRUE
    GROUP BY i.product_id
    ),
    ranked AS (
    SELECT
        product_id,
        recent_rev,
        prior_rev,
        (recent_rev - prior_rev) AS delta,
        CASE WHEN prior_rev = 0 THEN NULL ELSE (recent_rev - prior_rev) / prior_rev END AS pct_change
    FROM agg
    )
    SELECT
    p.name AS product,
    recent_rev,
    prior_rev,
    delta,
    pct_change
    FROM ranked r
    LEFT JOIN products p ON p.uuid = r.product_id
    ORDER BY {order_expr}
    LIMIT {limit};
    """.strip()

        return BuiltQuery(sql=sql, params={"restaurant": plan.restaurant})

    # Normal path
    metric_def = SEMANTICS.metric(plan.metric)
    base = metric_def.base_table

    # parameters (psycopg named placeholders)
    params: Dict[str, Any] = {"restaurant": plan.restaurant}

    # time field
    time_table, time_col = resolve_time_field(base)

    # SELECT + GROUP BY
    select_parts: List[str] = []
    group_by_aliases: List[str] = []

    if plan.time_grain != "none":
        trunc = {"day": "day", "week": "week", "month": "month"}[plan.time_grain]
        select_parts.append(f"DATE_TRUNC('{trunc}', {time_table}.{time_col}) AS period")
        group_by_aliases.append("period")

    for dim in plan.dimensions:
        alias = dim.alias or f"{dim.table}_{dim.column}"
        select_parts.append(f"{dim.table}.{dim.column} AS {alias}")
        group_by_aliases.append(alias)

    select_parts.append(f"{metric_def.expression_sql} AS value")

    # minimal join plan
    required_tables: Set[str] = {base, time_table} | {d.table for d in plan.dimensions}

    join_set: List[Join] = []
    seen: Set[Tuple[str, str, str, str, str]] = set()

    for t in sorted(required_tables):
        for j in _find_path_joins(base, t):
            key = (j.left_table, j.left_key, j.right_table, j.right_key, j.join_type)
            if key not in seen:
                seen.add(key)
                join_set.append(j)

    join_clauses = _emit_join_clauses(base, join_set)

    # WHERE clauses
    where_parts: List[str] = []

    # Restaurant filter: for items-based queries, filter on SALES (source of truth)
    if base == "items":
        where_parts.append("LOWER(sales.restaurant) = LOWER(%(restaurant)s)")
    else:
        where_parts.append(f"LOWER({base}.restaurant) = LOWER(%(restaurant)s)")

    # Enforce CLOSED whenever we're using sales as the business "completed transaction" gate.
    # For items queries, sales is joined, so we enforce it too.
    if base in ("sales", "items"):
        where_parts.append("sales.sale_state = 'CLOSED'")

    # Canceled flags
    # - For items analytics: ALWAYS exclude canceled items.
    # - For other bases: apply whatever SchemaPack declares.
    if base == "items":
        # treat NULL as "not canceled"
        where_parts.append("items.canceled IS NOT TRUE")
    else:
        t = SCHEMA.get_table(base)
        if t.canceled_column:
            # treat NULL as "not canceled"
            where_parts.append(f"{base}.{t.canceled_column} IS NOT TRUE")

    # date range (inclusive start, inclusive whole end day)
    if plan.date_range:
        if plan.date_range.start:
            params["start_date"] = plan.date_range.start
            where_parts.append(f"{time_table}.{time_col} >= %(start_date)s")
        if plan.date_range.end:
            params["end_date"] = plan.date_range.end
            where_parts.append(f"{time_table}.{time_col} < (%(end_date)s::date + interval '1 day')")

    # comparison_dates: restrict to specific days only
    # This is what enables "yesterday vs same day last week" to return only two rows.
    if plan.comparison_dates:
        if len(plan.comparison_dates) != 2:
            raise ValueError("comparison_dates currently supports exactly 2 dates")

        params["cmp_start_1"] = plan.comparison_dates[0]
        params["cmp_start_2"] = plan.comparison_dates[1]

        # Month comparisons: always restrict to those month buckets
        if plan.time_grain == "month":
            where_parts.append(
                f"DATE_TRUNC('month', {time_table}.{time_col}) IN (%(cmp_start_1)s::date, %(cmp_start_2)s::date)"
            )

            # If we also have explicit ends, use two explicit ranges (MTD vs prior MTD)
            if getattr(plan, "comparison_ends", None):
                if len(plan.comparison_ends) != 2:
                    raise ValueError("comparison_ends must have exactly 2 dates")

                params["cmp_end_1"] = plan.comparison_ends[0]
                params["cmp_end_2"] = plan.comparison_ends[1]

                where_parts.append(
                    "("
                    f"({time_table}.{time_col} >= %(cmp_start_1)s::date AND {time_table}.{time_col} < (%(cmp_end_1)s::date + interval '1 day'))"
                    " OR "
                    f"({time_table}.{time_col} >= %(cmp_start_2)s::date AND {time_table}.{time_col} < (%(cmp_end_2)s::date + interval '1 day'))"
                    ")"
                )
        else:
            # Day-level comparisons: pick exactly the 2 days
            if getattr(plan, "comparison_ends", None):
                # ignore ends for day comparisons
                pass

            params["cmp_date_1"] = plan.comparison_dates[0]
            params["cmp_date_2"] = plan.comparison_dates[1]
            where_parts.append(
                f"DATE({time_table}.{time_col}) IN (%(cmp_date_1)s::date, %(cmp_date_2)s::date)"
            )

    # assemble SQL
    lines: List[str] = []
    lines.append("SELECT")
    lines.append("  " + ",\n  ".join(select_parts))
    lines.append(f"FROM {base}")
    lines.extend(join_clauses)

    if where_parts:
        lines.append("WHERE " + " AND ".join(where_parts))

    if group_by_aliases:
        lines.append("GROUP BY " + ", ".join(group_by_aliases))

    # ordering
    if "period" in group_by_aliases:
        lines.append("ORDER BY period ASC")
    elif plan.limit:
        lines.append("ORDER BY value DESC")

    if plan.limit:
        lines.append(f"LIMIT {int(plan.limit)}")

    return BuiltQuery(sql="\n".join(lines), params=params)
