from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Any


_SCHEMA_PATH = Path(__file__).parent / "schema" / "fudo_schema.json"


def load_schema() -> Dict[str, Any]:
    with _SCHEMA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def schema_prompt() -> str:
    """
    Returns a concise schema + business rules prompt for the LLM.
    """
    data = load_schema()
    tables: Dict[str, Any] = data.get("tables", {})

    lines: List[str] = []
    lines.append("You are a SQL generator for a restaurant analytics database (Postgres).")
    lines.append("Return ONLY JSON with keys: sql, expected_result, notes.")
    lines.append("")
    lines.append("Business rules (must follow):")
    lines.append("- Always filter to completed sales: sales.sale_state = 'CLOSED'.")
    lines.append("- Always use created_at for time filters (never closed_at).")
    lines.append("- Gross sales default = SUM(sales.total).")
    lines.append("- Use payments.amount only when breaking down by payment method.")
    lines.append("- Use items.price * items.quantity for item revenue (items.price is historical).")
    lines.append("- If the question is about a specific product's sales/revenue, use items.price * items.quantity (not sales.total).")
    lines.append("- Do NOT use products.price for revenue (it's current price).")
    lines.append("- Always include restaurant scoping using %(restaurant)s param.")
    lines.append("- DB timezone is correct; no manual timezone conversion.")
    lines.append("")
    lines.append("Joins:")
    lines.append("- items.sale_id = sales.uuid")
    lines.append("- items.product_id = products.uuid")
    lines.append("- products.category_id = product_categories.uuid")
    lines.append("- product_categories.parent_category_id = product_categories.uuid")
    lines.append("- payments.sale_id = sales.uuid")
    lines.append("- payments.pay_method_id = payment_methods.uuid")
    lines.append("- discounts.sale_id = sales.uuid")
    lines.append("")
    lines.append("Tables:")
    for tname, tinfo in tables.items():
        desc = tinfo.get("description") or ""
        lines.append(f"- {tname}: {desc}")
        for col in tinfo.get("columns", []):
            cname = col.get("name")
            ctype = col.get("type")
            cdesc = col.get("description") or ""
            lines.append(f"  - {cname} ({ctype}): {cdesc}")
        lines.append("")

    return "\n".join(lines).strip()
