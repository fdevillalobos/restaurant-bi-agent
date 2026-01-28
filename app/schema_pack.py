from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


# -------------------------
# Core schema structures
# -------------------------

@dataclass(frozen=True)
class Join:
    left_table: str
    left_key: str
    right_table: str
    right_key: str
    join_type: str = "INNER"


@dataclass(frozen=True)
class TableSpec:
    name: str
    pk: str = "uuid"
    role: str = "fact"  # fact | dimension
    description: str = ""
    restaurant_column: Optional[str] = "restaurant"
    default_date_column: Optional[str] = None
    canceled_column: Optional[str] = None


# -------------------------
# Schema Pack
# -------------------------

class SchemaPack:
    """
    Canonical semantic schema for the BI agent.

    This is the single source of truth for:
    - table roles (fact vs dimension)
    - join paths
    - default filters (restaurant, canceled)
    - category hierarchy
    """

    def __init__(self) -> None:
        # -------------------------
        # Tables
        # -------------------------

        self.tables: Dict[str, TableSpec] = {
            # FACT TABLES
            "sales": TableSpec(
                name="sales",
                role="fact",
                description="Order / check header (one row per sale).",
                restaurant_column="restaurant",
                default_date_column="created_at",
                canceled_column=None,  # handled via sale_state later
            ),
            "items": TableSpec(
                name="items",
                role="fact",
                description="Order line items (one row per product added to a sale).",
                restaurant_column="restaurant",
                default_date_column=None,  # items has no created_at; use sales.created_at via join
                canceled_column="canceled",
            ),
            "payments": TableSpec(
                name="payments",
                role="fact",
                description="Payment transactions (can be multiple per sale).",
                restaurant_column="restaurant",
                default_date_column="created_at",
                canceled_column="canceled",
            ),
            "expenses": TableSpec(
                name="expenses",
                role="fact",
                description="Operational expenses.",
                restaurant_column="restaurant",
                default_date_column="created_at",
                canceled_column="canceled",
            ),

            # DIMENSION TABLES
            "products": TableSpec(
                name="products",
                role="dimension",
                description="Menu items master data.",
                restaurant_column="restaurant",
            ),
            "product_categories": TableSpec(
                name="product_categories",
                role="dimension",
                description="Product categories (hierarchical).",
                restaurant_column="restaurant",
            ),
            "payment_methods": TableSpec(
                name="payment_methods",
                role="dimension",
                description="Payment methods master data.",
                restaurant_column="restaurant",
            ),
            "expense_categories": TableSpec(
                name="expense_categories",
                role="dimension",
                description="Expense categories master data.",
                restaurant_column="restaurant",
            ),
        }

        # -------------------------
        # Joins (explicit graph)
        # -------------------------

        self.joins: List[Join] = [
            # sales ↔ items
            Join("items", "sale_id", "sales", "uuid", "INNER"),

            # items ↔ products
            Join("items", "product_id", "products", "uuid", "LEFT"),

            # products ↔ product_categories
            Join("products", "category_id", "product_categories", "uuid", "LEFT"),

            # product_categories self-join (hierarchy)
            Join(
                "product_categories",
                "parent_category_id",
                "product_categories",
                "uuid",
                "LEFT",
            ),

            # payments ↔ sales
            Join("payments", "sale_id", "sales", "uuid", "INNER"),

            # payments ↔ payment_methods
            Join("payments", "pay_method_id", "payment_methods", "uuid", "LEFT"),

            # expenses ↔ expense_categories
            Join("expenses", "exp_category_id", "expense_categories", "uuid", "LEFT"),

            # expenses ↔ payment_methods
            Join("expenses", "pay_method_id", "payment_methods", "uuid", "LEFT"),
        ]

        # -------------------------
        # Hierarchies
        # -------------------------

        self.hierarchies = {
            "product_categories": {
                "id": "uuid",
                "parent_id": "parent_category_id",
                "label": "name",
            }
        }

    # -------------------------
    # Helpers
    # -------------------------

    def get_table(self, name: str) -> TableSpec:
        if name not in self.tables:
            raise KeyError(f"Unknown table: {name}")
        return self.tables[name]

    def default_filters_sql(
        self,
        table: str,
        restaurant_param: str = "%(restaurant)s",
    ) -> List[str]:
        """
        Default WHERE clauses applied unless explicitly overridden.
        """
        t = self.get_table(table)
        clauses: List[str] = []

        if t.restaurant_column:
            clauses.append(f"LOWER({t.name}.{t.restaurant_column}) = LOWER({restaurant_param})")

        if t.canceled_column:
            clauses.append(f"{t.name}.{t.canceled_column} = false")

        return clauses


# Singleton instance
SCHEMA = SchemaPack()
