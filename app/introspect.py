from __future__ import annotations

from typing import Dict, List, Any
import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.schema_pack import SCHEMA


def introspect_tables(schema: str = "public") -> Dict[str, Any]:
    """
    Confirms that expected tables/columns exist. Returns a report.
    """
    expected_tables = list(SCHEMA.tables.keys())

    with psycopg.connect(settings.database_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = ANY(%s)
                ORDER BY table_name, ordinal_position
                """,
                (schema, expected_tables),
            )
            rows = cur.fetchall()

    by_table: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_table.setdefault(r["table_name"], []).append(
            {"column": r["column_name"], "type": r["data_type"]}
        )

    missing_tables = [t for t in expected_tables if t not in by_table]

    return {
        "schema": schema,
        "missing_tables": missing_tables,
        "tables": by_table,
    }
