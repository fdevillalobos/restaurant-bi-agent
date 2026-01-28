from __future__ import annotations

from typing import Any, Dict, List, Optional

import os
import psycopg
from psycopg import connect
from psycopg.rows import dict_row

from app.config import settings
from app.sql_safety import validate_select_only, ensure_limit, UnsafeSQL


class DatabaseError(Exception):
    pass


def run_select(
    sql: str,
    params: dict | None = None,
    preview: bool = True,
    statement_timeout_ms: int | None = None,
):

    try:
        with connect(settings.database_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                # statement timeout (override or default)
                timeout = (
                    statement_timeout_ms
                    if statement_timeout_ms is not None
                    else settings.statement_timeout_ms
                )

                cur.execute(f"SET LOCAL statement_timeout = {int(timeout)};")
                cur.execute(sql, params or {})

                preview_limit = (
                    getattr(settings, "preview_limit", None)
                    or getattr(settings, "default_preview_limit", None)
                    or int(os.getenv("DEFAULT_PREVIEW_LIMIT", "200"))
                )

                if preview:
                    rows = cur.fetchmany(int(preview_limit))
                else:
                    rows = cur.fetchall()

                return rows

    except Exception as e:
        raise DatabaseError(str(e)) from e
