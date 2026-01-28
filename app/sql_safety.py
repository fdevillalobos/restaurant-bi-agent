from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import sqlglot
from sqlglot import expressions as exp

import re

_PARAM_RE = re.compile(r"%\([a-zA-Z_][a-zA-Z0-9_]*\)s")

def _sanitize_psycopg_named_params(sql: str) -> str:
    """
    Replace psycopg named params like %(restaurant)s with a neutral literal
    so sqlglot can parse the SQL.
    """
    return _PARAM_RE.sub("NULL", sql)

class UnsafeSQL(Exception):
    pass


@dataclass
class SafetyResult:
    normalized_sql: str
    has_limit: bool


def _is_select_statement(tree: exp.Expression) -> bool:
    # Allow WITH ... SELECT ...
    if isinstance(tree, exp.Select):
        return True
    if isinstance(tree, exp.With):
        # WITH <cte> SELECT ...
        return isinstance(tree.this, exp.Select)
    return False


def validate_select_only(sql: str) -> SafetyResult:
    if not sql or not sql.strip():
        raise UnsafeSQL("Empty SQL.")

    # Parse; reject multiple statements
    try:
        sql_for_parse = _sanitize_psycopg_named_params(sql)
        trees = sqlglot.parse(sql_for_parse, read="postgres")
    except Exception as e:
        raise UnsafeSQL(f"SQL parse error: {e}")

    if len(trees) != 1:
        raise UnsafeSQL("Multiple SQL statements are not allowed.")

    tree = trees[0]

    if not _is_select_statement(tree):
        raise UnsafeSQL("Only SELECT queries are allowed.")

    # Basic denylist via AST inspection: block any mutation nodes
    def _maybe(node_name: str):
        return getattr(exp, node_name, None)

    forbidden_names = [
        "Insert",
        "Update",
        "Delete",
        "Create",
        "Alter",
        "Drop",
        "Truncate",
        "Command",
        "Grant",
        "Revoke",
    ]
    forbidden = tuple(x for x in (_maybe(n) for n in forbidden_names) if x is not None)

    for node in tree.walk():
        if forbidden and isinstance(node, forbidden):
            raise UnsafeSQL("DDL/DML statements are not allowed.")

    # Detect limit
    has_limit = any(isinstance(node, exp.Limit) for node in tree.walk())

    return SafetyResult(normalized_sql=sql.strip(), has_limit=has_limit)


def ensure_limit(sql: str, limit: int) -> str:
    """
    Add LIMIT if absent by wrapping the query.
    This is more robust across sqlglot versions than AST mutation.
    """
    sql = (sql or "").strip()
    if not sql:
        return sql

    # remove trailing semicolon if present
    if sql.endswith(";"):
        sql = sql[:-1].strip()

    # Wrap to avoid dialect/AST differences:
    # SELECT * FROM (<original query>) AS __q LIMIT <n>;
    return f"SELECT * FROM ({sql}) AS __q LIMIT {int(limit)}"
