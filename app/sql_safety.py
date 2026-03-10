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
    # Allow plain SELECT
    if isinstance(tree, exp.Select):
        return True
    # Allow UNION / UNION ALL / INTERSECT / EXCEPT between SELECTs
    if isinstance(tree, (exp.Union, exp.Intersect, exp.Except)):
        return True
    # Allow WITH ... SELECT ... (CTEs)
    if isinstance(tree, exp.With):
        return isinstance(tree.this, (exp.Select, exp.Union, exp.Intersect, exp.Except))
    return False


# Regex fallback: block DML/DDL keywords at word boundaries
_DENYLIST_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXECUTE|CALL|COPY)\b",
    re.IGNORECASE,
)
# Must start with SELECT or WITH (allowing CTEs)
_SELECT_START_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
# Detect multiple statements: semicolons not inside quotes
_MULTI_STMT_RE = re.compile(r";(?!\s*$)")


def _regex_validate(sql: str) -> SafetyResult:
    """Fallback validator used when sqlglot cannot parse the SQL (e.g. ISODOW, custom PG syntax)."""
    if not _SELECT_START_RE.match(sql):
        raise UnsafeSQL("Only SELECT queries are allowed.")
    if _DENYLIST_RE.search(sql):
        raise UnsafeSQL("DDL/DML statements are not allowed.")
    if _MULTI_STMT_RE.search(sql):
        raise UnsafeSQL("Multiple SQL statements are not allowed.")
    has_limit = bool(re.search(r"\bLIMIT\b", sql, re.IGNORECASE))
    return SafetyResult(normalized_sql=sql.strip(), has_limit=has_limit)


def validate_select_only(sql: str) -> SafetyResult:
    if not sql or not sql.strip():
        raise UnsafeSQL("Empty SQL.")

    # Strip trailing semicolon before parsing
    sql_stripped = sql.strip().rstrip(";").strip()

    # Try AST-based validation via sqlglot first
    try:
        sql_for_parse = _sanitize_psycopg_named_params(sql_stripped)
        trees = sqlglot.parse(sql_for_parse, read="postgres")
        trees = [t for t in trees if t is not None]

        if len(trees) != 1:
            raise UnsafeSQL("Multiple SQL statements are not allowed.")

        tree = trees[0]

        if not _is_select_statement(tree):
            raise UnsafeSQL("Only SELECT queries are allowed.")

        def _maybe(node_name: str):
            return getattr(exp, node_name, None)

        forbidden_names = [
            "Insert", "Update", "Delete", "Create", "Alter",
            "Drop", "Truncate", "Command", "Grant", "Revoke",
        ]
        forbidden = tuple(x for x in (_maybe(n) for n in forbidden_names) if x is not None)
        for node in tree.walk():
            if forbidden and isinstance(node, forbidden):
                raise UnsafeSQL("DDL/DML statements are not allowed.")

        has_limit = any(isinstance(node, exp.Limit) for node in tree.walk())
        return SafetyResult(normalized_sql=sql.strip(), has_limit=has_limit)

    except UnsafeSQL:
        raise  # always propagate explicit safety rejections
    except Exception:
        # sqlglot failed on valid Postgres syntax (e.g. ISODOW, custom casts)
        # Fall back to regex-based check
        return _regex_validate(sql_stripped)


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
