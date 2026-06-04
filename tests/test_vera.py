import json
import unittest
from unittest.mock import patch

from app.sql_safety import UnsafeSQL
from app.vera import MAX_VERA_QUERIES, normalize_postgres_sql, plan_with_vera
from app import vera as vera_module


class _Choice:
    def __init__(self, content):
        self.message = type("Message", (), {"content": content})()


class _Response:
    def __init__(self, content):
        self.choices = [_Choice(content)]


def _mock_completion(payload):
    return _Response(json.dumps(payload))


class VeraPlanTests(unittest.TestCase):
    def test_clarify_plan_parses(self):
        payload = {
            "action": "clarify",
            "clarifying_question": "Which period should I analyze?",
            "queries": [],
            "chart": None,
            "knowledge_question": None,
        }
        with patch("app.vera.chat_completion", return_value=_mock_completion(payload)):
            plan = plan_with_vera("How are we doing?", ["A"], [], {})
        self.assertEqual(plan.action, "clarify")
        self.assertEqual(plan.clarifying_question, "Which period should I analyze?")

    def test_query_plan_is_limited_to_three_queries(self):
        query = {
            "purpose": "gross sales",
            "sql": "SELECT SUM(sales.total) AS gross_sales FROM sales WHERE sales.sale_state = 'CLOSED' AND sales.restaurant = %(restaurant)s",
        }
        payload = {
            "action": "query",
            "clarifying_question": None,
            "queries": [query, query, query, query],
            "chart": {"type": "none"},
            "knowledge_question": None,
        }
        with patch("app.vera.chat_completion", return_value=_mock_completion(payload)):
            plan = plan_with_vera("Sales?", ["A"], [], {})
        self.assertEqual(len(plan.queries), MAX_VERA_QUERIES)

    def test_missing_restaurant_scope_rejected(self):
        payload = {
            "action": "query",
            "clarifying_question": None,
            "queries": [
                {
                    "purpose": "unsafe",
                    "sql": "SELECT SUM(sales.total) AS gross_sales FROM sales WHERE sales.sale_state = 'CLOSED'",
                }
            ],
            "chart": None,
            "knowledge_question": None,
        }
        with patch("app.vera.chat_completion", return_value=_mock_completion(payload)):
            with self.assertRaises(UnsafeSQL):
                plan_with_vera("Sales?", ["A"], [], {})

    def test_non_select_rejected(self):
        payload = {
            "action": "query",
            "clarifying_question": None,
            "queries": [
                {
                    "purpose": "unsafe",
                    "sql": "DELETE FROM sales WHERE restaurant = %(restaurant)s",
                }
            ],
            "chart": None,
            "knowledge_question": None,
        }
        with patch("app.vera.chat_completion", return_value=_mock_completion(payload)):
            with self.assertRaises(UnsafeSQL):
                plan_with_vera("Delete?", ["A"], [], {})

    def test_multi_statement_rejected(self):
        payload = {
            "action": "query",
            "clarifying_question": None,
            "queries": [
                {
                    "purpose": "unsafe",
                    "sql": "SELECT 1 AS value WHERE %(restaurant)s IS NOT NULL; SELECT 2 AS value",
                }
            ],
            "chart": None,
            "knowledge_question": None,
        }
        with patch("app.vera.chat_completion", return_value=_mock_completion(payload)):
            with self.assertRaises(UnsafeSQL):
                plan_with_vera("Run two?", ["A"], [], {})

    def test_multi_restaurant_scope_rewrite_handles_whitespace(self):
        sql, params = vera_module._apply_restaurant_scope(
            "SELECT 1 FROM sales WHERE sales.restaurant=%(restaurant)s",
            ["A", "B"],
        )
        self.assertIn("ANY(%(restaurants)s)", sql)
        self.assertEqual(params["restaurants"], ["A", "B"])
        self.assertEqual(params["restaurant"], "A")

    def test_round_with_scale_casts_expression_to_numeric(self):
        sql = "SELECT ROUND(((COALESCE(a.total, 0) - COALESCE(b.total, 0)) / NULLIF(b.total, 0)) * 100, 2) AS pct"
        normalized = normalize_postgres_sql(sql)
        self.assertIn(")::numeric, 2)", normalized)
        self.assertIn("ROUND((", normalized)


if __name__ == "__main__":
    unittest.main()
