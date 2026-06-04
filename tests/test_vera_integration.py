import json
import unittest
from unittest.mock import patch

from app.vera import answer_with_vera


class _Choice:
    def __init__(self, content):
        self.message = type("Message", (), {"content": content})()


class _Response:
    def __init__(self, content):
        self.choices = [_Choice(content)]


def _completion(payload):
    return _Response(json.dumps(payload))


class VeraIntegrationTests(unittest.TestCase):
    def test_clarify_does_not_query_database(self):
        plan_payload = {
            "action": "clarify",
            "clarifying_question": "Which period should I analyze?",
            "queries": [],
            "chart": None,
            "knowledge_question": None,
        }
        with patch("app.vera.chat_completion", return_value=_completion(plan_payload)), \
             patch("app.vera.run_select") as run_select:
            response = answer_with_vera("How are sales?", ["A"], "postgres://x", [], {})
        self.assertEqual(response.action, "clarify")
        self.assertEqual(response.message, "Which period should I analyze?")
        run_select.assert_not_called()

    def test_query_executes_and_finalizes_with_chart(self):
        plan_payload = {
            "action": "query",
            "clarifying_question": None,
            "queries": [
                {
                    "purpose": "daily sales",
                    "sql": "SELECT DATE(sales.created_at) AS day, SUM(sales.total) AS gross_sales FROM sales WHERE sales.sale_state = 'CLOSED' AND sales.restaurant = %(restaurant)s GROUP BY day ORDER BY day",
                }
            ],
            "chart": {
                "type": "line",
                "title": "Daily sales",
                "x": "day",
                "y": "gross_sales",
                "label": None,
                "caption": "Sales trend",
            },
            "knowledge_question": None,
        }
        final_payload = {
            "answer": "Sales rose over the period.",
            "recommendations": ["Compare against last week."],
            "suggested_next_questions": ["Which daypart drove the increase?"],
            "knowledge_to_save": None,
        }
        rows = [
            {"day": "2026-06-01", "gross_sales": 1000},
            {"day": "2026-06-02", "gross_sales": 1200},
        ]
        with patch("app.vera.chat_completion", side_effect=[_completion(plan_payload), _completion(final_payload)]), \
             patch("app.vera.run_select", return_value=rows) as run_select, \
             patch("app.vera.make_chart", return_value=b"png") as make_chart:
            response = answer_with_vera("Sales by day?", ["A"], "postgres://x", [], {})
        self.assertEqual(response.action, "answer")
        self.assertIn("Sales rose", response.message)
        self.assertEqual(response.chart_bytes, b"png")
        self.assertEqual(response.chart_caption, "Sales trend")
        run_select.assert_called_once()
        make_chart.assert_called_once()


if __name__ == "__main__":
    unittest.main()
