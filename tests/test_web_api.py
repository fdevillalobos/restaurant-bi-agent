import os
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.db import DatabaseError
from app.tenant_store import User
from app.vera import QueryResult, VeraChartSpec, VeraQueryExecutionError, VeraResponse
from app.web_api import router


def _client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class WebApiTests(unittest.TestCase):
    def setUp(self):
        os.environ["WEB_SESSION_SECRET"] = "test-secret"

    def test_login_sets_session_and_returns_me(self):
        user = User(id=1, email="owner@example.com", password_hash="hash", role="user", dsn_id=10)
        with patch("app.web_api.get_user_by_email", return_value=user), \
             patch("app.web_api.verify_password", return_value=True), \
             patch("app.web_api.get_dsn_by_id", return_value={"id": 10, "name": "Client"}), \
             patch("app.web_api.list_accessible_restaurants", return_value=[{"name": "A"}]):
            res = _client().post("/api/login", json={"email": "owner@example.com", "password": "pw"})
        self.assertEqual(res.status_code, 200)
        self.assertIn("vera_session", res.cookies)
        self.assertEqual(res.json()["restaurants"], ["A"])

    def test_select_rejects_inaccessible_restaurant(self):
        user = User(id=1, email="owner@example.com", password_hash="hash", role="user", dsn_id=10)
        client = _client()
        with patch("app.web_api.get_user_by_email", return_value=user), \
             patch("app.web_api.verify_password", return_value=True), \
             patch("app.web_api.get_user_by_id", return_value=user), \
             patch("app.web_api.get_dsn_by_id", return_value={"id": 10, "name": "Client"}), \
             patch("app.web_api.list_accessible_restaurants", return_value=[{"name": "A"}]):
            client.post("/api/login", json={"email": "owner@example.com", "password": "pw"})
            res = client.post("/api/restaurants/select", json={"restaurant_names": ["B"]})
        self.assertEqual(res.status_code, 403)

    def test_chat_returns_structured_payload_without_html(self):
        user = User(id=1, email="owner@example.com", password_hash="hash", role="user", dsn_id=10)
        vera_response = VeraResponse(
            action="answer",
            message="Sales increased.",
            rows=[{"day": "2026-06-01", "gross_sales": 1000}],
            table="",
            chart_bytes=None,
            chart_caption=None,
            sql="SELECT 1",
            executed_queries=[QueryResult("sales", "SELECT 1", [{"day": "2026-06-01", "gross_sales": 1000}], "")],
            recommendations=["Compare dayparts."],
            suggested_next_questions=["Which products drove it?"],
            chart_spec=VeraChartSpec(type="bar", title="Sales", x="day", y="gross_sales"),
        )
        client = _client()
        with patch("app.web_api.get_user_by_email", return_value=user), \
             patch("app.web_api.verify_password", return_value=True), \
             patch("app.web_api.get_user_by_id", return_value=user), \
             patch("app.web_api.get_dsn_by_id", return_value={"id": 10, "name": "Client", "dsn": "postgres://x"}), \
             patch("app.web_api.list_accessible_restaurants", return_value=[{"name": "A"}]), \
             patch("app.web_api.get_user_conversation_memory", return_value=[]), \
             patch("app.web_api.get_restaurant_knowledge", return_value={}), \
             patch("app.web_api.answer_with_vera", return_value=vera_response), \
             patch("app.web_api.append_user_conversation_memory") as append_memory, \
             patch("app.web_api.append_web_chat_message") as append_web:
            client.post("/api/login", json={"email": "owner@example.com", "password": "pw"})
            res = client.post("/api/chat", json={"message": "Sales?", "restaurant_names": ["A"], "include_debug": True})
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["message"], "Sales increased.")
        self.assertEqual(data["charts"][0]["type"], "bar")
        self.assertEqual(data["tables"][0]["rows"][0]["gross_sales"], 1000)
        self.assertIn("debug", data)
        append_memory.assert_called_once()
        self.assertEqual(append_web.call_count, 2)

    def test_chat_resolves_pending_clarification_with_original_question(self):
        user = User(id=1, email="owner@example.com", password_hash="hash", role="user", dsn_id=10)
        clarify_response = VeraResponse(
            action="clarify",
            message="Do you mean May vs April?",
            rows=[],
            table="",
            chart_bytes=None,
            chart_caption=None,
            sql=None,
            executed_queries=[],
        )
        answer_response = VeraResponse(
            action="answer",
            message="May sales were lower than April.",
            rows=[{"period": "2026-04-01", "gross_sales": 10}, {"period": "2026-05-01", "gross_sales": 8}],
            table="",
            chart_bytes=None,
            chart_caption=None,
            sql="SELECT 1",
            executed_queries=[QueryResult("sales", "SELECT 1", [], "")],
        )
        client = _client()
        with patch("app.web_api.get_user_by_email", return_value=user), \
             patch("app.web_api.verify_password", return_value=True), \
             patch("app.web_api.get_user_by_id", return_value=user), \
             patch("app.web_api.get_dsn_by_id", return_value={"id": 10, "name": "Client", "dsn": "postgres://x"}), \
             patch("app.web_api.list_accessible_restaurants", return_value=[{"name": "A"}]), \
             patch("app.web_api.get_user_conversation_memory", return_value=[]), \
             patch("app.web_api.get_restaurant_knowledge", return_value={}), \
             patch("app.web_api.answer_with_vera", side_effect=[clarify_response, answer_response]) as answer_with_vera, \
             patch("app.web_api.append_user_conversation_memory"), \
             patch("app.web_api.append_web_chat_message"):
            client.post("/api/login", json={"email": "owner@example.com", "password": "pw"})
            client.post("/api/chat", json={"message": "How much were sales last month vs previous?", "restaurant_names": ["A"]})
            res = client.post("/api/chat", json={"message": "May completed vs April. Both complete.", "restaurant_names": ["A"]})
        self.assertEqual(res.status_code, 200)
        second_question = answer_with_vera.call_args_list[1].args[0]
        self.assertIn("Original question: How much were sales last month vs previous?", second_question)
        self.assertIn("User clarification answer: May completed vs April. Both complete.", second_question)
        self.assertIn("Resolve the prior ambiguity and proceed", second_question)

    def test_chat_database_error_returns_assistant_message(self):
        user = User(id=1, email="owner@example.com", password_hash="hash", role="user", dsn_id=10)
        client = _client()
        with patch("app.web_api.get_user_by_email", return_value=user), \
             patch("app.web_api.verify_password", return_value=True), \
             patch("app.web_api.get_user_by_id", return_value=user), \
             patch("app.web_api.get_dsn_by_id", return_value={"id": 10, "name": "Client", "dsn": "postgres://x"}), \
             patch("app.web_api.list_accessible_restaurants", return_value=[{"name": "A"}]), \
             patch("app.web_api.get_user_conversation_memory", return_value=[]), \
             patch("app.web_api.get_restaurant_knowledge", return_value={}), \
             patch("app.web_api.answer_with_vera", side_effect=DatabaseError("canceling statement due to statement timeout")), \
             patch("app.web_api.append_user_conversation_memory") as append_memory, \
             patch("app.web_api.append_web_chat_message") as append_web:
            client.post("/api/login", json={"email": "owner@example.com", "password": "pw"})
            res = client.post("/api/chat", json={"message": "Big diagnostic?", "restaurant_names": ["A"], "include_debug": True})
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["action"], "answer")
        self.assertIn("timed out", data["message"])
        self.assertEqual(data["tables"], [])
        self.assertIn("debug", data)
        append_memory.assert_called_once()
        self.assertEqual(append_web.call_count, 2)

    def test_chat_execution_error_debug_includes_failed_sql(self):
        user = User(id=1, email="owner@example.com", password_hash="hash", role="user", dsn_id=10)
        error = VeraQueryExecutionError(
            "canceling statement due to statement timeout",
            purpose="Find top products by daypart",
            sql="SELECT product_name, SUM(total) FROM sales WHERE restaurant_name = %(restaurant)s GROUP BY 1",
            params={"restaurant": "A"},
        )
        client = _client()
        with patch("app.web_api.get_user_by_email", return_value=user), \
             patch("app.web_api.verify_password", return_value=True), \
             patch("app.web_api.get_user_by_id", return_value=user), \
             patch("app.web_api.get_dsn_by_id", return_value={"id": 10, "name": "Client", "dsn": "postgres://x"}), \
             patch("app.web_api.list_accessible_restaurants", return_value=[{"name": "A"}]), \
             patch("app.web_api.get_user_conversation_memory", return_value=[]), \
             patch("app.web_api.get_restaurant_knowledge", return_value={}), \
             patch("app.web_api.answer_with_vera", side_effect=error), \
             patch("app.web_api.append_user_conversation_memory"), \
             patch("app.web_api.append_web_chat_message"):
            client.post("/api/login", json={"email": "owner@example.com", "password": "pw"})
            res = client.post("/api/chat", json={"message": "Top lunch vs dinner products?", "restaurant_names": ["A"], "include_debug": True})
        self.assertEqual(res.status_code, 200)
        debug = res.json()["debug"]
        self.assertEqual(debug["failed_query"]["purpose"], "Find top products by daypart")
        self.assertIn("GROUP BY 1", debug["failed_query"]["sql"])
        self.assertEqual(debug["failed_query"]["params"], {"restaurant": "A"})

    def test_chat_history_returns_stored_web_messages(self):
        user = User(id=1, email="owner@example.com", password_hash="hash", role="user", dsn_id=10)
        stored = [
            {"id": 1, "role": "user", "content": "Sales?", "payload": None, "selected_restaurants": ["A"], "created_at": "2026-06-04T10:00:00"},
            {
                "id": 2,
                "role": "assistant",
                "content": "Sales increased.",
                "payload": {"action": "answer", "message": "Sales increased.", "tables": [], "charts": [], "recommendations": [], "suggested_next_questions": []},
                "selected_restaurants": ["A"],
                "created_at": "2026-06-04T10:00:01",
            },
        ]
        client = _client()
        with patch("app.web_api.get_user_by_email", return_value=user), \
             patch("app.web_api.verify_password", return_value=True), \
             patch("app.web_api.get_user_by_id", return_value=user), \
             patch("app.web_api.get_dsn_by_id", return_value={"id": 10, "name": "Client"}), \
             patch("app.web_api.list_accessible_restaurants", return_value=[{"name": "A"}]), \
             patch("app.web_api.list_web_chat_messages", return_value=stored):
            client.post("/api/login", json={"email": "owner@example.com", "password": "pw"})
            res = client.get("/api/chat/history")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["messages"][1]["payload"]["message"], "Sales increased.")


if __name__ == "__main__":
    unittest.main()
