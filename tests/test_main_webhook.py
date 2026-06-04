import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

import app.main as main


class _FakeTelegramApp:
    bot = object()

    def __init__(self):
        self.process_update = AsyncMock()


class MainWebhookTests(unittest.TestCase):
    def test_telegram_webhook_processes_update(self):
        fake = _FakeTelegramApp()
        with patch.object(main, "_telegram_app", fake), \
             patch("app.main.Update.de_json", return_value={"update": 1}):
            res = TestClient(main.app).post("/telegram/webhook", json={"update_id": 1})
        self.assertEqual(res.status_code, 200)
        fake.process_update.assert_awaited_once_with({"update": 1})

    def test_production_requires_non_default_web_session_secret(self):
        with patch.dict("os.environ", {"RAILWAY_SERVICE_ID": "svc", "WEB_SESSION_SECRET": "dev-secret-change-me"}, clear=True):
            with self.assertRaises(RuntimeError):
                main._require_production_session_secret()

    def test_local_allows_default_web_session_secret(self):
        with patch.dict("os.environ", {"WEB_SESSION_SECRET": "dev-secret-change-me"}, clear=True):
            main._require_production_session_secret()


if __name__ == "__main__":
    unittest.main()
