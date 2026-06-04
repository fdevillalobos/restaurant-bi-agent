import tempfile
import unittest
from pathlib import Path

from app.tenant_store import (
    export_restaurant_knowledge_markdown,
    trim_conversation_messages,
    _normalize_email,
)


class TenantStoreHelperTests(unittest.TestCase):
    def test_memory_trim_keeps_last_30_exchanges(self):
        history = []
        for i in range(35):
            history.append({"role": "user", "content": f"q{i}"})
            history.append({"role": "assistant", "content": f"a{i}"})
        trimmed = trim_conversation_messages(history)
        self.assertEqual(len(trimmed), 60)
        self.assertEqual(trimmed[0]["content"], "q5")
        self.assertEqual(trimmed[-1]["content"], "a34")

    def test_markdown_export_uses_runtime_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = export_restaurant_knowledge_markdown(
                "Client DSN",
                7,
                "Fine Dining Norte",
                "Has delivery only on weekends.",
                base_dir=tmp,
            )
            self.assertTrue(path.exists())
            self.assertEqual(path.parent.name, "client-dsn")
            self.assertEqual(path.name, "fine-dining-norte.md")
            text = Path(path).read_text(encoding="utf-8")
            self.assertIn("# Fine Dining Norte", text)
            self.assertIn("Has delivery only on weekends.", text)

    def test_email_normalization_is_case_insensitive(self):
        self.assertEqual(_normalize_email(" Owner@Example.COM "), "owner@example.com")


if __name__ == "__main__":
    unittest.main()
