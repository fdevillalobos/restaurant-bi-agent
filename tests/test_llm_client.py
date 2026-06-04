import os
import unittest
from unittest.mock import patch

from app.llm_client import get_llm_config


class LLMClientConfigTests(unittest.TestCase):
    def test_openrouter_defaults(self):
        with patch.dict(os.environ, {"LLM_PROVIDER": "openrouter"}, clear=True):
            cfg = get_llm_config()
        self.assertEqual(cfg.provider, "openrouter")
        self.assertEqual(cfg.model, "deepseek/deepseek-v4-flash")
        self.assertEqual(cfg.base_url, "https://openrouter.ai/api/v1")

    def test_openai_provider(self):
        with patch.dict(os.environ, {"LLM_PROVIDER": "openai", "OPENAI_MODEL": "gpt-test"}, clear=True):
            cfg = get_llm_config()
        self.assertEqual(cfg.provider, "openai")
        self.assertEqual(cfg.model, "gpt-test")
        self.assertIsNone(cfg.base_url)

    def test_invalid_provider_rejected(self):
        with patch.dict(os.environ, {"LLM_PROVIDER": "bad"}, clear=True):
            with self.assertRaises(ValueError):
                get_llm_config()


if __name__ == "__main__":
    unittest.main()
