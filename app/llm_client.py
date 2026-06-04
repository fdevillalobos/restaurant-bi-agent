from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from openai import OpenAI


@dataclass(frozen=True)
class LLMConfig:
    provider: str
    model: str
    base_url: Optional[str] = None


def get_llm_config() -> LLMConfig:
    provider = os.getenv("LLM_PROVIDER", "openrouter").strip().lower() or "openrouter"
    if provider not in {"openrouter", "openai"}:
        raise ValueError("LLM_PROVIDER must be 'openrouter' or 'openai'.")

    if provider == "openrouter":
        return LLMConfig(
            provider="openrouter",
            model=os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-v4-flash").strip(),
            base_url=os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip(),
        )

    return LLMConfig(
        provider="openai",
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip(),
        base_url=None,
    )


def get_llm_client() -> OpenAI:
    cfg = get_llm_config()
    if cfg.provider == "openrouter":
        api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY is not set.")
        headers: dict[str, str] = {}
        referer = os.getenv("OPENROUTER_HTTP_REFERER", "").strip()
        title = os.getenv("OPENROUTER_APP_TITLE", "").strip()
        if referer:
            headers["HTTP-Referer"] = referer
        if title:
            headers["X-Title"] = title
        return OpenAI(api_key=api_key, base_url=cfg.base_url, default_headers=headers or None)

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    return OpenAI(api_key=api_key)


def chat_completion(
    messages: list[dict[str, Any]],
    *,
    temperature: float = 0.0,
    response_format: Optional[dict[str, str]] = None,
    max_tokens: Optional[int] = None,
):
    client = get_llm_client()
    cfg = get_llm_config()
    kwargs: dict[str, Any] = {
        "model": cfg.model,
        "messages": messages,
        "temperature": temperature,
    }
    if response_format is not None:
        kwargs["response_format"] = response_format
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    return client.chat.completions.create(**kwargs)


def smoke_test() -> dict[str, str]:
    cfg = get_llm_config()
    resp = chat_completion(
        [
            {"role": "system", "content": "Reply with exactly: OK"},
            {"role": "user", "content": "Connection test."},
        ],
        temperature=0.0,
        max_tokens=10,
    )
    content = (resp.choices[0].message.content or "").strip()
    return {"provider": cfg.provider, "model": cfg.model, "status": content or "(empty)"}
