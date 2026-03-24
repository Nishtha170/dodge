import json
import os
from typing import Any, Dict, List

import requests


def _provider_defaults(provider: str) -> Dict[str, str]:
    if provider == "groq":
        return {
            "base_url": "https://api.groq.com/openai/v1",
            "model": "llama3-8b-8192",
        }
    if provider == "openrouter":
        return {
            "base_url": "https://openrouter.ai/api/v1",
            "model": "meta-llama/llama-3.1-8b-instruct",
        }
    return {
        "base_url": os.environ.get("LLM_BASE_URL", ""),
        "model": os.environ.get("LLM_MODEL", ""),
    }


def chat_completion(messages: List[Dict[str, str]], response_format: Dict[str, Any] | None = None) -> str:
    provider = os.environ.get("LLM_PROVIDER", "openrouter").lower()
    defaults = _provider_defaults(provider)
    base_url = os.environ.get("LLM_BASE_URL", defaults["base_url"])
    model = os.environ.get("LLM_MODEL", defaults["model"])
    api_key = os.environ.get("LLM_API_KEY", "")
    if not base_url or not model or not api_key:
        raise RuntimeError("LLM configuration missing. Set LLM_API_KEY, LLM_MODEL, and LLM_BASE_URL or LLM_PROVIDER.")

    url = base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if provider == "openrouter":
        headers["HTTP-Referer"] = os.environ.get("OPENROUTER_REFERRER", "http://localhost")
        headers["X-Title"] = os.environ.get("OPENROUTER_TITLE", "O2C Graph Explorer")

    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": float(os.environ.get("LLM_TEMPERATURE", "0.1")),
    }
    if response_format:
        payload["response_format"] = response_format

    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]
