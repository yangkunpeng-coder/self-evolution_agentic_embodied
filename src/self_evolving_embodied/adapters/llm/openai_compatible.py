"""Online Chat Completions provider (OpenAI / DeepSeek / compatible endpoints).

API keys are read from the environment at request time. Secrets and raw provider
exceptions are never included in domain errors. The model is not trusted to
choose tools or authorize actions.
"""
from __future__ import annotations

import os
import math
from dataclasses import dataclass
from urllib.parse import urlsplit

from ...core.contracts import DomainError


@dataclass(frozen=True)
class OpenAICompatibleConfig:
    base_url: str
    model: str
    api_key_env: str = "LLM_API_KEY"
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not isinstance(self.base_url, str):
            raise DomainError("INVALID_LLM_BASE_URL")
        try:
            parsed = urlsplit(self.base_url)
            parsed.port  # reject malformed port values before constructing SDK
        except ValueError:
            raise DomainError("INVALID_LLM_BASE_URL") from None
        if (parsed.scheme != "https" or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment):
            raise DomainError("INVALID_LLM_BASE_URL")
        if not isinstance(self.model, str) or not self.model.strip() or len(self.model) > 128:
            raise DomainError("INVALID_LLM_MODEL")
        if (not isinstance(self.api_key_env, str) or not self.api_key_env.isidentifier()
            or isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(self.timeout_seconds)
            or not 0 < self.timeout_seconds <= 300):
            raise DomainError("INVALID_LLM_CONFIG")

    @classmethod
    def from_env(cls) -> OpenAICompatibleConfig:
        return cls(
            base_url=os.getenv("LLM_BASE_URL", "https://api.openai.com/v1"),
            model=os.getenv("LLM_MODEL", ""),
        )


class OpenAICompatibleProvider:
    def __init__(self, config: OpenAICompatibleConfig) -> None:
        self.config = config

    def complete(self, system_prompt: str, user_prompt: str) -> str:
        key = os.getenv(self.config.api_key_env)
        if not key:
            raise DomainError("LLM_API_KEY_MISSING")
        try:
            from openai import OpenAI  # optional dependency; imported only when called
        except ImportError:
            raise DomainError("LLM_SDK_NOT_INSTALLED") from None
        try:
            with OpenAI(api_key=key, base_url=self.config.base_url,
                        timeout=self.config.timeout_seconds, max_retries=0) as client:
                response = client.chat.completions.create(
                    model=self.config.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                result = response.choices[0].message.content
        except Exception:
            raise DomainError("LLM_REQUEST_FAILED") from None
        if not isinstance(result, str) or not result.strip():
            raise DomainError("LLM_EMPTY_RESPONSE")
        return result
