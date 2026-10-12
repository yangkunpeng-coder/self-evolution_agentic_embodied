"""Online LLM API adapter, separate from embodied action adapters."""

from .openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider

__all__ = ["OpenAICompatibleConfig", "OpenAICompatibleProvider"]
