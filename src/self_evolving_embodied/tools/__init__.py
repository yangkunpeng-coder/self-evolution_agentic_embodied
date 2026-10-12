"""Trusted capability catalog and bounded informational model tool dispatch."""

from .contracts import Effect, ToolHandler, ToolKind, ToolSpec
from .presets import research_tool_catalog
from .registry import PluggableToolRegistry, ToolResult

__all__ = ["Effect", "PluggableToolRegistry", "ToolRegistry", "ToolHandler", "ToolKind", "ToolResult", "ToolSpec", "research_tool_catalog"]

# Generic-facing alias; the original harness.ToolRegistry remains legacy-specific.
ToolRegistry = PluggableToolRegistry
