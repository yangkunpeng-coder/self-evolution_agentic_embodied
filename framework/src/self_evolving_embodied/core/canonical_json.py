"""MVP canonical JSON is intentionally narrower than RFC 8785."""
from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any

from .contracts import DomainError


def _validate(value: Any) -> None:
    if value is None or type(value) in (str, int, bool) or isinstance(value, StrEnum):
        return
    if isinstance(value, list):
        for item in value:
            _validate(item)
        return
    if isinstance(value, dict) and all(type(k) is str for k in value):
        for item in value.values():
            _validate(item)
        return
    raise DomainError("NON_CANONICAL_JSON", "Only object/list/string/int/bool/null permitted")


def canonical_bytes(value: Any) -> bytes:
    _validate(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()
