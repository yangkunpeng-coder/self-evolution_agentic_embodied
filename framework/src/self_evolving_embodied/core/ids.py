from datetime import UTC, datetime
from uuid import uuid4


def uid() -> str:
    return str(uuid4())


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")
