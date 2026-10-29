"""Small retry helper for Delta commit conflicts and transient IO."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
LOGGER = logging.getLogger("northline.retry")

_RETRY_TOKENS = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "MetadataChangedException",
    "CommitFailedException",
    "ProtocolChangedException",
)


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    text = f"{type(exc).__name__}: {exc}"
    return any(token in text for token in _RETRY_TOKENS)


def call_with_retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    backoff_seconds: float = 0.2,
    retry_if: Callable[[BaseException], bool] = is_retryable,
    operation: str = "delta-write",
) -> T:
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last = exc
            if attempt >= attempts or not retry_if(exc):
                raise
            delay = backoff_seconds * (2 ** (attempt - 1))
            LOGGER.warning(
                "retrying %s after %s (attempt %s/%s, sleep %.2fs)",
                operation,
                type(exc).__name__,
                attempt,
                attempts,
                delay,
            )
            time.sleep(delay)
    assert last is not None
    raise last
