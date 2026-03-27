from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from config import (
    REQUEST_ANALYTICS_DIR,
    REQUEST_ANALYTICS_ENABLED,
    REQUEST_ANALYTICS_MAX_BODY_CHARS,
)

log = logging.getLogger(__name__)

_LOCK = threading.Lock()
_MAX_DEPTH = 6
_MAX_ITEMS = 50
_SECRET_KEYS = {
    "authorization",
    "password",
    "passwd",
    "psw",
    "secret",
    "token",
    "userpsw",
}


def _truncate_text(value: str, limit: int | None) -> str:
    if limit is None or limit <= 0 or len(value) <= limit:
        return value
    return f"{value[:limit]}...<truncated {len(value) - limit} chars>"


def _normalize_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= _MAX_DEPTH:
        return "<max_depth>"

    if isinstance(value, dict):
        items = list(value.items())
        normalized: dict[str, Any] = {}
        for idx, (key, item) in enumerate(items):
            if idx >= _MAX_ITEMS:
                normalized["__truncated_items__"] = len(items) - _MAX_ITEMS
                break

            key_str = str(key)
            if key_str.lower() in _SECRET_KEYS and item not in (None, ""):
                normalized[key_str] = "***"
            else:
                normalized[key_str] = _normalize_value(item, depth=depth + 1)
        return normalized

    if isinstance(value, (list, tuple, set)):
        items = list(value)
        normalized = [_normalize_value(item, depth=depth + 1) for item in items[:_MAX_ITEMS]]
        if len(items) > _MAX_ITEMS:
            normalized.append({"__truncated_items__": len(items) - _MAX_ITEMS})
        return normalized

    if isinstance(value, bytes):
        return _truncate_text(value.decode("utf-8", errors="replace"), REQUEST_ANALYTICS_MAX_BODY_CHARS)

    if isinstance(value, str):
        return _truncate_text(value, REQUEST_ANALYTICS_MAX_BODY_CHARS)

    if value is None or isinstance(value, (int, float, bool)):
        return value

    return _truncate_text(repr(value), REQUEST_ANALYTICS_MAX_BODY_CHARS)


def mask_url(url: str) -> str:
    if not url:
        return url

    try:
        parts = urlsplit(url)
        segments = [segment for segment in parts.path.split("/") if segment]
        if len(segments) >= 3 and segments[0].lower() == "rest":
            segments[1] = "***"
            segments[2] = "***"
            masked_path = "/" + "/".join(segments)
            if parts.path.endswith("/"):
                masked_path += "/"
            return urlunsplit((parts.scheme, parts.netloc, masked_path, parts.query, parts.fragment))
    except Exception:
        return url

    return url


def _build_log_path(now: datetime) -> Path:
    log_dir = Path(REQUEST_ANALYTICS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"request_analytics_{now.strftime('%Y-%m-%d')}.log"


def record_http_transaction(
    *,
    provider: str,
    operation: str,
    http_method: str,
    url: str,
    request_payload: Any,
    response_payload: Any = None,
    status_code: int | None = None,
    success: bool,
    error: Exception | str | None = None,
    duration_ms: float | None = None,
    attempt: int | None = None,
) -> None:
    if not REQUEST_ANALYTICS_ENABLED:
        return

    now = datetime.now().astimezone()
    entry = {
        "timestamp": now.isoformat(timespec="milliseconds"),
        "provider": provider,
        "operation": operation,
        "http_method": (http_method or "").upper(),
        "url": mask_url(url),
        "attempt": attempt,
        "request": _normalize_value(request_payload),
        "response": _normalize_value(response_payload),
        "success": bool(success),
        "outcome": "positive" if success else "negative",
        "http_status": status_code,
        "duration_ms": round(duration_ms, 2) if duration_ms is not None else None,
        "error": _truncate_text(str(error), REQUEST_ANALYTICS_MAX_BODY_CHARS) if error else None,
    }
    entry = {key: value for key, value in entry.items() if value is not None}

    try:
        line = json.dumps(entry, ensure_ascii=False, default=str)
        log_path = _build_log_path(now)
        with _LOCK:
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)
                fh.write("\n")
    except Exception as exc:
        log.warning("Failed to write request analytics entry: %s", exc)
