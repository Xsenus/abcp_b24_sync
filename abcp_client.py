# abcp_client.py

import logging
import time
from datetime import date, datetime, time as dt_time
from typing import Any, Dict, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter

from config import (
    ABCP_BASE_URL,
    ABCP_LIMIT,
    ABCP_MAX_PAGES,
    ABCP_USERLOGIN,
    ABCP_USERPSW,
    RATE_LIMIT_SLEEP,
    REQUESTS_RETRIES,
    REQUESTS_RETRY_BACKOFF,
    REQUESTS_TIMEOUT,
)
from request_analytics import record_http_transaction
from utils import with_retries

log = logging.getLogger(__name__)

_HTTP = requests.Session()
_HTTP.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0))
_HTTP.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0))

_REQ_TIMEOUT: int = int(REQUESTS_TIMEOUT or 20)
_RETRIES: int = int(REQUESTS_RETRIES or 3)
_BACKOFF: float = float(REQUESTS_RETRY_BACKOFF or 1.5)
_RATE_LIMIT_INTERVAL: float = max(float(RATE_LIMIT_SLEEP or 0.0), 3.0)
_LIMIT: int = int(ABCP_LIMIT or 500)
_MAX_PAGES: Optional[int] = int(ABCP_MAX_PAGES) if ABCP_MAX_PAGES is not None else None

_last_request_ts: Optional[float] = None


def _wait_rate_limit() -> None:
    if _RATE_LIMIT_INTERVAL <= 0:
        return

    global _last_request_ts
    if _last_request_ts is None:
        return

    now = time.monotonic()
    wait_until = _last_request_ts + _RATE_LIMIT_INTERVAL
    if wait_until > now:
        delay = wait_until - now
        log.debug("Rate-limit wait before request: %.3fs", delay)
        time.sleep(delay)


def _mark_request_complete() -> None:
    if _RATE_LIMIT_INTERVAL <= 0:
        return

    global _last_request_ts
    _last_request_ts = time.monotonic()


def _format_abcp_datetime(value: datetime) -> str:
    return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def _build_params(skip: int, limit: int, extra_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "userlogin": ABCP_USERLOGIN,
        "userpsw": ABCP_USERPSW,
        "limit": limit,
        "skip": skip,
        "format": "p",
    }
    for key, value in (extra_params or {}).items():
        if value not in (None, ""):
            params[key] = value
    return params


def _safe_params(params: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in params.items() if k != "userpsw"}


def _extract_response_payload(response: Optional[requests.Response]) -> Any:
    if response is None:
        return None
    try:
        return response.json()
    except ValueError:
        body = (response.text or "").strip()
        return body or None


def _fetch_page(skip: int, limit: int, extra_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    params = _build_params(skip=skip, limit=limit, extra_params=extra_params)
    safe_params = _safe_params(params)
    log.debug("ABCP GET %s params=%s", ABCP_BASE_URL, safe_params)
    attempt = 0

    def do() -> Dict[str, Any]:
        nonlocal attempt
        attempt += 1
        _wait_rate_limit()
        started = time.perf_counter()
        response: Optional[requests.Response] = None
        try:
            response = _HTTP.get(ABCP_BASE_URL, params=params, timeout=_REQ_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected ABCP response type: {type(data)}")
            record_http_transaction(
                provider="ABCP",
                operation="users.list",
                http_method="GET",
                url=ABCP_BASE_URL,
                request_payload=safe_params,
                response_payload=data,
                status_code=response.status_code,
                success=True,
                duration_ms=(time.perf_counter() - started) * 1000.0,
                attempt=attempt,
            )
            return data
        except Exception as exc:
            record_http_transaction(
                provider="ABCP",
                operation="users.list",
                http_method="GET",
                url=ABCP_BASE_URL,
                request_payload=safe_params,
                response_payload=_extract_response_payload(response),
                status_code=response.status_code if response is not None else None,
                success=False,
                error=exc,
                duration_ms=(time.perf_counter() - started) * 1000.0,
                attempt=attempt,
            )
            raise
        finally:
            _mark_request_complete()

    data = with_retries(do, retries=_RETRIES, backoff=_BACKOFF)
    items = data.get("items")
    log.debug(
        "ABCP page fetched: skip=%s, limit=%s, items_count=%s, filters=%s",
        skip,
        limit,
        len(items) if isinstance(items, list) else "n/a",
        {k: v for k, v in safe_params.items() if k not in {"userlogin", "limit", "skip", "format"}},
    )
    return data


def _fetch_count(extra_params: Optional[Dict[str, Any]] = None) -> int:
    params = _build_params(skip=0, limit=0, extra_params=extra_params)
    safe_params = _safe_params(params)
    log.debug("ABCP COUNT %s params=%s", ABCP_BASE_URL, safe_params)
    attempt = 0

    def do() -> int:
        nonlocal attempt
        attempt += 1
        _wait_rate_limit()
        started = time.perf_counter()
        response: Optional[requests.Response] = None
        try:
            response = _HTTP.get(ABCP_BASE_URL, params=params, timeout=_REQ_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or "count" not in data:
                raise RuntimeError("ABCP count response has no 'count'")
            record_http_transaction(
                provider="ABCP",
                operation="users.count",
                http_method="GET",
                url=ABCP_BASE_URL,
                request_payload=safe_params,
                response_payload=data,
                status_code=response.status_code,
                success=True,
                duration_ms=(time.perf_counter() - started) * 1000.0,
                attempt=attempt,
            )
            return int(str(data["count"]))
        except Exception as exc:
            record_http_transaction(
                provider="ABCP",
                operation="users.count",
                http_method="GET",
                url=ABCP_BASE_URL,
                request_payload=safe_params,
                response_payload=_extract_response_payload(response),
                status_code=response.status_code if response is not None else None,
                success=False,
                error=exc,
                duration_ms=(time.perf_counter() - started) * 1000.0,
                attempt=attempt,
            )
            raise
        finally:
            _mark_request_complete()

    count = with_retries(do, retries=_RETRIES, backoff=_BACKOFF)
    log.info("ABCP total count: %s", count)
    return count


def _iter_users(
    *,
    extra_params: Optional[Dict[str, Any]] = None,
    label: str,
    max_pages: Optional[int] = _MAX_PAGES,
) -> Iterator[Dict[str, Any]]:
    skip = 0
    page = 0
    limit = _LIMIT
    safe_filters = {
        k: v
        for k, v in (extra_params or {}).items()
        if v not in (None, "")
    }

    log.info(
        "ABCP iterate %s: start, limit=%s, max_pages=%s, filters=%s",
        label,
        limit,
        max_pages,
        safe_filters or "{}",
    )

    while True:
        if max_pages is not None and page >= max_pages:
            log.warning("ABCP_MAX_PAGES reached at page=%s for %s, stopping.", page, label)
            break

        payload = _fetch_page(skip=skip, limit=limit, extra_params=safe_filters)
        items = payload.get("items") or []
        if not items:
            log.info("ABCP iterate %s: no items on page=%s (skip=%s). Done.", label, page, skip)
            break

        log.info("ABCP %s page=%s fetched: items=%s (skip=%s, limit=%s)", label, page, len(items), skip, limit)

        for item in items:
            user_id = item.get("userId") or item.get("userID") or item.get("id")
            log.debug("ABCP yield %s: userId=%r", label, user_id)
            yield item

        processed = len(items)
        skip += processed
        page += 1

        if processed < limit:
            log.info("ABCP iterate %s: short page=%s (items=%s < limit=%s). Done.", label, page - 1, processed, limit)
            break

    log.info("ABCP iterate %s: finished at page=%s, last skip=%s", label, page, skip)


def iter_all_users() -> Iterator[Dict[str, Any]]:
    yield from _iter_users(label="all users", max_pages=_MAX_PAGES)


def iter_users_filtered(
    *,
    date_registered_start: Optional[datetime] = None,
    date_registered_end: Optional[datetime] = None,
    date_updated_start: Optional[datetime] = None,
    date_updated_end: Optional[datetime] = None,
    label: str = "filtered users",
) -> Iterator[Dict[str, Any]]:
    extra_params: Dict[str, Any] = {}
    if date_registered_start is not None:
        extra_params["dateRegisteredStart"] = _format_abcp_datetime(date_registered_start)
    if date_registered_end is not None:
        extra_params["dateRegisteredEnd"] = _format_abcp_datetime(date_registered_end)
    if date_updated_start is not None:
        extra_params["dateUpdatedStart"] = _format_abcp_datetime(date_updated_start)
    if date_updated_end is not None:
        extra_params["dateUpdatedEnd"] = _format_abcp_datetime(date_updated_end)

    yield from _iter_users(extra_params=extra_params, label=label, max_pages=None)


def iter_users_registered_between(start: datetime, end: datetime) -> Iterator[Dict[str, Any]]:
    yield from iter_users_filtered(
        date_registered_start=start,
        date_registered_end=end,
        label="registered window",
    )


def iter_users_updated_between(start: datetime, end: datetime) -> Iterator[Dict[str, Any]]:
    yield from iter_users_filtered(
        date_updated_start=start,
        date_updated_end=end,
        label="updated window",
    )


def iter_today_users(today: Optional[date] = None) -> Iterator[Dict[str, Any]]:
    today = today or date.today()
    start = datetime.combine(today, dt_time(0, 0, 0))
    end = datetime.combine(today, dt_time(23, 59, 59))
    yield from iter_users_filtered(
        date_registered_start=start,
        date_registered_end=end,
        label=f"today users ({today.isoformat()})",
    )
