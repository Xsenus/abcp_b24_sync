# b24_client.py
import logging
import time
import re
import requests
from typing import Any, Optional, Dict, List

from config import (
    B24_WEBHOOK_URL,
    REQUESTS_TIMEOUT,
    REQUESTS_RETRIES,
    REQUESTS_RETRY_BACKOFF,
    RATE_LIMIT_SLEEP,
)

log = logging.getLogger(__name__)

# -------------------------
# Утилиты нормализации
# -------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _normalize_email(email: Optional[str]) -> Optional[str]:
    """
    Валидируем e-mail по простой маске user@host.tld.
    Возвращаем None, если некорректен/пустой.
    """
    if not email:
        return None
    e = email.strip()
    return e if _EMAIL_RE.match(e) else None

def _normalize_phone(phone: Optional[str]) -> Optional[str]:
    """
    Оставляем только цифры и возможный ведущий '+'. Слишком короткие номера отбрасываем.
    """
    if not phone:
        return None
    p = phone.strip()
    sign = "+" if p.startswith("+") else ""
    digits = re.sub(r"\D", "", p)
    # минимальная длина: 6 цифр (условно)
    if len(digits) < 6:
        return None
    return f"{sign}{digits}"

# -------------------------
# Базовые вызовы
# -------------------------

def _to_int(x: Any) -> int:
    """Безопасное преобразование результата Bitrix24 в int с внятной ошибкой."""
    if isinstance(x, bool):
        raise ValueError("Expected int-like value, got bool")
    if isinstance(x, int):
        return x
    try:
        return int(str(x).strip())
    except Exception as e:
        raise ValueError(f"Cannot convert to int: {x!r}") from e


def _call(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Вызов REST Bitrix24 с повторами и подробной диагностикой:
    - не прерываемся на r.raise_for_status(); сначала пробуем разобрать JSON и вытянуть error_description
    - логируем 4xx с телом ответа
    """
    url = B24_WEBHOOK_URL.rstrip("/") + "/" + method

    def do() -> Dict[str, Any]:
        r = requests.post(url, json=params, timeout=REQUESTS_TIMEOUT)
        # Пытаемся всегда разобрать JSON — даже на 4xx, чтобы достать описание
        try:
            data: Any = r.json()
        except ValueError:
            data = None

        if r.status_code >= 400:
            # Максимально информативная ошибка
            if isinstance(data, dict):
                err = data.get("error")
                desc = data.get("error_description")
                raise RuntimeError(f"B24 {method} HTTP {r.status_code}: {err or 'ERROR'} - {desc or data}")
            # Если не JSON — поднимаем HTTPError с текстом ответа
            r.raise_for_status()

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected B24 response type: {type(data)}")

        if "error" in data:
            # Bitrix может вернуть 200 с полем error
            raise RuntimeError(f"B24 error: {data.get('error')} - {data.get('error_description')}")

        return data

    # Повторы
    retries = int(REQUESTS_RETRIES or 0)
    backoff = float(REQUESTS_RETRY_BACKOFF or 1.0)
    last: Optional[Exception] = None

    for attempt in range(0, retries + 1):
        try:
            if attempt:
                log.warning("B24 RETRY %s (%d/%d)", method, attempt, retries)
            data = do()
            time.sleep(float(RATE_LIMIT_SLEEP or 0))
            return data
        except Exception as e:
            last = e
            if attempt >= retries:
                break
            time.sleep(backoff * (attempt + 1))

    assert last is not None
    raise last

# -------------------------
# API-обёртки
# -------------------------

def add_contact_quick(
    name: Optional[str],
    last_name: Optional[str],
    second_name: Optional[str],
    phone: Optional[str],
    email: Optional[str],
    comment: str,
) -> int:
    # Нормализуем поля контакта
    n_phone = _normalize_phone(phone)
    n_email = _normalize_email(email)

    fields: Dict[str, Any] = {
        "NAME": name or "",
        "LAST_NAME": last_name or "",
        "SECOND_NAME": second_name or "",
        "OPENED": "Y",
        "COMMENTS": comment or "",
    }
    if n_phone:
        fields["PHONE"] = [{"VALUE": n_phone, "VALUE_TYPE": "WORK"}]
    if n_email:
        fields["EMAIL"] = [{"VALUE": n_email, "VALUE_TYPE": "WORK"}]

    log.info(
        "B24 CONTACT ADD (quick): name=%r, last=%r, has_phone=%s, has_email=%s",
        fields.get("NAME"), fields.get("LAST_NAME"), bool(n_phone), bool(n_email),
    )
    data = _call("crm.contact.add", {"fields": fields})
    return _to_int(data.get("result"))


def find_contact_by_phone_or_email(phone: Optional[str], email: Optional[str]) -> Optional[int]:
    """
    Ищем контакт по телефону/почте. Пробуем нормализованные значения.
    Возвращаем ID первого найденного контакта, иначе None.
    """
    queries: List[Dict[str, Any]] = []
    n_phone = _normalize_phone(phone)
    n_email = _normalize_email(email)

    if n_phone:
        queries.append({"filter": {"PHONE": n_phone}, "select": ["ID"]})
    if n_email:
        queries.append({"filter": {"EMAIL": n_email}, "select": ["ID"]})

    for q in queries:
        data = _call("crm.contact.list", q)
        result = data.get("result")
        if isinstance(result, list) and result:
            first = result[0]
            if isinstance(first, dict) and "ID" in first:
                try:
                    cid = _to_int(first["ID"])
                    log.debug("B24 CONTACT FOUND: %s -> id=%s", q["filter"], cid)
                    return cid
                except Exception:
                    continue
    return None


def add_or_update_contact(
    name: Optional[str],
    last_name: Optional[str],
    second_name: Optional[str],
    phone: Optional[str],
    email: Optional[str],
    comment: str,
) -> int:
    """
    Ищем контакт по телефону/почте; если найден — обновляем, иначе создаём.
    """
    contact_id = find_contact_by_phone_or_email(phone, email)

    n_phone = _normalize_phone(phone)
    n_email = _normalize_email(email)

    fields: Dict[str, Any] = {
        "NAME": name or "",
        "LAST_NAME": last_name or "",
        "SECOND_NAME": second_name or "",
        "OPENED": "Y",
        "COMMENTS": comment or "",
    }
    if n_phone:
        fields["PHONE"] = [{"VALUE": n_phone, "VALUE_TYPE": "WORK"}]
    if n_email:
        fields["EMAIL"] = [{"VALUE": n_email, "VALUE_TYPE": "WORK"}]

    if contact_id is not None:
        log.info("B24 CONTACT UPDATE: id=%s", contact_id)
        _call("crm.contact.update", {"id": contact_id, "fields": fields})
        return contact_id
    else:
        log.info(
            "B24 CONTACT ADD: name=%r, last=%r, has_phone=%s, has_email=%s",
            fields.get("NAME"), fields.get("LAST_NAME"), bool(n_phone), bool(n_email),
        )
        data = _call("crm.contact.add", {"fields": fields})
        return _to_int(data.get("result"))


def add_deal_with_fields(fields: Dict[str, Any]) -> int:
    log.info(
        "B24 DEAL ADD (Users funnel): title=%r, category=%r, stage=%r, has_contact=%s",
        fields.get("TITLE"), fields.get("CATEGORY_ID"), fields.get("STAGE_ID"), bool(fields.get("CONTACT_ID")),
    )
    data = _call("crm.deal.add", {"fields": fields})
    return _to_int(data.get("result"))
