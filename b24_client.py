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

# Строже: финальный TLD минимум из 2 латинских букв (чтобы yandex.r не проходил),
# поддерживаются формы с угловыми скобками и разделителями — мы их предварительно режем.
_EMAIL_RE = re.compile(r"^[^@\s<>]+@[^@\s<>]+\.[A-Za-z]{2,}$")

def _normalize_email(email: Optional[str]) -> Optional[str]:
    """
    Возвращает первый валидный email из строки (поддержка 'a@b,c@d' / 'a@b; c@d' / 'Имя <a@b>').
    Если валидного нет — None (поле EMAIL в B24 не отправляем).
    """
    if not email:
        return None

    for token in re.split(r"[;,\s]+", email):
        t = token.strip().strip("<>").strip('"')
        if _EMAIL_RE.match(t):
            return t
    return None

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

# Нормализация ИНН
def _normalize_inn(inn: Optional[str]) -> Optional[str]:
    """
    Оставляет только цифры. Валидны длины 10 (юрлица) или 12 (физлица/ИП).
    Если невалидно — вернём None (поле UF_CRM_1759218031 не отправляем).
    """
    if not inn:
        return None
    digits = re.sub(r"\D", "", str(inn))
    if len(digits) in (10, 12):
        return digits
    return None

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
# Вспомогательное: детектор ошибки про некорректный e-mail
# -------------------------

def _is_bad_email_error(e: Exception) -> bool:
    msg = str(e).lower()
    return ("email" in msg or "e-mail" in msg) and ("некоррект" in msg or "invalid" in msg)

# -------------------------
# Общий билдер полей контакта
# -------------------------

def _build_contact_fields(
    name: Optional[str],
    phone: Optional[str],
    email: Optional[str],
    comment: str,
    *,
    last_name: str = "",
    second_name: str = "",
    inn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Строит словарь полей для crm.contact.add/update:
    - NAME — как передано (или пустая строка)
    - LAST_NAME/SECOND_NAME — по умолчанию принудительно пустые строки
    - EMAIL/PHONE — нормализуются; при отсутствии не включаются
    - UF_CRM_1759218031 — ИНН (если валиден: 10 или 12 цифр)
    """
    n_phone = _normalize_phone(phone)
    n_email = _normalize_email(email)
    n_inn = _normalize_inn(inn)

    fields: Dict[str, Any] = {
        "NAME": name or "",
        "LAST_NAME": last_name,
        "SECOND_NAME": second_name,
        "OPENED": "Y",
        "COMMENTS": comment or "",
    }
    if n_phone:
        fields["PHONE"] = [{"VALUE": n_phone, "VALUE_TYPE": "WORK"}]
    if n_email:
        fields["EMAIL"] = [{"VALUE": n_email, "VALUE_TYPE": "WORK"}]
    if n_inn:
        fields["UF_CRM_1759218031"] = n_inn

    return fields

# -------------------------
# API-обёртки (общие)
# -------------------------

def add_contact_quick(
    name: Optional[str],
    last_name: Optional[str],
    second_name: Optional[str],
    phone: Optional[str],
    email: Optional[str],
    comment: str,
    *,
    inn: Optional[str] = None,
) -> int:
    """
    БАЗОВЫЙ вариант (совместимость): использует переданные ФИО как есть.
    Для ABCP используйте add_contact_quick_abcp().
    """
    fields = _build_contact_fields(
        name=name,
        phone=phone,
        email=email,
        comment=comment,
        last_name=last_name or "",
        second_name=second_name or "",
        inn=inn,
    )

    log.info(
        "B24 CONTACT ADD (quick): name=%r, last=%r, has_phone=%s, has_email=%s, has_inn=%s",
        fields.get("NAME"), fields.get("LAST_NAME"),
        bool(fields.get("PHONE")), bool(fields.get("EMAIL")), bool(fields.get("UF_CRM_1759218031")),
    )
    # --- Мягкая обработка «битого» e-mail ---
    try:
        data = _call("crm.contact.add", {"fields": fields})
        return _to_int(data.get("result"))
    except Exception as e:
        if _is_bad_email_error(e) and "EMAIL" in fields:
            log.warning("B24 CONTACT ADD: bad email, retry without EMAIL; err=%s", e)
            fields_wo_email = dict(fields)
            fields_wo_email.pop("EMAIL", None)
            data = _call("crm.contact.add", {"fields": fields_wo_email})
            return _to_int(data.get("result"))
        raise


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
    *,
    inn: Optional[str] = None,
) -> int:
    """
    БАЗОВЫЙ вариант (совместимость): ищем по телефону/почте; если найден — обновляем, иначе создаём.
    Для ABCP используйте add_or_update_contact_abcp().
    """
    contact_id = find_contact_by_phone_or_email(phone, email)
    fields = _build_contact_fields(
        name=name,
        phone=phone,
        email=email,
        comment=comment,
        last_name=last_name or "",
        second_name=second_name or "",
        inn=inn,
    )

    # Вспомогательные функции
    def _create(f: Dict[str, Any]) -> int:
        data = _call("crm.contact.add", {"fields": f})
        return _to_int(data.get("result"))

    def _update(cid: int, f: Dict[str, Any]) -> None:
        _call("crm.contact.update", {"id": cid, "fields": f})

    try:
        if contact_id is not None:
            log.info("B24 CONTACT UPDATE: id=%s, has_inn=%s", contact_id, bool(fields.get("UF_CRM_1759218031")))
            _update(contact_id, fields)
            return contact_id
        else:
            log.info(
                "B24 CONTACT ADD: name=%r, has_phone=%s, has_email=%s, has_inn=%s",
                fields.get("NAME"), bool(fields.get("PHONE")), bool(fields.get("EMAIL")),
                bool(fields.get("UF_CRM_1759218031")),
            )
            return _create(fields)
    except Exception as e:
        # --- Мягкая обработка «битого» e-mail для add/update ---
        if _is_bad_email_error(e) and "EMAIL" in fields:
            log.warning("B24 CONTACT add/update: bad email, retry without EMAIL; err=%s", e)
            fields_wo_email = dict(fields)
            fields_wo_email.pop("EMAIL", None)
            if contact_id is not None:
                _update(contact_id, fields_wo_email)
                return contact_id
            else:
                return _create(fields_wo_email)
        raise

# -------------------------
# ABCP-специфичные обёртки
# -------------------------

def add_contact_quick_abcp(
    organization_name: Optional[str],
    phone: Optional[str],
    email: Optional[str],
    comment: str,
    *,
    inn: Optional[str] = None,
) -> int:
    """
    ABCP: NAME ← organizationName; LAST_NAME="", SECOND_NAME="" всегда.
    """
    fields = _build_contact_fields(
        name=organization_name or "",
        phone=phone,
        email=email,
        comment=comment,
        last_name="",
        second_name="",
        inn=inn,
    )

    log.info(
        "B24 CONTACT ADD (ABCP): name=%r, has_phone=%s, has_email=%s, has_inn=%s",
        fields.get("NAME"), bool(fields.get("PHONE")), bool(fields.get("EMAIL")),
        bool(fields.get("UF_CRM_1759218031")),
    )
    try:
        data = _call("crm.contact.add", {"fields": fields})
        return _to_int(data.get("result"))
    except Exception as e:
        if _is_bad_email_error(e) and "EMAIL" in fields:
            log.warning("B24 CONTACT ADD (ABCP): bad email, retry without EMAIL; err=%s", e)
            fields_wo_email = dict(fields)
            fields_wo_email.pop("EMAIL", None)
            data = _call("crm.contact.add", {"fields": fields_wo_email})
            return _to_int(data.get("result"))
        raise


def add_or_update_contact_abcp(
    organization_name: Optional[str],
    phone: Optional[str],
    email: Optional[str],
    comment: str,
    *,
    inn: Optional[str] = None,
) -> int:
    """
    ABCP: ищем по телефону/почте; NAME ← organizationName; LAST_NAME="", SECOND_NAME="" всегда.
    """
    contact_id = find_contact_by_phone_or_email(phone, email)
    fields = _build_contact_fields(
        name=organization_name or "",
        phone=phone,
        email=email,
        comment=comment,
        last_name="",
        second_name="",
        inn=inn,
    )

    def _create(f: Dict[str, Any]) -> int:
        data = _call("crm.contact.add", {"fields": f})
        return _to_int(data.get("result"))

    def _update(cid: int, f: Dict[str, Any]) -> None:
        _call("crm.contact.update", {"id": cid, "fields": f})

    try:
        if contact_id is not None:
            log.info(
                "B24 CONTACT UPDATE (ABCP): id=%s, has_inn=%s",
                contact_id, bool(fields.get("UF_CRM_1759218031")),
            )
            _update(contact_id, fields)
            return contact_id
        else:
            log.info(
                "B24 CONTACT ADD (ABCP): name=%r, has_phone=%s, has_email=%s, has_inn=%s",
                fields.get("NAME"), bool(fields.get("PHONE")), bool(fields.get("EMAIL")),
                bool(fields.get("UF_CRM_1759218031")),
            )
            return _create(fields)
    except Exception as e:
        if _is_bad_email_error(e) and "EMAIL" in fields:
            log.warning("B24 CONTACT add/update (ABCP): bad email, retry without EMAIL; err=%s", e)
            fields_wo_email = dict(fields)
            fields_wo_email.pop("EMAIL", None)
            if contact_id is not None:
                _update(contact_id, fields_wo_email)
                return contact_id
            else:
                return _create(fields_wo_email)
        raise

# -------------------------
# Сделки
# -------------------------

def add_deal_with_fields(fields: Dict[str, Any]) -> int:
    log.info(
        "B24 DEAL ADD (Users funnel): title=%r, category=%r, stage=%r, has_contact=%s",
        fields.get("TITLE"), fields.get("CATEGORY_ID"), fields.get("STAGE_ID"), bool(fields.get("CONTACT_ID")),
    )
    data = _call("crm.deal.add", {"fields": fields})
    return _to_int(data.get("result"))
