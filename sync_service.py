# sync_service.py

# Логирование шагов импорта/синхронизации
import logging
import re  # для разбора офсета в TZ
# Метки времени для полей синхронизации, и дата для инкрементального импорта
from datetime import datetime, date, timedelta, timezone
# Аннотации типов
from typing import Iterable, Optional, Dict, Any
# Сессии ORM
from sqlalchemy.orm import Session
# Часовые пояса
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Наши модули БД и клиентов
from db import get_engine, init_db, User, upsert_user, set_meta
from abcp_client import iter_all_users, iter_today_users
from b24_client import add_or_update_contact_abcp, add_deal_with_fields
from config import (
    SQLITE_PATH, B24_DEAL_TITLE_PREFIX,             # путь к SQLite и дефолтный префикс для названия сделки
    B24_DEAL_CATEGORY_ID_USERS, B24_DEAL_STAGE_NEW_USERS,  # настройки воронки «Пользователи»
    UF_B24_DEAL_ABCP_USER_ID, UF_B24_DEAL_INN, UF_B24_DEAL_SALDO,  # UF-поля сделки
    UF_B24_DEAL_REG_DATE, UF_B24_DEAL_UPDATE_TIME,
    ABCP_TIMEZONE, B24_OUT_TZ_ISO
)

# Модульный логгер
logger = logging.getLogger(__name__)


def _fmt_user(u: User) -> str:
    """
    Короткая строка-описание пользователя для логов (без «сырых» данных).
    """
    return (
        f"abcp_user_id={u.abcp_user_id!r}, "
        f"name={u.name!r}, surname={u.surname!r}, second_name={u.second_name!r}, "
        f"email={u.email!r}, mobile={u.mobile!r}, phone={u.phone!r}, city={u.city!r}, "
        f"registration_date={u.registration_date!r}, state={u.state!r}"
    )


def import_users(items: Iterable[Dict[str, Any]], *, label: str) -> int:
    """
    Общая функция импорта пользователей в SQLite.
    :param items: итерируемая коллекция словарей пользователей (как из ABCP API)
    :param label: метка для записи в meta (например, 'full' или 'incremental')
    :return: количество обработанных записей
    """
    # Идемпотентно гарантируем, что схема БД существует (устраняет 'no such table').
    init_db(SQLITE_PATH)

    # Создаём Engine для работы с SQLite по сконфигурированному пути
    engine = get_engine(SQLITE_PATH)
    count = 0
    logger.info("Начало импорта (%s) в БД SQLite: %s", label, SQLITE_PATH)

    # Одна сессия на всю операцию — меньше накладных расходов
    with Session(engine) as session:
        for item in items:
            try:
                count += 1
                # Каждую сотую запись подсвечиваем в INFO, остальные — в DEBUG
                if count % 100 == 1:
                    logger.info("Импорт: обработка записи №%d", count)
                logger.debug("Импорт: сырой JSON #%d: %s", count, item)

                # UPSERT пользователя по abcp_user_id
                u = upsert_user(session, item)
                logger.debug("Импорт: upsert %s", _fmt_user(u))

                # Периодически фиксируем транзакцию, чтобы не копить слишком много в памяти
                if count % 500 == 0:
                    session.commit()
                    logger.info("Импорт: промежуточный COMMIT после %d записей", count)
            except Exception as e:
                # Любая ошибка по записи — логируем и откатываем, продолжаем
                logger.exception("Импорт: ошибка на записи #%d: %s", count, e)
                session.rollback()

        # Финальный COMMIT по хвосту
        session.commit()
        logger.info("Импорт: финальный COMMIT, всего записей: %d", count)

        # Обновляем метку в meta: last_{label}_import_at=UTC now
        set_meta(session, f"last_{label}_import_at", datetime.utcnow().isoformat())
        session.commit()
        logger.info("Импорт: записан meta.last_%s_import_at", label)

    logger.info("Импорт (%s) завершён: %d записей", label, count)
    return count


def import_all() -> int:
    """
    Полный импорт всех пользователей ABCP (постранично).
    """
    return import_users(iter_all_users(), label="full")


def import_today(today: Optional[date] = None) -> int:
    """
    Инкрементальный импорт: только зарегистрированные «сегодня».
    """
    return import_users(iter_today_users(today=today), label="incremental")


def _parse_money_ru(s: Optional[str]) -> Optional[float]:
    """
    Преобразует строку вида '-1 582,00' → -1582.00 (float).
    Возвращает None, если пусто или не удалось распарсить.
    """
    if not s:
        logger.debug("Парсинг суммы: пустое значение → None")
        return None
    # Удаляем пробелы/неразрывные пробелы и меняем запятую на точку
    raw = s.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    try:
        val = float(raw)
        logger.debug("Парсинг суммы: %r → %s", s, val)
        return val
    except Exception:
        logger.debug("Парсинг суммы: %r → не удалось распарсить", s)
        return None


# ===== TZ helpers =====

_TZ_OFFSET_RE = re.compile(r'^([+-])(\d{2}):(\d{2})$')

def _tz_from_str(s: str):
    """
    Поддерживает:
      - офсет: '+03:00', '-01:00'
      - IANA: 'Europe/Moscow', 'UTC', ...
    Если база таймзон недоступна (Windows без tzdata), делаем безопасный fallback:
      - 'UTC' -> timezone.utc
      - 'Europe/Moscow' -> UTC+03:00 (в РФ нет DST)
      - иначе -> timezone.utc
    """
    s = (s or "").strip()

    # 1) Явный офсет ±HH:MM
    m = _TZ_OFFSET_RE.match(s)
    if m:
        sign, hh, mm = m.groups()
        minutes = int(hh) * 60 + int(mm)
        if sign == '-':
            minutes = -minutes
        return timezone(timedelta(minutes=minutes))

    # 2) Попытка IANA через ZoneInfo
    try:
        return ZoneInfo(s or "UTC")
    except ZoneInfoNotFoundError as e:
        logging.getLogger(__name__).warning("ZoneInfo not found for %r (%s); using fallback.", s or "UTC", e)
        key = (s or "UTC").strip().lower()
        if key in ("utc", "etc/utc", "z"):
            return timezone.utc
        if key == "europe/moscow":
            # В РФ нет перехода на летнее время — +03:00 стабильно
            return timezone(timedelta(hours=3))
        # общий fallback
        return timezone.utc

def _normalize_dt(s: Optional[str]) -> Optional[str]:
    """
    Нормализует дату/время от ABCP:
      1) аккуратно парсит разные форматы ('YYYY-MM-DD HH:MM[:SS][.mmm]', 'DD.MM.YYYY HH:MM[:SS]', ISO с tz, и т.д.)
      2) трактует как ABCP_TIMEZONE (если на входе нет tz),
      3) конвертирует в B24_OUT_TZ_ISO (IANA или офсет ±HH:MM),
      4) возвращает ISO-8601 с tz: 'YYYY-MM-DDTHH:MM:SS±HH:MM'.
    """
    if not s:
        return None

    # ——— ШАГ 0. Санитайзинг — убираем NBSP/узкие пробелы, приводим T->' ' и отрезаем Z ———
    raw = (
        s.replace("\u00A0", " ")  # NBSP
         .replace("\u2007", " ")  # Figure space
         .replace("\u202F", " ")  # Narrow NBSP
         .strip()
         .replace("T", " ")
         .replace("Z", "")        # ISO 'Z' (UTC)
    )

    # ——— ШАГ 1. Быстрая попытка через fromisoformat ———
    dt: Optional[datetime]
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        dt = None

    # ——— ШАГ 2. Попробуем через набор strptime-шаблонов ———
    if dt is None:
        from datetime import datetime as _DT

        tmp = raw
        # уберём миллисекунды, если есть (наши паттерны без дробной части)
        if "." in tmp:
            left, right = tmp.split(".", 1)
            if right and right[0].isdigit():
                tmp = left

        # Игнорируем возможный встроенный офсет, парсить будем без него
        m = re.search(r'([+-]\d{2}:\d{2})$', tmp)
        if m:
            tmp = tmp[: -len(m.group(1))].strip()

        patterns = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M",
            "%d.%m.%Y",
        ]

        for p in patterns:
            try:
                dt = _DT.strptime(tmp, p)
                break
            except Exception:
                continue

    # ——— ШАГ 3. Если так и не распарсили — вернём исходное, чтобы запись не упала ———
    if dt is None:
        return raw

    # ——— ШАГ 4. Навешиваем исходный TZ и конвертируем в целевой ———
    src_tz = _tz_from_str(ABCP_TIMEZONE or "Europe/Moscow")
    out_tz = _tz_from_str(B24_OUT_TZ_ISO or "Europe/Moscow")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=src_tz)
    else:
        dt = dt.astimezone(src_tz)

    return dt.astimezone(out_tz).isoformat(timespec="seconds")


def _safe_add_or_update_contact(name: str,
                                phone: Optional[str],
                                email: Optional[str],
                                comment: str,
                                *,
                                inn: Optional[str] = None) -> Optional[int]:
    """
    ABCP-логика контакта:
      - NAME ← organizationName (или fallback)
      - LAST_NAME/SECOND_NAME всегда пустые (делает b24_client.add_or_update_contact_abcp)
    Сначала пробуем создать/обновить контакт с email.
    Если Bitrix24 вернул ошибку — повторяем без EMAIL.
    Если снова ошибка — возвращаем None (запись будет пропущена).
    """
    try:
        return add_or_update_contact_abcp(name, phone, email, comment, inn=inn)
    except Exception as e1:
        logger.warning("Контакт: ошибка при add_or_update (с email): %s — пробую без EMAIL", e1)
        try:
            return add_or_update_contact_abcp(name, phone, None, comment, inn=inn)
        except Exception as e2:
            logger.error("Контакт: не удалось даже без EMAIL: %s — пропускаю запись", e2)
            return None


def sync_to_b24(limit: Optional[int] = None) -> int:
    """
    Синхронизирует несинхронизированные записи в Bitrix24:
    - Ищем/обновляем контакт по телефону/email (без дублей) через add_or_update_contact_abcp;
      фамилию/отчество не заполняем, в NAME пишем organizationName (если его нет — «Клиент №{userId}»).
    - Создаём сделку в воронке «Пользователи» (CATEGORY_ID/STAGE_ID) с заполнением UF-полей.
    - Помечаем запись как синхронизированную, фиксируем дату и ID сущностей Б24.

    TITLE сделки в формате: "Клиент №{userId}".
    Дополнительно пишем UF: дата регистрации ABCP и дата обновления ABCP (в ISO-8601 с tz B24_OUT_TZ_ISO).
    :param limit: ограничение количества записей за прогон (None — без лимита)
    :return: число успешно синхронизированных записей
    """
    # Идемпотентно гарантируем, что схема БД существует (устраняет 'no such table').
    init_db(SQLITE_PATH)

    # Подключение к SQLite
    engine = get_engine(SQLITE_PATH)
    synced = 0
    logger.info("Синхронизация в Bitrix24: старт (limit=%s)", limit)

    with Session(engine) as session:
        # Выбираем все записи, где synced == False
        q = session.query(User).filter(User.synced == False)  # noqa: E712
        if limit:
            q = q.limit(limit)

        # Материализуем выборку в «пакет» для логирования общего количества
        batch = list(q)
        logger.info("Синхронизация: к обработке %d записей", len(batch))

        # Если обрабатывать нечего — выходим раньше
        if not batch:
            logger.info("Синхронизация: нет записей для обработки (synced=false).")
            return 0

        # Проходим по каждой записи
        for idx, u in enumerate(batch, start=1):
            logger.info("Синхронизация: #%d → %s", idx, _fmt_user(u))

            # Извлекаем исходный JSON, чтобы подобрать дополнительные поля
            try:
                import json
                j = json.loads(u.raw_json)  # j — словарь исходной записи ABCP
            except Exception:
                j = {}
                logger.debug("Синхронизация: raw_json не разобран (user %s) — использую поля из модели", u.abcp_user_id)

            # Собираем атрибуты, нужные для контакта/сделки
            abcp_user_id = str(j.get("userId") or u.abcp_user_id or "")
            org_name     = (j.get("organizationName") or u.name or "").strip()
            email        = (j.get("email") or u.email or "").strip() or None
            phone        = (j.get("phone") or u.mobile or u.phone or "").strip() or None
            inn          = (j.get("inn") or "").strip()
            saldo_raw    = (j.get("saldo") or "").strip()
            saldo_val    = _parse_money_ru(saldo_raw)
            reg_raw      = (j.get("registrationDate") or u.registration_date or "").strip() or None
            upd_raw      = (j.get("updateTime") or u.update_time or "").strip() or None
            reg_val      = _normalize_dt(reg_raw)
            upd_val      = _normalize_dt(upd_raw)

            # Название сделки
            title = f"Клиент №{abcp_user_id}"
            # Имя контакта — строго organizationName; если пусто — fallback на title
            contact_name = org_name or title

            logger.debug(
                "Синхронизация: поля — abcp_user_id=%r, contact.NAME=%r, has_phone=%s, has_email=%s, inn=%r, saldo_raw=%r, saldo_val=%r",
                abcp_user_id, contact_name, bool(phone), bool(email), inn, saldo_raw, saldo_val
            )

            # Контакт: создаём/обновляем, если нет привязки
            if u.b24_contact_id:
                contact_id = int(u.b24_contact_id)
                logger.debug("B24: reuse contact_id=%s из БД", contact_id)
            else:
                comment = f"ABCP userId: {abcp_user_id}; Город: {u.city or ''}; Регистрация: {u.registration_date or ''}"

                logger.debug(
                    "B24: add_or_update_contact_abcp → START; NAME=%r, has_phone=%s, has_email=%s",
                    contact_name, bool(phone), bool(email)
                )

                contact_id = _safe_add_or_update_contact(contact_name, phone, email, comment, inn=inn)
                if not contact_id:
                    session.rollback()
                    logger.warning("Синхронизация: #%d пропущена (контакт не создан) — abcp_user_id=%s", idx, abcp_user_id)
                    continue

                logger.info("B24: контакт создан/обновлён (NAME=%r), contact_id=%s", contact_name, contact_id)
                logger.debug("B24: контакт %s обновлён/создан; ИНН=%r отправлен в UF_CRM_1759218031", contact_id, inn or None)

                u.b24_contact_id = str(contact_id)
                session.commit()
                logger.debug("B24: contact_id=%s сохранён в БД", contact_id)

            # Готовим поля сделки (воронка «Пользователи»)
            fields: Dict[str, Any] = {
                "TITLE": title,                              # Название сделки
                "CATEGORY_ID": B24_DEAL_CATEGORY_ID_USERS,  # Категория (воронка «Пользователи»)
                "STAGE_ID": B24_DEAL_STAGE_NEW_USERS,       # Стартовая стадия
                "CONTACT_ID": contact_id,                   # Привязка к контакту
                # UF-поля
                UF_B24_DEAL_ABCP_USER_ID: abcp_user_id,
                UF_B24_DEAL_INN: inn,
            }

            # Баланс
            if saldo_val is not None:
                fields[UF_B24_DEAL_SALDO] = saldo_val
            elif saldo_raw:
                fields[UF_B24_DEAL_SALDO] = saldo_raw

            # Даты ABCP (ISO-8601 с tz)
            if reg_val:
                fields[UF_B24_DEAL_REG_DATE] = reg_val
            if upd_val:
                fields[UF_B24_DEAL_UPDATE_TIME] = upd_val

            logger.debug(
                "B24: add_deal_with_fields → START; title=%r, CATEGORY_ID=%r, STAGE_ID=%r, CONTACT_ID=%r, UF_keys=%s",
                title, B24_DEAL_CATEGORY_ID_USERS, B24_DEAL_STAGE_NEW_USERS, contact_id,
                [k for k in fields.keys() if str(k).startswith("UF_")]
            )

            try:
                deal_id = add_deal_with_fields(fields)
                logger.info("B24: сделка создана (воронка «Пользователи», TITLE=%r), deal_id=%s", title, deal_id)

                u.b24_deal_id = str(deal_id)
                u.synced = True
                u.synced_at = datetime.utcnow()
                session.commit()
                synced += 1
                logger.info(
                    "Синхронизация: #%d успешно — COMMIT (contact_id=%s, deal_id=%s)",
                    idx, contact_id, deal_id
                )
            except Exception as e:
                logger.error(
                    "Синхронизация: #%d ошибка создания сделки для abcp_user_id=%s: %s",
                    idx, abcp_user_id, e
                )
                session.rollback()

    logger.info("Синхронизация завершена: успешно %d из %d", synced, len(batch))
    return synced
