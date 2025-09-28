# sync_service.py

# Логирование шагов импорта/синхронизации
import logging
# Метки времени для полей синхронизации, и дата для инкрементального импорта
from datetime import datetime, date
# Аннотации типов
from typing import Iterable, Optional, Dict, Any
# Сессии ORM
from sqlalchemy.orm import Session

# Наши модули БД и клиентов
from db import get_engine, init_db, User, upsert_user, set_meta
from abcp_client import iter_all_users, iter_today_users
from b24_client import add_contact_quick, add_or_update_contact, add_deal_with_fields
from config import (
    SQLITE_PATH, B24_DEAL_TITLE_PREFIX,             # путь к SQLite и дефолтный префикс для названия сделки
    B24_DEAL_CATEGORY_ID_USERS, B24_DEAL_STAGE_NEW_USERS,  # настройки воронки «Пользователи»
    UF_B24_DEAL_ABCP_USER_ID, UF_B24_DEAL_INN, UF_B24_DEAL_SALDO,  # UF-поля сделки
    UF_B24_DEAL_REG_DATE, UF_B24_DEAL_UPDATE_TIME,
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


def _normalize_dt(s: Optional[str]) -> Optional[str]:
    """
    Нормализует строку даты/времени к 'YYYY-MM-DD HH:MM:SS', если возможно.
    Если не удалось — возвращает исходную строку/None.
    """
    if not s:
        return None
    raw = s.strip().replace("T", " ").replace("Z", "")
    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return raw


def sync_to_b24(limit: Optional[int] = None) -> int:
    """
    Синхронизирует несинхронизированные записи в Bitrix24:
    - Быстро создаёт контакт (add_contact_quick), если его ещё нет в БД; иначе — переиспользует b24_contact_id.
    - Создаёт сделку в воронке «Пользователи» (CATEGORY_ID/STAGE_ID) с заполнением UF-полей.
    - Помечает запись как синхронизированную, фиксирует дату и ID сущностей B24.

    ДОПОЛНЕНО:
    - Ищем/обновляем контакт по телефону/email (без дублей) через add_or_update_contact;
      фамилию/отчество не заполняем, в NAME пишем ровно то же, что в TITLE сделки.
    - TITLE сделки в формате: "Клиент №{userId}" (требование).
    - В сделку дополнительно пишем UF: Дата регистрации ABCP и Дата обновления ABCP.
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

            # Название сделки; оно же будет NAME контакта
            # title = org_name or f"{B24_DEAL_TITLE_PREFIX} {abcp_user_id}"
            title = f"Клиент №{abcp_user_id}"  # ← требование: записывать именно так
            contact_name = title  # Критерий: имя контакта = название сделки

            logger.debug(
                "Синхронизация: поля — abcp_user_id=%r, title/NAME=%r, has_phone=%s, has_email=%s, inn=%r, saldo_raw=%r, saldo_val=%r",
                abcp_user_id, contact_name, bool(phone), bool(email), inn, saldo_raw, saldo_val
            )

            # Контакт: быстрый сценарий — создаём только если ещё нет ID в БД
            if u.b24_contact_id:
                # Если уже есть ID контакта — переиспользуем
                contact_id = int(u.b24_contact_id)
                logger.debug("B24: reuse contact_id=%s из БД", contact_id)
            else:
                # Комментарий с полезной информацией по источнику
                comment = f"ABCP userId: {abcp_user_id}; Город: {u.city or ''}; Регистрация: {u.registration_date or ''}"

                # НЕ пишем фамилию/отчество — только NAME = TITLE.
                # Ищем/обновляем по телефону/почте — не создаём дубликаты.
                logger.debug(
                    "B24: add_or_update_contact → START; NAME=%r, has_phone=%s, has_email=%s",
                    contact_name, bool(phone), bool(email)
                )
                contact_id = add_or_update_contact(contact_name, "", "", phone, email, comment)
                logger.info("B24: контакт создан/обновлён (NAME=%r), contact_id=%s", contact_name, contact_id)

                # Сохраняем contact_id сразу, чтобы не потерять при возможной ошибке сделки
                u.b24_contact_id = str(contact_id)
                session.commit()
                logger.debug("B24: contact_id=%s сохранён в БД", contact_id)

            # Готовим поля сделки (воронка «Пользователи»)
            fields: Dict[str, Any] = {
                "TITLE": title,                              # Название сделки
                "CATEGORY_ID": B24_DEAL_CATEGORY_ID_USERS,  # Категория (воронка «Пользователи»)
                "STAGE_ID": B24_DEAL_STAGE_NEW_USERS,       # Стартовая стадия
                "CONTACT_ID": contact_id,                   # Привязка к контакту
                # UF-поля (соответствие из вашего ТЗ)
                UF_B24_DEAL_ABCP_USER_ID: abcp_user_id,     # ID клиента ABCP
                UF_B24_DEAL_INN: inn,                       # ИНН
            }

            # Баланс: если удалось распарсить — отправляем числом,
            # иначе — исходной строкой (на случай если UF-поле строкового типа)
            if saldo_val is not None:
                fields[UF_B24_DEAL_SALDO] = saldo_val
            elif saldo_raw:
                fields[UF_B24_DEAL_SALDO] = saldo_raw
                
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
                # Создаём сделку одной командой (быстро)
                deal_id = add_deal_with_fields(fields)
                logger.info("B24: сделка создана (воронка «Пользователи», TITLE=%r), deal_id=%s", title, deal_id)

                # Отмечаем запись как синхронизированную
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
                # Ошибка на уровне сделки — откатываем и продолжаем со следующей записью
                logger.error(
                    "Синхронизация: #%d ошибка создания сделки для abcp_user_id=%s: %s",
                    idx, abcp_user_id, e
                )
                session.rollback()

    # Итоговый лог по количеству успехов
    logger.info("Синхронизация завершена: успешно %d из %d", synced, len(batch))
    return synced
