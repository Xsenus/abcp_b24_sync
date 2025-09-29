# abcp_client.py

# Импорт стандартного логгера
import logging
# Импорт функции задержки между запросами для бережного rate-limit
import time
# HTTP-клиент для вызова API ABCP
import requests
# Типы для аннотаций (итераторы, опциональные значения, словари)
from typing import Iterator, Optional, Dict, Any
# Тип даты для фильтрации «сегодня»
from datetime import date

# Импорт конфигурации из .env через наш модуль config
from config import (
    ABCP_BASE_URL,            # базовый URL эндпоинта /cp/users
    ABCP_USERLOGIN,           # логин для ABCP API
    ABCP_USERPSW,             # пароль/ключ для ABCP API (НЕ логируем)
    ABCP_LIMIT,               # размер страницы
    ABCP_MAX_PAGES,           # ограничение количества страниц (может быть None)
    REQUESTS_TIMEOUT,         # таймаут HTTP-запросов
    REQUESTS_RETRIES,         # число повторов при ошибках
    REQUESTS_RETRY_BACKOFF,   # коэффициент backoff между повторами
    RATE_LIMIT_SLEEP,         # пауза между запросами
)
# Универсальный помощник «с повторами» (экспоненциальный backoff реализуем в utils)
from utils import with_retries

# Инициализируем модульный логгер (имя = abcp_client)
log = logging.getLogger(__name__)

# -------- Локальные константы c жёсткими типами, чтобы убрать Optional для Pylance --------

# Таймаут запроса как целое число (сек)
_REQ_TIMEOUT: int = int(REQUESTS_TIMEOUT or 20)
# Количество повторов как целое число
_RETRIES: int = int(REQUESTS_RETRIES or 3)
# Базовая задержка между повторами как float
_BACKOFF: float = float(REQUESTS_RETRY_BACKOFF or 1.5)
# Пауза между удачными запросами (rate-limit) как float
_SLEEP: float = float(RATE_LIMIT_SLEEP or 0.0)
# Лимит записей на страницу как целое число
_LIMIT: int = int(ABCP_LIMIT or 500)
# Максимум страниц — может быть None (тогда без лимита), иначе приводим к int
_MAX_PAGES: Optional[int] = int(ABCP_MAX_PAGES) if ABCP_MAX_PAGES is not None else None

# Защитный предел страниц при «сегодняшнем» обходе (чтобы не перебирать всё)
_TODAY_MAX_PAGES: int = 5  # при необходимости вынесем в конфиг


def _fetch_page(skip: int, limit: int) -> Dict[str, Any]:
    """
    Загружает одну страницу пользователей ABCP.
    :param skip: смещение (сколько записей пропустить)
    :param limit: размер страницы
    :return: распарсенный JSON-словарь ответа
    """
    # Формируем query-параметры запроса (пароль НЕ логируем)
    params: Dict[str, Any] = {
        "userlogin": ABCP_USERLOGIN,  # логин
        "userpsw": ABCP_USERPSW,      # пароль (секрет)
        "limit": limit,               # размер страницы
        "skip": skip,                 # смещение
        "format": "p",                # формат «p» согласно вашему примеру
    }

    # Отладочно фиксируем старт запроса (без секрета)
    log.debug("ABCP GET %s?skip=%s&limit=%s&format=p&userlogin=%s",
              ABCP_BASE_URL, skip, limit, ABCP_USERLOGIN)

    # Внутренняя функция, непосредственно выполняющая HTTP-вызов
    def do() -> Dict[str, Any]:
        # Выполняем GET на ABCP_BASE_URL с параметрами и таймаутом
        r = requests.get(ABCP_BASE_URL, params=params, timeout=_REQ_TIMEOUT)
        # Бросаем исключение при HTTP-ошибке (4xx/5xx)
        r.raise_for_status()
        # Пытаемся распарсить JSON
        data = r.json()
        # Проверяем тип, ожидаем словарь (dict)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected ABCP response type: {type(data)}")
        # Возвращаем распарсенный JSON
        return data

    # Вызываем do() с повторами при ошибках (retries/backoff настроены в константах выше)
    data = with_retries(do, retries=_RETRIES, backoff=_BACKOFF)

    # Логируем успешный ответ на уровне DEBUG с краткой сводкой
    items = data.get("items")
    log.debug("ABCP page fetched: skip=%s, limit=%s, items_count=%s",
              skip, limit, (len(items) if isinstance(items, list) else "n/a"))

    # Делаем небольшую паузу для соблюдения rate-limit (если задана)
    if _SLEEP > 0:
        log.debug("Rate-limit sleep: %s sec", _SLEEP)
        time.sleep(_SLEEP)

    # Возвращаем тело ответа
    return data

def _fetch_count() -> int:
    """
    Получает общее количество записей (count) одной лёгкой выборкой:
    GET /cp/users?limit=0&skip=0&format=p&userlogin=...&userpsw=...
    """
    params: Dict[str, Any] = {
        "userlogin": ABCP_USERLOGIN,
        "userpsw": ABCP_USERPSW,
        "limit": 0,
        "skip": 0,
        "format": "p",
    }

    log.debug("ABCP COUNT %s?limit=0&skip=0&format=p&userlogin=%s", ABCP_BASE_URL, ABCP_USERLOGIN)

    def do() -> int:
        r = requests.get(ABCP_BASE_URL, params=params, timeout=_REQ_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict) or "count" not in data:
            raise RuntimeError("ABCP count response has no 'count'")
        try:
            return int(str(data["count"]))
        except Exception as e:
            raise RuntimeError(f"ABCP count is not int-like: {data.get('count')!r}") from e

    cnt = with_retries(do, retries=_RETRIES, backoff=_BACKOFF)
    log.info("ABCP total count: %s", cnt)
    return cnt

def iter_all_users() -> Iterator[Dict[str, Any]]:
    """
    Итерирует по всем пользователям ABCP постранично.
    :yield: словарь пользователя (как в JSON ABCP)
    """
    # Начальное смещение
    skip = 0
    # Номер страницы (для лога/ограничения)
    page = 0
    # Размер страницы, фиксируем один раз локально
    limit = _LIMIT

    # Информируем о старте общей итерации по всем пользователям
    log.info("ABCP iterate all users: start, limit=%s, max_pages=%s", limit, _MAX_PAGES)

    # Бесконечный цикл, прервёмся по пустой странице или достижении лимита страниц
    while True:
        # Если задан максимум страниц и мы его достигли — выходим
        if _MAX_PAGES is not None and page >= _MAX_PAGES:
            log.warning("ABCP_MAX_PAGES reached at page=%s, stopping.", page)
            break

        # Загружаем страницу (skip/limit)
        payload = _fetch_page(skip=skip, limit=limit)

        # Извлекаем массив пользователей из ответа
        items = payload.get("items") or []

        # Если нет элементов — это сигнал окончания данных
        if not items:
            log.info("ABCP iterate all users: no items on page=%s (skip=%s). Done.", page, skip)
            break

        # Логируем прогресс страницы и количество найденных записей
        log.info("ABCP page=%s fetched: items=%s (skip=%s, limit=%s)",
                 page, len(items), skip, limit)

        # Поочерёдно отдаём наружу каждого пользователя
        for it in items:
            # При желании можно логировать идентификаторы (если есть)
            user_id = it.get("userId") or it.get("userID") or it.get("id")
            reg_date = it.get("registrationDate")
            log.debug("ABCP yield user: userId=%r, registrationDate=%r", user_id, reg_date)
            yield it

        # Увеличиваем смещение на размер фактически полученной порции
        processed = len(items)
        skip += processed
        # Переходим к следующей странице
        page += 1

    # Финальный лог о завершении итерации
    log.info("ABCP iterate all users: finished at page=%s, last skip=%s", page, skip)


def iter_today_users(today: Optional[date] = None) -> Iterator[Dict[str, Any]]:
    """
    Итерирует по пользователям, зарегистрированным «сегодня».

    ВАЖНО: теперь НЕ вызываем iter_all_users (чтобы не обходить всё).
    Идём постранично через _fetch_page и останавливаемся рано:
    - как только встретим страницу, где все regDate < today (при сортировке по убыванию);
    - или по достижении защитного лимита страниц.

    Клиентская фильтрация по полю 'registrationDate', начинающемуся с 'YYYY-MM-DD'.
    :param today: дата «сегодня» (для тестов можно подставить)
    :yield: словарь пользователя, отфильтрованный по текущей дате
    """
    # Если дата не передана — берём системную
    today = today or date.today()
    # Строка сравнения вида 'YYYY-MM-DD'
    today_str = today.strftime("%Y-%m-%d")

    # Логируем старт инкрементальной выборки
    log.info("ABCP iterate today users: start for date=%s", today_str)

    try:
        total = _fetch_count()
        if total <= 0:
            log.info("ABCP today: total=0 — done.")
            return

        limit = _LIMIT
        last_skip = ((total - 1) // limit) * limit 
        pages_checked = 0
        seen_today = False

        skip = last_skip
        while skip >= 0:
            if pages_checked >= _TODAY_MAX_PAGES:
                log.warning("ABCP today(backward): reached safeguard pages limit=%s, stop early", _TODAY_MAX_PAGES)
                break

            payload = _fetch_page(skip=skip, limit=limit)
            items = payload.get("items") or []
            if not items:
                log.info("ABCP today(backward): empty page at skip=%s — stop.", skip)
                break

            todays = []
            for it in items:
                reg = (it.get("registrationDate") or "").strip()
                if reg.startswith(today_str):
                    todays.append(it)

            log.info("ABCP today(backward) skip=%s: todays=%s", skip, len(todays))

            if todays:
                seen_today = True
                for it in todays:
                    user_id = it.get("userId") or it.get("userID") or it.get("id")
                    log.debug("ABCP today match: userId=%r, registrationDate=%r", user_id, it.get("registrationDate"))
                    yield it
            else:
                if seen_today:
                    log.info("ABCP today(backward): first non-today page after todays — stop early.")
                    break

            pages_checked += 1
            skip -= limit

        log.info("ABCP iterate today users: finished for date=%s (backward scan)", today_str)
        return

    except Exception as e:
        log.warning("ABCP today(backward) failed (%s) — fallback to forward scan.", e)

    skip = 0
    page = 0
    limit = _LIMIT
    pages_yielded = 0

    while True:
        if page >= _TODAY_MAX_PAGES:
            log.warning("ABCP today: reached safeguard pages limit=%s, stop early", _TODAY_MAX_PAGES)
            break

        payload = _fetch_page(skip=skip, limit=limit)
        items = payload.get("items") or []
        if not items:
            log.info("ABCP today: no items on page=%s (skip=%s). Done.", page, skip)
            break

        todays = []
        older_than_today = True 

        for it in items:
            reg = (it.get("registrationDate") or "").strip()
            day = reg[:10] if len(reg) >= 10 else ""
            if day >= today_str:
                older_than_today = False
            if day == today_str:
                todays.append(it)

        if todays:
            for it in todays:
                user_id = it.get("userId") or it.get("userID") or it.get("id")
                log.debug("ABCP today match: userId=%r, registrationDate=%r", user_id, it.get("registrationDate"))
                yield it
            pages_yielded += 1
        else:
            log.debug("ABCP today: no matches on page=%s", page)

        if older_than_today:
            log.info("ABCP today: page=%s is older than %s — stop early", page, today_str)
            break

        skip += len(items)
        page += 1

    log.info("ABCP iterate today users: finished for date=%s (pages with matches=%s)", today_str, pages_yielded)
