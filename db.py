# db.py

# Импортируем типы столбцов и функции для работы с SQLAlchemy Core/DDL.
from sqlalchemy import create_engine, Integer, String, Text, DateTime, Boolean, func
# ORM-инструменты: базовый класс декларативных моделей, типизированные колонки и сессии.
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
# Конструктор запросов (используем для выборок).
from sqlalchemy.sql import select
# Стандартные типы.
from typing import Optional
# Python-тип datetime для корректной аннотации Mapped[datetime].
from datetime import datetime as PyDT
# Работа с путями в файловой системе (для гарантии каталога БД).
from pathlib import Path
# JSON для хранения исходной записи пользователя в БД.
import json
# Логирование операций (инициализация, upsert, meta).
import logging

# Инициируем модульный логгер.
log = logging.getLogger(__name__)


# Базовый класс всех ORM-моделей в проекте (SQLAlchemy 2.x способ).
class Base(DeclarativeBase):
    pass


# Таблица ключ-значение для технических отметок (например, время последнего импорта).
class MetaKV(Base):
    __tablename__ = "meta"  # имя таблицы в SQLite
    # Ключ (PRIMARY KEY), короткая строка — имя метки.
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    # Значение (как текст), формат данных произвольный (например, ISO8601).
    value: Mapped[str] = mapped_column(Text, nullable=False)


# Модель пользователя ABCP, хранимая в локальной БД (SQLite).
class User(Base):
    __tablename__ = "users"  # имя таблицы

    # Внутренний первичный ключ (автоинкремент).
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Уникальный идентификатор пользователя из ABCP (строкой для надёжности).
    abcp_user_id: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)

    # Основные атрибуты пользователя (все опциональные — зависят от ABCP).
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    second_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    surname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    mobile: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    phone: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # Временные поля из ABCP — храним как строки (форматы могут отличаться).
    registration_date: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    update_time: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # Оригинальный JSON пользователя, сериализованный в строку — для трассировки и доп. полей.
    raw_json: Mapped[str] = mapped_column(Text, nullable=False)

    # Поля статуса синхронизации с Bitrix24.
    synced: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    # Дата/время успешной синхронизации (python datetime в аннотации, SQLAlchemy DateTime в колонке).
    synced_at: Mapped[Optional[PyDT]] = mapped_column(DateTime(timezone=False), nullable=True)
    # Сохранённые идентификаторы сущностей в B24 (для идемпотентности).
    b24_contact_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    b24_deal_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # Технические поля аудита (создание/обновление строки на стороне SQLite).
    created_at: Mapped[PyDT] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    updated_at: Mapped[PyDT] = mapped_column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now(), nullable=False)


def get_engine(sqlite_path: str):
    """
    Создаёт SQLAlchemy Engine для подключения к заданному файлу SQLite.
    echo=False — без verbose SQL, future=True — стиль 2.x.
    """
    log.debug("Создание Engine для SQLite по пути: %s", sqlite_path)
    return create_engine(f"sqlite:///{sqlite_path}", echo=False, future=True)


def init_db(sqlite_path: str):
    """
    Инициализирует БД:
      - гарантирует существование каталога для файла;
      - создаёт таблицы по метаданным моделей.
    Возвращает созданный Engine.
    """
    # Абсолютный нормализованный путь к файлу БД.
    db_path = Path(sqlite_path).expanduser().resolve()
    log.info("Инициализация БД: %s", db_path)

    # Если родительская папка отсутствует — создаём её (рекурсивно).
    if db_path.parent and not db_path.parent.exists():
        log.debug("Каталог не найден, создаю: %s", db_path.parent)
        db_path.parent.mkdir(parents=True, exist_ok=True)

    # Создаём Engine и строим таблицы по ORM-моделям.
    engine = get_engine(str(db_path))
    log.debug("Создание таблиц (если отсутствуют)")
    Base.metadata.create_all(engine)
    log.info("Инициализация БД завершена")
    return engine


def upsert_user(session: Session, item: dict) -> User:
    """
    Вставляет или обновляет пользователя по ключу abcp_user_id (upsert).
    Сохраняет оригинальный JSON в поле raw_json.
    Возвращает ORM-объект User (ещё не зафиксированный в БД, пока не session.commit()).
    """
    # Достаём идентификатор пользователя, который может называться по-разному в ABCP.
    abcp_user_id = str(item.get("userId") or item.get("userID") or item.get("id"))
    # Без ID не можем выполнить upsert — валидируем вход.
    assert abcp_user_id, "userId missing"

    # Пробуем найти существующую запись по уникальному индексу abcp_user_id.
    existing: Optional[User] = session.scalar(select(User).where(User.abcp_user_id == abcp_user_id))

    # Сериализуем оригинальный item в строку (UTF-8, без ASCII-escape) для хранения в raw_json.
    payload = json.dumps(item, ensure_ascii=False)

    if existing is None:
        # Нет записи — добавляем новую.
        log.debug("UPSERT: create user abcp_user_id=%s", abcp_user_id)
        u = User(
            abcp_user_id=abcp_user_id,
            name=item.get("name") or None,
            second_name=item.get("secondName") or None,
            surname=item.get("surname") or None,
            email=item.get("email") or None,
            mobile=item.get("mobile") or None,
            phone=item.get("phone") or None,
            city=item.get("city") or None,
            state=str(item.get("state") or ""),
            registration_date=item.get("registrationDate") or None,
            update_time=item.get("updateTime") or None,
            raw_json=payload,
        )
        # Добавляем новый объект в сессию (не фиксируем — это делает вызывающий код).
        session.add(u)
        return u
    else:
        # Запись существует — обновляем отдельные поля только если в item они присутствуют.
        log.debug("UPSERT: update user abcp_user_id=%s", abcp_user_id)
        existing.name = item.get("name") or existing.name
        existing.second_name = item.get("secondName") or existing.second_name
        existing.surname = item.get("surname") or existing.surname
        existing.email = item.get("email") or existing.email
        existing.mobile = item.get("mobile") or existing.mobile
        existing.phone = item.get("phone") or existing.phone
        existing.city = item.get("city") or existing.city
        existing.state = str(item.get("state") or existing.state)
        existing.registration_date = item.get("registrationDate") or existing.registration_date
        existing.update_time = item.get("updateTime") or existing.update_time
        # raw_json всегда обновляем актуальным снимком источника.
        existing.raw_json = payload
        return existing


def set_meta(session: Session, key: str, value: str):
    """
    Устанавливает или обновляет значение в таблице meta по заданному ключу.
    Коммит выполняет вызывающая сторона.
    """
    log.debug("META set: %r = %r", key, value)
    # Пробуем найти существующую запись по ключу.
    m = session.get(MetaKV, key)
    if m is None:
        # Нет записи — создаём новую.
        m = MetaKV(key=key, value=value)
        session.add(m)
    else:
        # Обновляем существующее значение.
        m.value = value


def get_meta(session: Session, key: str) -> Optional[str]:
    """
    Возвращает значение мета-ключа или None, если ключ отсутствует.
    """
    log.debug("META get: %r", key)
    m = session.get(MetaKV, key)
    return m.value if m else None
