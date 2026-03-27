# ABCP -> Bitrix24 Sync

Сервис синхронизирует пользователей из ABCP в локальную SQLite-базу, а затем переносит их в Bitrix24.

Что делает проект:

- получает пользователей из ABCP;
- хранит локальный снимок данных в SQLite;
- создаёт или обновляет контакт в Bitrix24;
- создаёт или обновляет сделку в воронке `Пользователи`;
- не пересоздаёт уже найденные сущности в Bitrix24;
- ведёт обычные сервисные логи и отдельную HTTP-аналитику исходящих запросов.

## Актуальная схема работы

Текущее поведение проекта соответствует фактической реализации и live-проверкам от **25 марта 2026** и **27 марта 2026**.

Основная логика:

1. Данные из ABCP сначала попадают в SQLite.
2. В таблице `users` хранится актуальный снимок пользователя и технический флаг `synced`.
3. Если `raw_json` пользователя изменился, запись автоматически снова помечается как `synced=false`.
4. Затем `sync_to_b24()` берёт только несинхронизированные записи и выгружает их в Bitrix24.
5. После успешной выгрузки сохраняются `b24_contact_id`, `b24_deal_id`, `synced=true` и `synced_at`.

Важно:

- регулярный инкрементальный импорт работает по `updateTime`;
- по live-проверке от **25 марта 2026** фильтрация ABCP по `dateRegisteredStart/dateRegisteredEnd` на вашем endpoint не давала надёжной серверной выборки;
- поэтому в боевой логике используется окно `dateUpdatedStart/dateUpdatedEnd`;
- для новых регистраций этого достаточно, потому что у новых пользователей `updateTime` совпадает с моментом создания записи.

## Архитектура проекта

Основные файлы:

- `config.py` — загрузка `.env`, значения по умолчанию, валидация конфигурации.
- `db.py` — SQLite, ORM-модели `users` и `meta`, upsert, meta-ключи.
- `abcp_client.py` — HTTP-клиент ABCP, пагинация, rate-limit, повторы.
- `b24_client.py` — HTTP-клиент Bitrix24, поиск/создание/обновление контактов и сделок.
- `sync_service.py` — основной импорт и синхронизация в Bitrix24.
- `cli.py` — ручные команды запуска.
- `main.py` — daemon-режим с циклом по расписанию.
- `request_analytics.py` — отдельный журнал HTTP-запросов в формате JSON Lines.
- `dev_load_from_file.py` — загрузка тестовых данных из JSON-файла в SQLite.
- `scripts/run_service.sh` — обёртка для запуска сервиса на Linux.

## Хранилище и состояние

SQLite содержит:

- таблицу `users` — локальный снимок пользователей ABCP;
- таблицу `meta` — технические отметки.

Ключевые `meta`-ключи:

- `last_full_import_started_at`
- `last_full_import_at`
- `last_incremental_import_at`
- `last_incremental_window_end`

`last_incremental_window_end` — главный checkpoint для инкрементального импорта.

## Поведение импорта

### Полный импорт

`import_all()`:

- проходит по всем пользователям ABCP постранично;
- делает batch-upsert в SQLite;
- обновляет `last_full_import_at`;
- выставляет `last_incremental_window_end` на момент старта полного импорта.

### Инкрементальный импорт

`import_incremental()`:

- читает checkpoint из `meta`;
- строит окно `start/end` с overlap;
- при большом backlog режет окно на части через `ABCP_INCREMENTAL_MAX_WINDOW_MINUTES`;
- запрашивает ABCP по `dateUpdatedStart/dateUpdatedEnd`;
- сохраняет новый checkpoint в `last_incremental_window_end`.

### Первый запуск

Поддерживаются два режима:

- `ABCP_INITIAL_IMPORT_MODE=full` — на первом запуске выполняется `import-all`;
- `ABCP_INITIAL_IMPORT_MODE=incremental` — полный импорт пропускается, checkpoint выставляется назад на `ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES`, после чего сервис работает только инкрементально.

## Поведение синхронизации в Bitrix24

`sync_to_b24()`:

- берёт только `synced=false`;
- обрабатывает в первую очередь более свежие изменения;
- ищет или обновляет контакт;
- создаёт или обновляет сделку;
- привязывает сделку к контакту;
- сохраняет найденные ID в SQLite.

Особенности текущей логики:

- контакт создаётся по ABCP-логике через `organizationName`;
- `NAME` контакта берётся из `organizationName`, если оно пустое — используется fallback `Клиент №{userId}`;
- `LAST_NAME` и `SECOND_NAME` для ABCP-контактов принудительно очищаются;
- при наличии `b24_contact_id` или `b24_deal_id` сначала пробуется обновление по ним;
- если Bitrix24 вернул, что сущность не найдена, выполняется rebinding;
- для сделки дополнительно используется поиск по `UF_B24_DEAL_ABCP_USER_ID`, чтобы не плодить дубли при потере локальной БД;
- сумма `saldo` нормализуется в money-формат Bitrix24;
- даты ABCP нормализуются в ISO-8601 с учётом `ABCP_TIMEZONE` и `B24_OUT_TZ_ISO`.

## Установка и быстрый старт

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python cli.py init-db
python main.py
```

### Linux

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python cli.py init-db
python main.py
```

Если нужен разовый полный прогон вместо daemon-режима:

```bash
python cli.py init-db
python cli.py import-all
python cli.py sync-b24
```

## Команды CLI

Инициализация БД:

```bash
python cli.py init-db
```

Полный импорт из ABCP:

```bash
python cli.py import-all
```

Инкрементальный импорт:

```bash
python cli.py import-incremental
```

Совместимый алиас старой команды:

```bash
python cli.py import-today
```

Синхронизация в Bitrix24:

```bash
python cli.py sync-b24
```

Синхронизация ограниченной пачкой:

```bash
python cli.py sync-b24 --limit 100
```

Полный ручной цикл:

```bash
python cli.py run
```

Запуск с произвольным лог-файлом:

```bash
python cli.py --log-level INFO --log-file logs/manual_sync.log sync-b24
```

Локальная загрузка из JSON-файла:

```bash
python dev_load_from_file.py --path data/sample.json --log-level INFO
```

## Daemon-режим

Основной сервис:

```bash
python main.py
```

Linux-обёртка:

```bash
./scripts/run_service.sh
```

Что делает `main.py`:

- настраивает логирование;
- валидирует конфиг;
- инициализирует SQLite;
- на первом запуске делает либо полный импорт, либо incremental bootstrap;
- далее в цикле запускает:
  - `import_incremental`
  - `sync_to_b24`

Интервал берётся из `SYNC_INTERVAL_SECONDS`.

Ограничение:

- если задать значение меньше `300`, сервис всё равно поднимет его до `300` секунд.

## Переменные окружения

Источник истины — `.env.example`. Ниже перечислены все рабочие группы параметров.

### ABCP

```text
ABCP_BASE_URL
ABCP_USERLOGIN
ABCP_USERPSW
ABCP_LIMIT
ABCP_MAX_PAGES
ABCP_TIMEZONE
ABCP_INITIAL_IMPORT_MODE
ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES
ABCP_INCREMENTAL_LOOKBACK_MINUTES
ABCP_INCREMENTAL_OVERLAP_MINUTES
ABCP_INCREMENTAL_MAX_WINDOW_MINUTES
```

### Bitrix24

```text
B24_WEBHOOK_URL
B24_DEAL_TITLE_PREFIX
B24_OUT_TZ_ISO
B24_DEAL_CATEGORY_ID_USERS
B24_DEAL_STAGE_NEW_USERS
UF_B24_DEAL_ABCP_USER_ID
UF_B24_DEAL_INN
UF_B24_DEAL_SALDO
UF_B24_DEAL_REG_DATE
UF_B24_DEAL_UPDATE_TIME
```

### SQLite и HTTP

```text
SQLITE_PATH
REQUESTS_TIMEOUT
REQUESTS_RETRIES
REQUESTS_RETRY_BACKOFF
RATE_LIMIT_SLEEP
```

### HTTP-аналитика

```text
REQUEST_ANALYTICS_ENABLED
REQUEST_ANALYTICS_DIR
REQUEST_ANALYTICS_MAX_BODY_CHARS
```

### Сервис

```text
SYNC_INTERVAL_SECONDS
LOG_LEVEL
DOTENV_PATH
DOTENV_OVERRIDE
```

### Рекомендуемые значения для регулярной работы

```text
ABCP_INITIAL_IMPORT_MODE=incremental
ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES=1440
ABCP_INCREMENTAL_LOOKBACK_MINUTES=15
ABCP_INCREMENTAL_OVERLAP_MINUTES=5
ABCP_INCREMENTAL_MAX_WINDOW_MINUTES=1440
SYNC_INTERVAL_SECONDS=600
RATE_LIMIT_SLEEP=3
REQUEST_ANALYTICS_ENABLED=1
REQUEST_ANALYTICS_DIR=logs/http_requests
REQUEST_ANALYTICS_MAX_BODY_CHARS=20000
```

## Логи

### Логи daemon-режима

`main.py` пишет в:

```text
logs/service.log
```

Ротация:

- по полуночи;
- хранятся последние `14` файлов;
- формат архивных файлов:

```text
logs/service.log.YYYY-MM-DD
```

### Логи CLI-команд

По умолчанию:

```text
logs/sync_YYYY-MM-DD.log
```

### HTTP-аналитика запросов

Отдельный журнал исходящих HTTP-запросов включён по умолчанию.

Путь:

```text
logs/http_requests/request_analytics_YYYY-MM-DD.log
```

Формат — JSON Lines, одна строка на один HTTP-вызов.

Что попадает в запись:

- timestamp;
- provider;
- operation;
- HTTP method;
- URL;
- payload запроса;
- payload ответа;
- `success`;
- `outcome=positive|negative`;
- `http_status`;
- `attempt`;
- `duration_ms`;
- `error`.

Секреты маскируются:

- `userpsw`
- `token`
- `authorization`
- части webhook URL Bitrix24

Отключение:

```text
REQUEST_ANALYTICS_ENABLED=0
```

## Просмотр логов

### PowerShell

Следить за сервисом:

```powershell
Get-Content .\logs\service.log -Wait
```

Последние 100 строк сервиса:

```powershell
Get-Content .\logs\service.log -Tail 100
```

Следить за CLI-логом:

```powershell
Get-Content .\logs\sync_2026-03-27.log -Tail 100 -Wait
```

Следить за HTTP-аналитикой:

```powershell
Get-Content .\logs\http_requests\request_analytics_2026-03-27.log -Tail 100 -Wait
```

### Linux

```bash
tail -f logs/service.log
tail -f logs/sync_$(date +%F).log
tail -f logs/http_requests/request_analytics_$(date +%F).log
```

## Проверка состояния

Сколько записей в базе и есть ли очередь на синк:

```bash
python - << 'PY'
from config import SQLITE_PATH
from db import User, get_engine
from sqlalchemy.orm import Session

engine = get_engine(SQLITE_PATH)
with Session(engine) as session:
    total = session.query(User).count()
    unsynced = session.query(User).filter(User.synced == False).count()
    print("total =", total)
    print("unsynced =", unsynced)
PY
```

Проверить тесты:

```bash
python -m unittest discover -s tests -v
```

Проверить синтаксис:

```bash
python -m compileall .
```

## Типовые сценарии

### Новый сервер без полной исторической загрузки

```bash
python cli.py init-db
python main.py
```

При `ABCP_INITIAL_IMPORT_MODE=incremental` сервис сам выставит начальный checkpoint и стартует без `import-all`.

### Новый сервер с полной историей

```bash
python cli.py init-db
python cli.py import-all
python cli.py sync-b24
python main.py
```

### Ручной догон очереди

```bash
python cli.py import-incremental
python cli.py sync-b24
```

### Осторожная проверка на маленькой пачке

```bash
python cli.py sync-b24 --limit 10
```

## Восстановление после потери SQLite

Если локальная база потеряна:

1. Создайте пустую БД:

```bash
python cli.py init-db
```

2. Либо загрузите полный список:

```bash
python cli.py import-all
```

3. Либо сразу вернитесь в incremental-режим:

```bash
python main.py
```

4. Затем догоните Bitrix24:

```bash
python cli.py sync-b24
```

Почему это безопаснее обычного «слепого» пересоздания:

- контакт ищется по телефону и email;
- сделка дополнительно ищется по `UF_B24_DEAL_ABCP_USER_ID`;
- stale ID в локальной БД корректно rebinding-ятся.

## Типовые признаки проблем

### Слишком много запросов в ABCP

Проверьте:

- не выставлен ли слишком маленький `SYNC_INTERVAL_SECONDS`;
- не запущены ли параллельно несколько копий сервиса;
- не включён ли вручную частый внешний cron;
- не отключён ли rate-limit через изменения в коде или конфиге.

Нормальная схема для продакшена:

```text
ABCP_INITIAL_IMPORT_MODE=incremental
ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES=1440
ABCP_INCREMENTAL_LOOKBACK_MINUTES=15
ABCP_INCREMENTAL_OVERLAP_MINUTES=5
ABCP_INCREMENTAL_MAX_WINDOW_MINUTES=1440
SYNC_INTERVAL_SECONDS=600
RATE_LIMIT_SLEEP=3
```

### Несинхронизированные записи зависли в очереди

Смотрите:

- `logs/service.log`
- `logs/sync_YYYY-MM-DD.log`
- `logs/http_requests/request_analytics_YYYY-MM-DD.log`

Обычные причины:

- ошибка Bitrix24 по полям;
- временная сетeвая ошибка;
- stale `contact_id` или `deal_id`;
- запись недавно попала в overlap-окно и будет досинхронизирована следующим тиком.

## Безопасность

- `.env` не коммитится.
- В репозитории хранится только `.env.example`.
- SQLite-файлы и рабочие логи не должны попадать в git.
- В документации и логах нельзя публиковать реальные webhook/token/password.
- Если реальные секреты были показаны в чате, тикете или внешнем логе, их нужно перевыпустить.

## Эксплуатационная документация

Подробный runbook для продакшена находится в [docs/OPERATIONS.md](docs/OPERATIONS.md).
