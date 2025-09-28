# ABCP → Bitrix24 Users Sync (Python, SQLite)

Надёжный офлайн-скрипт для импорта пользователей из **ABCP** в локальную БД (SQLite) и их синхронизации с **Bitrix24**:

- создаётся/обновляется **Контакт** (быстрый путь);
- создаётся **Сделка** в воронке «Пользователи» с заполнением UF-полей.

---

## Возможности

- **Полная первичная загрузка** всех пользователей ABCP (постранично).
- **Инкрементальная загрузка** «зарегистрированы сегодня» (клиентская фильтрация по `registrationDate`).
- Локальная БД **SQLite** с флагами синхронизации и датами (`synced`, `synced_at`, `b24_contact_id`, `b24_deal_id`).
- **Быстрая синхронизация в Bitrix24**: контакт создаётся без поиска (`add_contact_quick`) и затем создаётся сделка в воронке «Пользователи».
- **Повторы/таймауты**, корректная обработка ошибок, подробное логирование.
- **CLI-команды**: `init-db`, `import-all`, `import-today`, `sync-b24`, `run`.
- **Автолог**: по умолчанию логи пишутся в файл за текущую дату `logs/sync_YYYY-MM-DD.log` + в консоль.
- **Идемпотентная инициализация БД**: схема автоматически создаётся перед операциями.

---

## Требования

- Python 3.10+
- Зависимости (см. `requirements.txt`):

```text
python-dotenv==1.0.1
requests==2.32.3
SQLAlchemy==2.0.35
```

---

## Быстрый старт

1. Скопируйте `/.env.example` → `/.env` и заполните переменные (особенно воронку и UF-поля).

2. Установите зависимости и активируйте окружение.

   **Linux/macOS:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

   **Windows PowerShell:**

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

3. Инициализация БД:

   ```bash
   python cli.py init-db
   ```

4. Полный импорт и синхронизация:

   ```bash
   python cli.py run
   # или отдельно
   python cli.py import-all
   python cli.py sync-b24
   ```

5. Ежедневный инкремент:

   ```bash
   python cli.py import-today
   python cli.py sync-b24
   ```

> По умолчанию логи пишутся в `logs/sync_YYYY-MM-DD.log` и в консоль. Можно переопределить `--log-level` и `--log-file`.

---

## CLI

```text
init-db                — создать/инициализировать SQLite
import-all             — полный импорт из ABCP
import-today           — импорт только «сегодняшних»
sync-b24 [--limit N]   — синхронизация в Bitrix24 (опциональный лимит)
run                    — import-all + sync-b24 (полный цикл)

Глобальные флаги:
  --log-level (DEBUG/INFO/...), --log-file (путь к файлу), -v/--verbose
```

Примеры:

```bash
python cli.py import-all --log-level INFO
python cli.py sync-b24 --limit 100 --log-level DEBUG
python cli.py run --log-file logs/custom.log
```

---

## Переменные окружения (`.env`)

### ABCP

```text
ABCP_BASE_URL     — базовый URL (без параметров), напр. https://abcpXXXX.public.api.abcp.ru/cp/users
ABCP_USERLOGIN    — логин API
ABCP_USERPSW      — пароль/ключ API
ABCP_LIMIT        — размер страницы (по умолчанию 500)
ABCP_MAX_PAGES    — максимум страниц (целое; пусто — без лимита)
```

### Bitrix24

```text
B24_WEBHOOK_URL           — URL вебхука Bitrix24 вида https://{domain}.bitrix24.ru/rest/{user_id}/{token}/
B24_DEAL_TITLE_PREFIX     — префикс названия сделки (по умолчанию "ABCP Регистрация:")
```

### Воронка «Пользователи»

```text
B24_DEAL_CATEGORY_ID_USERS — числовой CATEGORY_ID воронки (обязательно)
B24_DEAL_STAGE_NEW_USERS   — код стартовой стадии (обязательно), формат C{CATEGORY_ID}:NEW
# пример: если CATEGORY_ID=5 → B24_DEAL_STAGE_NEW_USERS=C5:NEW
```

### UF-поля сделки

```text
UF_B24_DEAL_ABCP_USER_ID  — код UF для «ID клиента ABCP» (например, UF_CRM_1738181468)
UF_B24_DEAL_INN           — код UF для «ИНН» (например, UF_CRM_1713393074421)
UF_B24_DEAL_SALDO         — код UF для «Баланс ABCP» (например, UF_CRM_1738182431)
```

### Хранилище

```text
SQLITE_PATH               — путь к БД SQLite (по умолчанию data/abcp_b24.sqlite3)
```

### HTTP

```text
REQUESTS_TIMEOUT          — таймаут запроса в секундах (по умолчанию 20)
REQUESTS_RETRIES          — количество повторов при ошибках (по умолчанию 3)
REQUESTS_RETRY_BACKOFF    — базовая задержка между повторами (сек; по умолчанию 1.5)
RATE_LIMIT_SLEEP          — пауза между запросами (сек; по умолчанию 0.2)
```

---

## Модель данных

### Таблица `users`

```text
id (PK, autoincrement)
abcp_user_id (уникально, индекс)
name, second_name, surname, email (индекс), mobile (индекс), phone, city, state
registration_date, update_time (строки)
raw_json (оригинальный JSON пользователя)
synced (bool, индекс), synced_at (datetime), b24_contact_id, b24_deal_id
created_at, updated_at
```

### Таблица `meta`

```text
key (PK), value
# используется для: last_full_import_at, last_incremental_import_at
```

---

## Поведение синхронизации

- **Контакт** создаётся быстро через вебхук (`crm.contact.add`) без предварительного поиска. Если у записи в БД уже есть `b24_contact_id`, он переиспользуется.
- **Сделка** создаётся в воронке «Пользователи» со следующими полями:
  - `TITLE` = `organizationName` (либо `{B24_DEAL_TITLE_PREFIX} {userId}`);
  - `CATEGORY_ID` = `B24_DEAL_CATEGORY_ID_USERS`;
  - `STAGE_ID` = `B24_DEAL_STAGE_NEW_USERS`;
  - `CONTACT_ID` = ID созданного/существующего контакта.
- **UF-поля сделки**:
  - `UF_CRM_… (ABCP_USER_ID)` ← `userId`;
  - `UF_CRM_… (ИНН)` ← `inn`;
  - `UF_CRM_… (Баланс ABCP)` ← `saldo` (строка вида `-1 582,00` парсится в `float`; если парсинг не удался, передаётся исходная строка).

После успешного создания сделки запись помечается `synced=True`, сохраняются `b24_deal_id` и `synced_at`.

> Примечание: при отсутствии телефона/почты в новых источниках возможны дубляжи контактов. После первого создания мы сохраняем `b24_contact_id` и далее его переиспользуем.

---

## Логирование

- По умолчанию логи пишутся в **консоль** и в файл за текущую дату:

  ```text
  logs/sync_YYYY-MM-DD.log
  ```

- Уровень логирования задаётся `--log-level` (по умолчанию `DEBUG`) или флагом `-v/--verbose`.
- Можно указать собственный файл логов: `--log-file logs/custom.log`.

---

## Проверка и диагностика

### Как узнать путь к БД

```bash
python - << 'PY'
from config import SQLITE_PATH
print("SQLITE_PATH =", SQLITE_PATH)
PY
```

### Как убедиться, что таблицы созданы

```bash
python - << 'PY'
from sqlalchemy import create_engine, inspect
from config import SQLITE_PATH
eng = create_engine(f"sqlite:///{SQLITE_PATH}")
print(inspect(eng).get_table_names())
PY
```

Ожидается минимум: `['meta', 'users']`.

> В коде `sync_service` перед операциями вызывается `init_db()` — схема будет создана автоматически, но явный `init-db` полезен для первичного развёртывания.

---

## Планировщики

### Linux (cron)

```cron
# ежедневный инкремент + синхронизация (02:10), с логом по умолчанию
10 2 * * * /path/to/.venv/bin/python /path/to/cli.py import-today && /path/to/.venv/bin/python /path/to/cli.py sync-b24
```

### Windows (Task Scheduler)

```text
Program/script:   C:\Path\to\python.exe
Add arguments:    C:\Path\to\project\cli.py import-today
Start in:         C:\Path\to\project
# создайте второе действие для sync-b24
```

---

## Безопасность

- Храните реальные ключи в `.env` (файл не коммитится; в репозитории только `.env.example`).
- Логи могут содержать технические идентификаторы; пароли/токены не логируются.
- SQLite-файл храните на защищённом диске/разделе.

---

## Частые вопросы (FAQ)

### Ошибка `no such table: users`

```text
Выполните: python cli.py init-db
(в актуальной версии init_db() вызывается автоматически, но первый запуск лучше делать явно)
```

### Нужно ли заполнять CATEGORY_ID и STAGE_ID?

```text
Да. Это обязательные параметры для воронки «Пользователи». Пример: CATEGORY_ID=5 → C5:NEW
```

### Как формируется баланс?

```text
Поле saldo, например "-1 582,00", парсится в -1582.00 (float). Если парсинг не удался — отправляется исходной строкой.
```
