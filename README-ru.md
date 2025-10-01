# Telegram Pair Bot — Weekly Pairs + Owner Broadcast

Функции:
- Еженедельный опрос по понедельникам 15:00 (МСК): “Кто готов участвовать?”
- В пары попадают только отметившие “Готов” на этой неделе.
- Антиповтор на 5 последних раундов.
- Команда админа чата: `/poll_now` — сразу запустить опрос в этом чате.
- Владелец бота (OWNER_ID) может хранить объявления и отправлять их **во все чаты**: `/ad_add`, `/ad_list`, `/ad_delete`, `/ad_send`.
- Приветствие, когда бота добавили в группу.

## Быстрый старт (Replit)
1) Импортируйте файлы `main.py` и `requirements.txt`.
2) В **Secrets** добавьте `BOT_TOKEN` и `OWNER_ID` (узнать свой id можно командой `/whoami`).
3) В Shell: `pip install -r requirements.txt`
4) Нажмите **Run**. В логах будет `Bot is running…`.

## 24/7 автономный режим (Railway / Fly.io / Render)
### Общее
- Команда запуска: `python main.py`
- Переменные окружения: `BOT_TOKEN`, `OWNER_ID`, по желанию `DB_PATH`, `ROUNDS_TO_AVOID`.
- Хранение данных: используем SQLite. Чтобы база сохранялась между перезапусками — нужен **персистентный диск/volume**:
  - **Railway**: добавить Volume (например, `/data`), выставить `DB_PATH=/data/pairbot.sqlite3`.
  - **Fly.io**: создать volume, примонтировать на `/data`, задать `DB_PATH=/data/pairbot.sqlite3`.
  - **Render**: добавить Persistent Disk (путь вроде `/var/data`), задать `DB_PATH=/var/data/pairbot.sqlite3`. (Background Worker у Render на бесплатном плане недоступен.)
- Часовой пояс для расписания — `Europe/Moscow` (через `tzdata` + `ZoneInfo`).

### Railway (кратко)
1) Залейте код в GitHub.
2) На Railway — New Project → Deploy from GitHub.
3) Add Variables: `BOT_TOKEN`, `OWNER_ID`, `DB_PATH=/data/pairbot.sqlite3`.
4) Add Volume: Mount path `/data`.
5) Start Command: `python main.py`.
6) Deploy → Logs: должно быть `Bot is running…`.

### Fly.io (кратко)
1) Поставьте локально `flyctl` (или используйте Codespaces).
2) `fly launch` → Python → настроить команду запуска.
3) `fly volumes create data --size 1` → монтировать на `/data`.
4) Задать `DB_PATH=/data/pairbot.sqlite3` и переменные.
5) `fly deploy`.

## Команды (сводка)
- Пользователям: `/join`, `/leave`, `/status`, `/help`.
- Админам чата: `/pair`, `/poll_now`.
- Владелец бота: `/whoami`, `/ad_add <текст>`, `/ad_list`, `/ad_delete <id>`, `/ad_send [id]`.
