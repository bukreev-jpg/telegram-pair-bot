import asyncio
import os
import random
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, PollAnswer
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

WELCOME_TEXT = (
    "Здравствуй, дорогой друг! Бот создан для того, чтобы нести людям тепло, заботу, преодолевать одиночество и помогать в трудных ситуациях."
    "\n\nКоманды:\n"
    "/join — участвовать в подборе пар (можно и без этого: по понедельникам приходит опрос)\n"
    "/leave — выйти из подбора (на эту неделю)\n"
    "/status — список участников, готовых на этой неделе\n"
    "/pair — собрать пары (для админов чата)\n"
    "/poll_now — запустить опрос сейчас (для админов чата)\n"
    "/help — помощь\n\n"
    "Каждый понедельник в 15:00 (МСК) бот пришлёт опрос ‘Кто готов участвовать на этой неделе?’. Отметь ‘Готов’, чтобы попасть в рандом на этой неделе.\n"
    "Приватность: храним только минимальные технические данные. /delete_me — удалить свои данные."
)

DB_PATH = os.getenv("DB_PATH", "pairbot.sqlite3")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ROUNDS_TO_AVOID = int(os.getenv("ROUNDS_TO_AVOID", "5"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "2000"))
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)  # Telegram user_id владельца бота

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не найден. Укажи его в .env/Secrets")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=ZoneInfo("Europe/Moscow"))

# ---------------- DB -----------------

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS members (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_members (
                chat_id INTEGER,
                user_id INTEGER,
                joined INTEGER DEFAULT 0,
                weekly_ready INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, user_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                created_at TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pairs (
                round_id INTEGER,
                a INTEGER,
                b INTEGER,
                c INTEGER
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS polls (
                poll_id TEXT PRIMARY KEY,
                chat_id INTEGER,
                created_at TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT,
                created_at TEXT
            );
            """
        )
        # На случай обновления старой БД — добавим недостающую колонку
        try:
            conn.execute("ALTER TABLE chat_members ADD COLUMN weekly_ready INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.commit()


def upsert_chat(chat_id: int, title: str | None):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO chats(chat_id, title) VALUES(?, ?)",
            (chat_id, title or ""),
        )
        conn.commit()


def upsert_member(user):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO members(user_id, username, first_name) VALUES(?,?,?)",
            (user.id, user.username or "", user.first_name or ""),
        )
        conn.commit()


def set_join(chat_id: int, user_id: int, joined: bool):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_members(chat_id, user_id, joined) VALUES(?,?,0)",
            (chat_id, user_id),
        )
        conn.execute(
            "UPDATE chat_members SET joined=? WHERE chat_id=? AND user_id=?",
            (1 if joined else 0, chat_id, user_id),
        )
        conn.commit()


def set_ready(chat_id: int, user_id: int, ready: bool):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_members(chat_id, user_id, joined, weekly_ready) VALUES(?,?,0,0)",
            (chat_id, user_id),
        )
        conn.execute(
            "UPDATE chat_members SET weekly_ready=? WHERE chat_id=? AND user_id=?",
            (1 if ready else 0, chat_id, user_id),
        )
        conn.commit()


def reset_weekly_ready(chat_id: int | None = None):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        if chat_id is None:
            conn.execute("UPDATE chat_members SET weekly_ready=0")
        else:
            conn.execute("UPDATE chat_members SET weekly_ready=0 WHERE chat_id=?", (chat_id,))
        conn.commit()


def get_ready_user_ids(chat_id: int) -> list[int]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute(
            "SELECT user_id FROM chat_members WHERE chat_id=? AND weekly_ready=1",
            (chat_id,),
        )
        return [row[0] for row in cur.fetchall()]


def record_round(chat_id: int, pairs_list: list[tuple[int, ...]]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute(
            "INSERT INTO rounds(chat_id, created_at) VALUES(?, ?)",
            (chat_id, now),
        )
        round_id = cur.lastrowid
        for tup in pairs_list:
            a = tup[0]
            b = tup[1] if len(tup) > 1 else None
            c = tup[2] if len(tup) > 2 else None
            conn.execute(
                "INSERT INTO pairs(round_id, a, b, c) VALUES(?,?,?,?)",
                (round_id, a, b, c),
            )
        conn.commit()
        return round_id


def get_recent_pair_edges(chat_id: int, k: int) -> set[frozenset[int]]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur2 = conn.execute(
            "SELECT id FROM rounds WHERE chat_id=? ORDER BY id DESC LIMIT ?",
            (chat_id, k),
        )
        last_round_ids = [row[0] for row in cur2.fetchall()]
        if not last_round_ids:
            return set()
        placeholders = ",".join(["?"] * len(last_round_ids))
        cur3 = conn.execute(
            f"SELECT a,b,c FROM pairs WHERE round_id IN ({placeholders})",
            tuple(last_round_ids),
        )
        edges: set[frozenset[int]] = set()
        for a, b, c in cur3.fetchall():
            if a and b:
                edges.add(frozenset({a, b}))
            if c:
                edges.add(frozenset({a, c}))
                edges.add(frozenset({b, c}))
        return edges


# ------------- pairing logic -------------

def make_pairs(user_ids: list[int], recent_edges: set[frozenset[int]], max_attempts: int = MAX_ATTEMPTS):
    if len(user_ids) < 2:
        return []

    best_solution = None
    best_conflicts = 10**9
    ids = user_ids[:]

    for _ in range(max_attempts):
        random.shuffle(ids)
        triad = None
        pool = ids[:]

        if len(pool) % 2 == 1 and len(pool) >= 3:
            triad = tuple(pool[-3:])
            pool = pool[:-3]

        pairs = []
        ok = True
        for i in range(0, len(pool), 2):
            a, b = pool[i], pool[i + 1]
            if frozenset({a, b}) in recent_edges:
                ok = False
                break
            pairs.append((a, b))

        if ok and triad:
            a, b, c = triad
            tri_ok = (
                frozenset({a, b}) not in recent_edges
                and frozenset({a, c}) not in recent_edges
                and frozenset({b, c}) not in recent_edges
            )
            if not tri_ok:
                ok = False

        if ok:
            return pairs + ([triad] if triad else [])

        conflicts = 0
        tmp_pairs = []
        pool2 = ids[:]
        tri2 = None
        if len(pool2) % 2 == 1 and len(pool2) >= 3:
            tri2 = tuple(pool2[-3:])
            pool2 = pool2[:-3]
        for i in range(0, len(pool2), 2):
            a, b = pool2[i], pool2[i + 1]
            if frozenset({a, b}) in recent_edges:
                conflicts += 1
            tmp_pairs.append((a, b))
        if tri2:
            a, b, c = tri2
            conflicts += int(frozenset({a, b}) in recent_edges)
            conflicts += int(frozenset({a, c}) in recent_edges)
            conflicts += int(frozenset({b, c}) in recent_edges)
        if conflicts < best_conflicts:
            best_conflicts = conflicts
            best_solution = tmp_pairs + ([tri2] if tri2 else [])
            if best_conflicts == 0:
                return best_solution

    return best_solution


# ------------- helpers -------------

async def ensure_group(message: Message) -> bool:
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Эта команда работает только в групповом чате.")
        return False
    return True


async def is_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        status = getattr(member, "status", None)
        if not status:
            return False
        status_val = str(status).lower()
        return any(s in status_val for s in ("creator", "owner", "administrator"))
    except TelegramBadRequest:
        return False


def is_owner(user_id: int) -> bool:
    return OWNER_ID and user_id == OWNER_ID


def mention(user_id: int, username: str | None, first_name: str | None) -> str:
    if username:
        return f"@{username}"
    name = first_name or "участник"
    return f"<a href=\"tg://user?id={user_id}\">{name}</a>"


# ------------- weekly poll helpers -------------

async def send_weekly_poll_to_chat(chat_id: int) -> bool:
    """Отправляем опрос в указанный чат и сохраняем poll_id."""
    question = "Кто готов участвовать на этой неделе?"
    options = ["Готов", "Не готов"]
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        msg = await bot.send_poll(
            chat_id=chat_id,
            question=question,
            options=options,
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        poll_id = msg.poll.id if msg and msg.poll else None
        if poll_id:
            with closing(sqlite3.connect(DB_PATH)) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO polls(poll_id, chat_id, created_at) VALUES(?,?,?)",
                    (poll_id, chat_id, now_iso),
                )
                conn.commit()
        return True
    except Exception:
        return False


async def weekly_poll_job():
    """Еженедельный опрос по всем чатам: понедельник 15:00 (МСК)."""
    # Сбросим готовность на новую неделю по всем чатам
    reset_weekly_ready(None)
    # Список чатов
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT chat_id FROM chats")
        chat_ids = [row[0] for row in cur.fetchall()]
    # Рассылка опроса по каждому чату
    for chat_id in chat_ids:
        await send_weekly_poll_to_chat(chat_id)


# ------------- handlers -------------

@dp.my_chat_member()
async def on_my_chat_member(event):
    """Приветствуем, когда бота добавили в группу, и запоминаем чат."""
    try:
        new_status = str(getattr(event.new_chat_member, "status", "")).lower()
    except Exception:
        new_status = ""
    if new_status in ("member", "administrator"):
        chat_id = event.chat.id
        upsert_chat(chat_id, getattr(event.chat, "title", ""))
        welcome = (
            "Здравствуй, дорогой друг! Я буду помогать знакомиться и поддерживать друг друга.\n\n"
            "Каждый понедельник в 15:00 (МСК) я пришлю опрос: ‘Готов/Не готов’. Отметь ‘Готов’, чтобы участвовать на этой неделе.\n"
            "Команды: /status, /pair (для админов), /poll_now (сразу опрос), /leave, /help."
        )
        try:
            await bot.send_message(chat_id, welcome)
        except Exception:
            pass


@dp.message(Command("poll_now"))
async def cmd_poll_now(message: Message):
    """Запускает опрос немедленно в текущем чате (для админов)."""
    if not await ensure_group(message):
        return
    if not await is_admin(message.chat.id, message.from_user.id):
        await message.answer("Только администратор чата может запускать опрос.")
        return
    # Сбрасываем отметки готовности только для этого чата и запускаем опрос
    reset_weekly_ready(message.chat.id)
    ok = await send_weekly_poll_to_chat(message.chat.id)
    if ok:
        await message.answer("Опрос отправлен в этот чат.")
    else:
        await message.answer("Не удалось отправить опрос. Проверьте права бота.")


@dp.poll_answer()
async def on_poll_answer(poll_answer: PollAnswer):
    """Фиксируем готовность на неделю по ответу опроса."""
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    option_ids = poll_answer.option_ids or []
    ready = len(option_ids) > 0 and option_ids[0] == 0  # 0 = "Готов"

    # найдём чат по poll_id
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT chat_id FROM polls WHERE poll_id=?", (poll_id,))
        row = cur.fetchone()
        if not row:
            return
        chat_id = row[0]

    upsert_member(user)
    if ready:
        set_join(chat_id, user.id, True)  # если человек впервые, считаем его участником
    set_ready(chat_id, user.id, ready)


@dp.message(CommandStart())
async def cmd_start(message: Message):
    upsert_member(message.from_user)
    await message.answer(
        "Привет! Я бот для случайного составления пар в группах. Добавь меня в групповой чат и используй /status, /pair.\n\n" + WELCOME_TEXT,
        disable_web_page_preview=True,
    )


@dp.message(Command("whoami"))
async def cmd_whoami(message: Message):
    yours = message.from_user.id
    info = [f"Твой user_id: {yours}"]
    if OWNER_ID:
        info.append("OWNER_ID настроен" if OWNER_ID == yours else f"OWNER_ID в боте = {OWNER_ID}")
    else:
        info.append("OWNER_ID не задан. Добавь в Secrets переменную OWNER_ID, чтобы рассылать объявления во все чаты.")
    await message.answer("\n".join(info))


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(WELCOME_TEXT, disable_web_page_preview=True)


@dp.message(Command("delete_me"))
async def cmd_delete_me(message: Message):
    user_id = message.from_user.id
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM pairs WHERE round_id IN (SELECT id FROM rounds WHERE chat_id IN (SELECT chat_id FROM chat_members WHERE user_id=?))", (user_id,))
        conn.execute("DELETE FROM rounds WHERE chat_id IN (SELECT chat_id FROM chat_members WHERE user_id=?)", (user_id,))
        conn.execute("DELETE FROM chat_members WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM members WHERE user_id=?", (user_id,))
        conn.commit()
    await message.answer("Твои данные удалены из бота.")


@dp.message(Command("join"))
async def cmd_join(message: Message):
    if not await ensure_group(message):
        return
    upsert_chat(message.chat.id, message.chat.title)
    upsert_member(message.from_user)
    set_join(message.chat.id, message.from_user.id, True)
    set_ready(message.chat.id, message.from_user.id, True)  # сразу считаем готовым до ближайшего сброса
    await message.answer("Готово! Ты участвуешь в подборе пар. ✨")


@dp.message(Command("leave"))
async def cmd_leave(message: Message):
    if not await ensure_group(message):
        return
    set_ready(message.chat.id, message.from_user.id, False)
    await message.answer("Ок, исключаю из подбора пар на эту неделю. Возвращайся через опрос или /join!")


@dp.message(Command("status"))
async def cmd_status(message: Message):
    if not await ensure_group(message):
        return
    user_ids = get_ready_user_ids(message.chat.id)
    if not user_ids:
        await message.answer("На этой неделе пока никто не отметил ‘Готов’. Дождитесь опроса по понедельникам в 15:00 (МСК) или используйте /join.")
        return
    mentions = []
    with closing(sqlite3.connect(DB_PATH)) as conn:
        placeholders = ",".join(["?"] * len(user_ids))
        cur = conn.execute(
            f"SELECT m.user_id, m.username, m.first_name FROM members m WHERE m.user_id IN ({placeholders})",
            tuple(user_ids),
        )
        for uid, uname, fname in cur.fetchall():
            mentions.append(mention(uid, uname, fname))
    await message.answer(f"Готовы на этой неделе ({len(user_ids)}):\n" + ", ".join(mentions), parse_mode="HTML")


@dp.message(Command("pair"))
async def cmd_pair(message: Message):
    if not await ensure_group(message):
        return
    if not await is_admin(message.chat.id, message.from_user.id):
        await message.answer("Только администратор чата может запускать подбор пар.")
        return

    user_ids = get_ready_user_ids(message.chat.id)
    if len(user_ids) < 2:
        await message.answer("Недостаточно участников ‘Готов’ на этой неделе. Нужно хотя бы 2.")
        return

    recent = get_recent_pair_edges(message.chat.id, ROUNDS_TO_AVOID)
    solution = make_pairs(user_ids, recent)
    if not solution:
        await message.answer("Не удалось собрать пары. Попробуй ещё раз /pair.")
        return

    round_id = record_round(message.chat.id, solution)

    id_set = {uid for tup in solution for uid in tup}
    users_map = {}
    with closing(sqlite3.connect(DB_PATH)) as conn:
        placeholders = ",".join(["?"] * len(id_set))
        cur = conn.execute(
            f"SELECT user_id, username, first_name FROM members WHERE user_id IN ({placeholders})",
            tuple(id_set),
        )
        for uid, uname, fname in cur.fetchall():
            users_map[uid] = (uname, fname)

    lines = ["💫 Пары недели готовы! (Раунд #{}).".format(round_id)]
    for tpl in solution:
        if len(tpl) == 2:
            a, b = tpl
            a_m = mention(a, *users_map.get(a, (None, None)))
            b_m = mention(b, *users_map.get(b, (None, None)))
            lines.append(f"— {a_m} 🤝 {b_m}")
        else:
            a, b, c = tpl
            a_m = mention(a, *users_map.get(a, (None, None)))
            b_m = mention(b, *users_map.get(b, (None, None)))
            c_m = mention(c, *users_map.get(c, (None, None)))
            lines.append(f"— {a_m} 🤝 {b_m} 🤝 {c_m} (трио)")

    lines.append("\nПусть эта неделя будет тёплой и поддерживающей. 🫶")
    await message.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


# -------- Объявления: ТОЛЬКО владелец бота (OWNER_ID) --------

@dp.message(Command("ad_add"))
async def cmd_ad_add(message: Message):
    if not is_owner(message.from_user.id):
        return
    parts = message.text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Использование: /ad_add текст объявления (доступно только владельцу)")
        return
    text = parts[1].strip()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute(
            "INSERT INTO ads(text, created_at) VALUES(?,?)",
            (text, datetime.now(timezone.utc).isoformat()),
        )
        ad_id = cur.lastrowid
        conn.commit()
    await message.answer(f"Добавлено объявление #{ad_id} (глобально).")


@dp.message(Command("ad_list"))
async def cmd_ad_list(message: Message):
    if not is_owner(message.from_user.id):
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT id, text, created_at FROM ads ORDER BY id")
        rows = cur.fetchall()
    if not rows:
        await message.answer("Объявлений пока нет. Добавь через /ad_add <текст>.")
        return
    lines = ["Сохранённые объявления (глобально):"]
    for i, t, ts in rows:
        t_short = (t[:160] + "…") if len(t) > 160 else t
        lines.append(f"#{i} — {t_short}")
    await message.answer("\n".join(lines))


@dp.message(Command("ad_delete"))
async def cmd_ad_delete(message: Message):
    if not is_owner(message.from_user.id):
        return
    parts = message.text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer("Использование: /ad_delete <id>")
        return
    ad_id = int(parts[1].strip())
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM ads WHERE id=?", (ad_id,))
        conn.commit()
    await message.answer(f"Удалено (если существовало) объявление #{ad_id}.")


@dp.message(Command("ad_send"))
async def cmd_ad_send(message: Message):
    if not is_owner(message.from_user.id):
        return
    # /ad_send [id]
    parts = message.text.split(" ", 1)
    ad_id = None
    if len(parts) > 1 and parts[1].strip().isdigit():
        ad_id = int(parts[1].strip())

    with closing(sqlite3.connect(DB_PATH)) as conn:
        if ad_id is not None:
            cur = conn.execute("SELECT text FROM ads WHERE id=?", (ad_id,))
            row = cur.fetchone()
            if not row:
                await message.answer("Нет объявления с таким id.")
                return
            (text,) = row
        else:
            cur = conn.execute("SELECT text FROM ads ORDER BY RANDOM() LIMIT 1")
            row = conn.fetchone()
            if not row:
                await message.answer("Объявлений пока нет. Добавь через /ad_add <текст>.")
                return
            (text,) = row

        cur = conn.execute("SELECT chat_id FROM chats")
        chat_ids = [r[0] for r in cur.fetchall()]

    ok, fail = 0, 0
    for chat_id in chat_ids:
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=False)
            ok += 1
        except Exception:
            fail += 1
    await message.answer(f"Объявление отправлено. Успех: {ok}, ошибок: {fail}.")


async def main():
    init_db()
    # Планировщик: опрос каждый понедельник в 15:00 (МСК)
    scheduler.add_job(weekly_poll_job, "cron", day_of_week="mon", hour=15, minute=0)
    scheduler.start()
    print("Bot is running…")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Stopped")
