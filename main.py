import asyncio
import os
import random
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

load_dotenv()

WELCOME_TEXT = (
    "Здравствуй, дорогой друг! Бот создан для того, чтобы нести людям тепло, заботу, преодолевать одиночество и помогать в трудных ситуациях."
    "\n\nКоманды:\n"
    "/join — участвовать в подборе пар\n"
    "/leave — выйти из подбора\n"
    "/status — список участников\n"
    "/pair — собрать пары (только для админов чата)\n"
    "/help — помощь\n\n"
    "Политика: храним только минимальные технические данные, необходимые для работы. Для удаления своих данных — команда /delete_me."
)

DB_PATH = os.getenv("DB_PATH", "pairbot.sqlite3")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ROUNDS_TO_AVOID = int(os.getenv("ROUNDS_TO_AVOID", "5"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "2000"))

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не найден. Укажи его в .env")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

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


def get_joined_user_ids(chat_id: int) -> list[int]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute(
            "SELECT user_id FROM chat_members WHERE chat_id=? AND joined=1",
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
        # получим id последних k раундов
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
            if c:  # для трио — все попарные связи
                edges.add(frozenset({a, c}))
                edges.add(frozenset({b, c}))
        return edges


# ------------- pairing logic -------------

def make_pairs(user_ids: list[int], recent_edges: set[frozenset[int]], max_attempts: int = 2000):
    if len(user_ids) < 2:
        return []

    best_solution = None
    best_conflicts = 10**9
    ids = user_ids[:]

    for _ in range(max_attempts):
        random.shuffle(ids)
        triad = None
        pool = ids[:]

        if len(pool) % 2 == 1:
            # сделаем одно трио — возьмём 3 последних
            triad = tuple(pool[-3:])
            pool = pool[:-3]

        pairs = []
        ok = True
        # соберём пары попарно
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
            result = pairs + ([triad] if triad else [])
            return result  # без конфликтов

        # иначе — посчитаем конфликты и сохраним лучшую попытку
        conflicts = 0
        tmp_pairs = []
        pool2 = ids[:]
        tri2 = None
        if len(pool2) % 2 == 1:
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

    return best_solution  # может содержать минимально возможные повторы


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
        # В aiogram 3 статусы — Enum, но на всякий случай сравниваем по строке
        status_val = str(status).lower()
        return any(s in status_val for s in ("creator", "owner", "administrator"))
    except TelegramBadRequest:
        return False


def mention(user_id: int, username: str | None, first_name: str | None) -> str:
    if username:
        return f"@{username}"
    name = first_name or "участник"
    return f"<a href=\"tg://user?id={user_id}\">{name}</a>"


# ------------- handlers -------------

@dp.message(CommandStart())
async def cmd_start(message: Message):
    upsert_member(message.from_user)
    await message.answer(
        "Привет! Я бот для случайного составления пар в группах. Добавь меня в групповой чат и используй /join, /pair.\n\n" + WELCOME_TEXT,
        disable_web_page_preview=True,
    )


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
    await message.answer("Готово! Ты участвуешь в подборе пар. ✨")


@dp.message(Command("leave"))
async def cmd_leave(message: Message):
    if not await ensure_group(message):
        return
    set_join(message.chat.id, message.from_user.id, False)
    await message.answer("Ок, исключаю из подбора пар. Возвращайся через /join!")


@dp.message(Command("status"))
async def cmd_status(message: Message):
    if not await ensure_group(message):
        return
    user_ids = get_joined_user_ids(message.chat.id)
    if not user_ids:
        await message.answer("Сейчас в пуле никого нет. Используйте /join в этом чате.")
        return
    # загрузим имена
    mentions = []
    with closing(sqlite3.connect(DB_PATH)) as conn:
        placeholders = ",".join(["?"] * len(user_ids))
        cur = conn.execute(
            f"SELECT m.user_id, m.username, m.first_name FROM members m WHERE m.user_id IN ({placeholders})",
            tuple(user_ids),
        )
        for uid, uname, fname in cur.fetchall():
            mentions.append(mention(uid, uname, fname))
    await message.answer(f"В пуле ({len(user_ids)}):\n" + ", ".join(mentions), parse_mode="HTML")


@dp.message(Command("pair"))
async def cmd_pair(message: Message):
    if not await ensure_group(message):
        return
    if not await is_admin(message.chat.id, message.from_user.id):
        await message.answer("Только администратор чата может запускать подбор пар.")
        return

    user_ids = get_joined_user_ids(message.chat.id)
    if len(user_ids) < 2:
        await message.answer("Недостаточно участников для пар. Нужно хотя бы 2, лучше 4+.")
        return

    recent = get_recent_pair_edges(message.chat.id, ROUNDS_TO_AVOID)
    solution = make_pairs(user_ids, recent)
    if not solution:
        await message.answer("Не удалось собрать пары. Попробуй ещё раз /pair.")
        return

    # сохраним раунд
    round_id = record_round(message.chat.id, solution)

    # подгрузим имена для упоминаний
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

    # Сообщение с парами/трио (свободный стиль)
    lines = ["💫 Пары дня готовы! (Раунд #{}).".format(round_id)]
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

    lines.append("\nПожелание от бота: будьте бережны друг к другу. Делитесь теплом и поддержкой. 🫶")
    await message.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


async def main():
    init_db()
    print("Bot is running…")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Stopped")
