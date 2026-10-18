import asyncio
import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import asyncpg
from aiohttp import web
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

# =====================================================================
# 1. LOAD .ENV & CONFIGURATION
# =====================================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("Nudgelet")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
RAW_ADMINS = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {int(x.strip()) for x in RAW_ADMINS.split(",") if x.strip().isdigit()}

PORT = int(os.getenv("PORT", "8080"))
DEFAULT_TZ_OFFSET = int(os.getenv("DEFAULT_TZ_OFFSET", "5"))

# =====================================================================
# 2. SUPABASE POSTGRESQL ENGINE (WRITE-THROUGH + ASYNCPG POOL)
# =====================================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS nudgelet_users (
    id BIGINT PRIMARY KEY,
    username TEXT DEFAULT '',
    full_name TEXT DEFAULT 'User',
    tz_offset INT DEFAULT 5,
    auto_pin BOOLEAN DEFAULT FALSE,
    dnd_enabled BOOLEAN DEFAULT FALSE,
    dnd_start INT DEFAULT 23,
    dnd_end INT DEFAULT 7,
    digest_enabled BOOLEAN DEFAULT TRUE,
    last_digest_date TEXT DEFAULT '',
    completed_count INT DEFAULT 0,
    streak_days INT DEFAULT 0,
    last_done_date TEXT DEFAULT '',
    joined_at TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS nudgelet_reminders (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES nudgelet_users(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    due_ts DOUBLE PRECISION NOT NULL,
    recurrence TEXT DEFAULT 'once',
    priority TEXT DEFAULT 'normal',
    category TEXT DEFAULT 'Personal',
    status TEXT DEFAULT 'active',
    nag_count INT DEFAULT 0,
    last_nag_ts DOUBLE PRECISION DEFAULT 0,
    pinned_msg_id BIGINT,
    media_type TEXT,
    file_id TEXT,
    created_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS nudgelet_notes (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES nudgelet_users(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    tag TEXT DEFAULT '📝 Draft',
    pinned BOOLEAN DEFAULT FALSE,
    media_type TEXT,
    file_id TEXT,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL
);
"""


class SupabaseEngine:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.users: Dict[int, Dict[str, Any]] = {}
        self.reminders: Dict[int, Dict[str, Any]] = {}
        self.notes: Dict[int, Dict[str, Any]] = {}
        self.deleted_reminders_cache: Dict[int, Dict[str, Any]] = {}

    async def init(self, dsn: str) -> None:
        # statement_cache_size=0 ensures compatibility with Supabase Transaction Pooler (port 6543)
        self.pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=10, statement_cache_size=0)
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)

            u_rows = await conn.fetch("SELECT * FROM nudgelet_users")
            for r in u_rows:
                self.users[r["id"]] = dict(r)

            rem_rows = await conn.fetch("SELECT * FROM nudgelet_reminders")
            for r in rem_rows:
                self.reminders[r["id"]] = dict(r)

            note_rows = await conn.fetch("SELECT * FROM nudgelet_notes")
            for r in note_rows:
                self.notes[r["id"]] = dict(r)

        logger.info(
            f"Supabase connected! Loaded {len(self.users)} users, "
            f"{len(self.reminders)} reminders, {len(self.notes)} notes."
        )

    async def ensure_user(self, tg_user) -> Dict[str, Any]:
        uid = tg_user.id
        now_iso = datetime.now(timezone.utc).isoformat()
        username = tg_user.username or ""
        full_name = tg_user.full_name or "User"

        if uid not in self.users:
            u = {
                "id": uid,
                "username": username,
                "full_name": full_name,
                "tz_offset": DEFAULT_TZ_OFFSET,
                "auto_pin": False,
                "dnd_enabled": False,
                "dnd_start": 23,
                "dnd_end": 7,
                "digest_enabled": True,
                "last_digest_date": "",
                "completed_count": 0,
                "streak_days": 0,
                "last_done_date": "",
                "joined_at": now_iso,
            }
            self.users[uid] = u
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO nudgelet_users (
                        id, username, full_name, tz_offset, auto_pin, dnd_enabled,
                        dnd_start, dnd_end, digest_enabled, last_digest_date,
                        completed_count, streak_days, last_done_date, joined_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                    ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name
                    """,
                    uid, username, full_name, DEFAULT_TZ_OFFSET, False, False,
                    23, 7, True, "", 0, 0, "", now_iso,
                )
        else:
            u = self.users[uid]
            if u["username"] != username or u["full_name"] != full_name:
                u["username"] = username
                u["full_name"] = full_name
                await self.save_user(uid)
        return self.users[uid]

    async def save_user(self, uid: int) -> None:
        u = self.users.get(uid)
        if not u:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE nudgelet_users SET
                    username=$2, full_name=$3, tz_offset=$4, auto_pin=$5,
                    dnd_enabled=$6, dnd_start=$7, dnd_end=$8, digest_enabled=$9,
                    last_digest_date=$10, completed_count=$11, streak_days=$12, last_done_date=$13
                WHERE id=$1
                """,
                uid, u["username"], u["full_name"], u["tz_offset"], u["auto_pin"],
                u["dnd_enabled"], u["dnd_start"], u["dnd_end"], u["digest_enabled"],
                u["last_digest_date"], u["completed_count"], u["streak_days"], u["last_done_date"],
            )

    async def create_reminder(
        self,
        user_id: int,
        text: str,
        due_utc: datetime,
        recurrence: str = "once",
        priority: str = "normal",
        category: str = "Personal",
        media_type: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        now_ts = datetime.now(timezone.utc).timestamp()
        due_ts = due_utc.timestamp()
        async with self.pool.acquire() as conn:
            rid = await conn.fetchval(
                """
                INSERT INTO nudgelet_reminders (
                    user_id, text, due_ts, recurrence, priority, category,
                    status, nag_count, last_nag_ts, pinned_msg_id, media_type, file_id, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,'active',0,0,NULL,$7,$8,$9)
                RETURNING id
                """,
                user_id, text, due_ts, recurrence, priority, category, media_type, file_id, now_ts,
            )
        rem = {
            "id": rid,
            "user_id": user_id,
            "text": text,
            "due_ts": due_ts,
            "recurrence": recurrence,
            "priority": priority,
            "category": category,
            "status": "active",
            "nag_count": 0,
            "last_nag_ts": 0.0,
            "pinned_msg_id": None,
            "media_type": media_type,
            "file_id": file_id,
            "created_at": now_ts,
        }
        self.reminders[rid] = rem
        return rem

    async def restore_reminder(self, rem: Dict[str, Any]) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO nudgelet_reminders (
                    id, user_id, text, due_ts, recurrence, priority, category,
                    status, nag_count, last_nag_ts, pinned_msg_id, media_type, file_id, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                ON CONFLICT (id) DO NOTHING
                """,
                rem["id"], rem["user_id"], rem["text"], rem["due_ts"], rem["recurrence"],
                rem["priority"], rem["category"], rem["status"], rem["nag_count"],
                rem.get("last_nag_ts", 0.0), rem.get("pinned_msg_id"),
                rem.get("media_type"), rem.get("file_id"), rem["created_at"],
            )
        self.reminders[rem["id"]] = rem
        return rem

    async def save_reminder(self, rid: int) -> None:
        r = self.reminders.get(rid)
        if not r:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE nudgelet_reminders SET
                    text=$2, due_ts=$3, recurrence=$4, priority=$5, category=$6,
                    status=$7, nag_count=$8, last_nag_ts=$9, pinned_msg_id=$10,
                    media_type=$11, file_id=$12
                WHERE id=$1
                """,
                rid, r["text"], r["due_ts"], r["recurrence"], r["priority"], r["category"],
                r["status"], r["nag_count"], r.get("last_nag_ts", 0.0), r.get("pinned_msg_id"),
                r.get("media_type"), r.get("file_id"),
            )

    async def delete_reminder(self, rid: int) -> Optional[Dict[str, Any]]:
        rem = self.reminders.pop(rid, None)
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM nudgelet_reminders WHERE id=$1", rid)
        return rem

    async def create_note(
        self,
        user_id: int,
        title: str,
        content: str,
        tag: str = "📝 Draft",
        media_type: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        now_ts = datetime.now(timezone.utc).timestamp()
        clean_title = (title or "Attachment Note")[:60]
        async with self.pool.acquire() as conn:
            nid = await conn.fetchval(
                """
                INSERT INTO nudgelet_notes (
                    user_id, title, content, tag, pinned, media_type, file_id, created_at, updated_at
                ) VALUES ($1,$2,$3,$4,FALSE,$5,$6,$7,$8)
                RETURNING id
                """,
                user_id, clean_title, content, tag, media_type, file_id, now_ts, now_ts,
            )
        note = {
            "id": nid,
            "user_id": user_id,
            "title": clean_title,
            "content": content,
            "tag": tag,
            "pinned": False,
            "media_type": media_type,
            "file_id": file_id,
            "created_at": now_ts,
            "updated_at": now_ts,
        }
        self.notes[nid] = note
        return note

    async def save_note(self, nid: int) -> None:
        n = self.notes.get(nid)
        if not n:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE nudgelet_notes SET
                    title=$2, content=$3, tag=$4, pinned=$5,
                    media_type=$6, file_id=$7, updated_at=$8
                WHERE id=$1
                """,
                nid, n["title"], n["content"], n["tag"], n["pinned"],
                n.get("media_type"), n.get("file_id"), n["updated_at"],
            )

    async def delete_note(self, nid: int) -> None:
        self.notes.pop(nid, None)
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM nudgelet_notes WHERE id=$1", nid)


db = SupabaseEngine()
router = Router()

# =====================================================================
# 3. MEDIA & ATTACHMENT HELPERS (PHOTO, VOICE, FILE, VIDEO, AUDIO)
# =====================================================================
MEDIA_BADGES = {
    "photo": "🖼 [Photo]",
    "voice": "🎤 [Voice Note]",
    "document": "📎 [File]",
    "video": "🎬 [Video]",
    "audio": "🎵 [Audio]",
}


def extract_media(message: Message) -> Tuple[Optional[str], Optional[str], str]:
    """
    Extracts (media_type, file_id, text_or_caption) from any message.
    """
    raw_text = (message.text or message.caption or "").strip()
    if message.photo:
        return "photo", message.photo[-1].file_id, (raw_text or "Photo Attachment")
    if message.voice:
        return "voice", message.voice.file_id, (raw_text or "Voice Note")
    if message.document:
        doc_name = message.document.file_name or "Document"
        return "document", message.document.file_id, (raw_text or f"File: {doc_name}")
    if message.video:
        return "video", message.video.file_id, (raw_text or "Video Attachment")
    if message.audio:
        return "audio", message.audio.file_id, (raw_text or "Audio Attachment")
    return None, None, raw_text


async def send_smart_media(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    media_type: Optional[str] = None,
    file_id: Optional[str] = None,
) -> Message:
    """
    Sends the appropriate media attachment with caption & buttons, or falls back to text.
    """
    if media_type and file_id:
        try:
            if media_type == "photo":
                return await bot.send_photo(chat_id, file_id, caption=text, reply_markup=reply_markup)
            if media_type == "voice":
                return await bot.send_voice(chat_id, file_id, caption=text, reply_markup=reply_markup)
            if media_type == "document":
                return await bot.send_document(chat_id, file_id, caption=text, reply_markup=reply_markup)
            if media_type == "video":
                return await bot.send_video(chat_id, file_id, caption=text, reply_markup=reply_markup)
            if media_type == "audio":
                return await bot.send_audio(chat_id, file_id, caption=text, reply_markup=reply_markup)
        except Exception as e:
            logger.warning(f"Media send fallback ({media_type}): {e}")
    return await bot.send_message(chat_id, text, reply_markup=reply_markup)


async def edit_or_resend(
    call: CallbackQuery,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    media_type: Optional[str] = None,
    file_id: Optional[str] = None,
    force_text_only: bool = False,
) -> None:
    """
    Safely edits text/caption messages, or replaces the message when switching
    between text lists and media attachments (photos/voice notes/files).
    """
    msg = call.message
    has_msg_media = msg.text is None

    # If we need to display a media item from a text menu, or go back to a text menu from a media item:
    if (media_type and file_id and not has_msg_media) or (force_text_only and has_msg_media):
        try:
            await msg.delete()
        except Exception:
            pass
        await send_smart_media(
            call.bot,
            msg.chat.id,
            text,
            reply_markup=reply_markup,
            media_type=None if force_text_only else media_type,
            file_id=None if force_text_only else file_id,
        )
        return

    try:
        if has_msg_media:
            await msg.edit_caption(caption=text, reply_markup=reply_markup)
        else:
            await msg.edit_text(text=text, reply_markup=reply_markup)
    except Exception:
        await send_smart_media(call.bot, msg.chat.id, text, reply_markup=reply_markup)


# =====================================================================
# 4. FSM STATES
# =====================================================================
class NudgeStates(StatesGroup):
    rem_waiting_text = State()
    rem_waiting_custom_time = State()
    rem_editing_text = State()
    rem_editing_time = State()
    rem_custom_snooze = State()
    note_waiting_content = State()
    note_appending_content = State()
    note_editing_content = State()
    waiting_search_query = State()


# =====================================================================
# 5. TIMEZONE & NATURAL LANGUAGE TIME PARSER
# =====================================================================
def get_user_tz(user_id: int) -> timezone:
    u = db.users.get(user_id)
    offset = u["tz_offset"] if u else DEFAULT_TZ_OFFSET
    return timezone(timedelta(hours=offset))


def now_user(user_id: int) -> datetime:
    return datetime.now(timezone.utc).astimezone(get_user_tz(user_id))


def fmt_dt(ts: float, user_id: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(get_user_tz(user_id))
    return dt.strftime("%Y-%m-%d %H:%M")


def parse_smart_time(raw: str, user_id: int) -> Optional[Tuple[datetime, str]]:
    text = raw.strip()
    u_now = now_user(user_id)
    text = re.sub(r"^(remind me to|remind me|nudge me to|nudge me)\s+", "", text, flags=re.I)

    # 1. Relative time at start: "in 15m <task>" or "+2h <task>" or "10m <task>"
    m_rel = re.match(
        r"^(?:in\s+|\+)?(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)\b\s*(.*)$",
        text,
        flags=re.I,
    )
    if m_rel:
        val = int(m_rel.group(1))
        unit = m_rel.group(2).lower()
        rest = re.sub(r"^to\s+", "", m_rel.group(3).strip(), flags=re.I)
        delta = (
            timedelta(minutes=val)
            if unit.startswith("m")
            else timedelta(hours=val)
            if unit.startswith("h")
            else timedelta(days=val)
        )
        return (u_now + delta).astimezone(timezone.utc), (rest or "Reminder")

    # 2. Relative time at end: "<task> in 30m"
    m_rel_end = re.match(
        r"^(.+?)\s+in\s+(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$",
        text,
        flags=re.I,
    )
    if m_rel_end:
        rest = m_rel_end.group(1).strip()
        val = int(m_rel_end.group(2))
        unit = m_rel_end.group(3).lower()
        delta = (
            timedelta(minutes=val)
            if unit.startswith("m")
            else timedelta(hours=val)
            if unit.startswith("h")
            else timedelta(days=val)
        )
        return (u_now + delta).astimezone(timezone.utc), rest

    # 3. "tomorrow HH:MM <task>"
    m_tom = re.match(r"^tomorrow\s+(?:at\s+)?(\d{1,2}):(\d{2})\s*(.*)$", text, flags=re.I)
    if m_tom:
        hh, mm, rest = int(m_tom.group(1)), int(m_tom.group(2)), m_tom.group(3).strip()
        if 0 <= hh < 24 and 0 <= mm < 60:
            due_local = (u_now + timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)
            return due_local.astimezone(timezone.utc), (rest or "Reminder")

    # 4. Full date "YYYY-MM-DD HH:MM <task>"
    m_full = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s*(.*)$", text)
    if m_full:
        try:
            d_str, hh, mm, rest = m_full.group(1), int(m_full.group(2)), int(m_full.group(3)), m_full.group(4).strip()
            dt_naive = datetime.strptime(d_str, "%Y-%m-%d").replace(hour=hh, minute=mm)
            due_local = dt_naive.replace(tzinfo=get_user_tz(user_id))
            return due_local.astimezone(timezone.utc), (rest or "Reminder")
        except ValueError:
            pass

    # 5. Time only "HH:MM <task>"
    m_time = re.match(r"^(?:at\s+)?(\d{1,2}):(\d{2})\s*(.*)$", text, flags=re.I)
    if m_time:
        hh, mm, rest = int(m_time.group(1)), int(m_time.group(2)), m_time.group(3).strip()
        if 0 <= hh < 24 and 0 <= mm < 60:
            due_local = u_now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if due_local <= u_now:
                due_local += timedelta(days=1)
            return due_local.astimezone(timezone.utc), (rest or "Reminder")

    return None


def compute_next_occurrence(due_ts: float, recurrence: str) -> Optional[float]:
    dt = datetime.fromtimestamp(due_ts, tz=timezone.utc)
    now_utc = datetime.now(timezone.utc)
    if recurrence == "once":
        return None
    while dt <= now_utc:
        if recurrence == "daily":
            dt += timedelta(days=1)
        elif recurrence == "weekly":
            dt += timedelta(days=7)
        elif recurrence == "2h":
            dt += timedelta(hours=2)
        elif recurrence == "weekdays":
            dt += timedelta(days=1)
            while dt.weekday() >= 5:
                dt += timedelta(days=1)
        else:
            return None
    return dt.timestamp()


# =====================================================================
# 6. KEYBOARDS (REPLY & INLINE)
# =====================================================================
def main_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⏰ New Reminder"), KeyboardButton(text="📋 My Reminders")],
            [KeyboardButton(text="📓 Notebook & Drafts"), KeyboardButton(text="✍️ Quick Draft")],
            [KeyboardButton(text="🔍 Search"), KeyboardButton(text="📊 Stats"), KeyboardButton(text="⚙️ Settings")],
        ],
        resize_keyboard=True,
    )


def fired_nudge_kb(rem_id: int) -> InlineKeyboardMarkup:
    """
    All requested Notification Action Buttons:
    ✅ Done, 💤 +10m, 💤 +1h, 📅 Tomorrow, ✏️ Edit, 🗑 Delete
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Done", callback_data=f"r:done:{rem_id}"),
                InlineKeyboardButton(text="💤 +10m", callback_data=f"r:snz10:{rem_id}"),
                InlineKeyboardButton(text="💤 +1h", callback_data=f"r:snz60:{rem_id}"),
            ],
            [
                InlineKeyboardButton(text="📅 Tomorrow", callback_data=f"r:snztm:{rem_id}"),
                InlineKeyboardButton(text="✏️ Edit", callback_data=f"r:edit:{rem_id}"),
                InlineKeyboardButton(text="🗑 Delete", callback_data=f"r:del:{rem_id}"),
            ],
            [
                InlineKeyboardButton(text="⏳ Custom Snooze", callback_data=f"r:csnz:{rem_id}"),
                InlineKeyboardButton(text="📓 Save to Notebook", callback_data=f"r:tonote:{rem_id}"),
            ],
        ]
    )


def wizard_setup_kb(priority: str, recurrence: str, category: str) -> InlineKeyboardMarkup:
    p_icons = {"normal": "🟢 Normal", "important": "🟡 Important", "urgent": "🔴 Urgent (Nag)"}
    r_icons = {
        "once": "1️⃣ Once",
        "daily": "🔁 Daily",
        "weekdays": "💼 Weekdays",
        "weekly": "📅 Weekly",
        "2h": "⏱ Every 2h",
    }
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"Priority: {p_icons.get(priority, priority)}", callback_data="wiz:prio"),
                InlineKeyboardButton(text=f"Repeat: {r_icons.get(recurrence, recurrence)}", callback_data="wiz:rec"),
            ],
            [
                InlineKeyboardButton(text=f"📂 Category: {category}", callback_data="wiz:cat"),
            ],
            [
                InlineKeyboardButton(text="⚡ +10m", callback_data="wiz:t:10m"),
                InlineKeyboardButton(text="⚡ +30m", callback_data="wiz:t:30m"),
                InlineKeyboardButton(text="⚡ +1h", callback_data="wiz:t:1h"),
                InlineKeyboardButton(text="⚡ +3h", callback_data="wiz:t:3h"),
            ],
            [
                InlineKeyboardButton(text="🌙 Tonight 20:00", callback_data="wiz:t:tonight"),
                InlineKeyboardButton(text="🌅 Tomorrow 09:00", callback_data="wiz:t:tom9"),
            ],
            [
                InlineKeyboardButton(text="📅 Next Monday 09:00", callback_data="wiz:t:mon9"),
                InlineKeyboardButton(text="⌨️ Type Custom Time", callback_data="wiz:t:custom"),
            ],
            [InlineKeyboardButton(text="❌ Cancel", callback_data="wiz:cancel")],
        ]
    )


def reminder_list_kb(user_id: int, filter_mode: str = "active") -> InlineKeyboardMarkup:
    now_ts = datetime.now(timezone.utc).timestamp()
    u_now = now_user(user_id)
    user_rems = [r for r in db.reminders.values() if r["user_id"] == user_id]

    filtered = []
    for r in user_rems:
        if filter_mode == "active" and r["status"] == "active":
            filtered.append(r)
        elif filter_mode == "today" and r["status"] == "active":
            r_dt = datetime.fromtimestamp(r["due_ts"], tz=timezone.utc).astimezone(get_user_tz(user_id))
            if r_dt.date() == u_now.date():
                filtered.append(r)
        elif filter_mode == "recurring" and r["status"] == "active" and r["recurrence"] != "once":
            filtered.append(r)
        elif filter_mode == "overdue" and r["status"] == "active" and r["due_ts"] < now_ts:
            filtered.append(r)
        elif filter_mode == "done" and r["status"] == "completed":
            filtered.append(r)

    filtered.sort(key=lambda x: x["due_ts"])
    rows = []
    p_map = {"normal": "🟢", "important": "🟡", "urgent": "🔴"}

    for r in filtered[:12]:
        icon = "✅" if r["status"] == "completed" else p_map.get(r["priority"], "🟢")
        rec_badge = "🔁" if r["recurrence"] != "once" else ""
        media_icon = "📎" if r.get("file_id") else ""
        t_str = fmt_dt(r["due_ts"], user_id)[5:]
        label = f"{icon}{rec_badge}{media_icon} [{t_str}] {r['text'][:22]}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"r:view:{r['id']}")])

    rows.append(
        [
            InlineKeyboardButton(
                text=("• All •" if filter_mode == "active" else "All"), callback_data="rlist:active"
            ),
            InlineKeyboardButton(
                text=("• Today •" if filter_mode == "today" else "Today"), callback_data="rlist:today"
            ),
            InlineKeyboardButton(
                text=("• Repeat •" if filter_mode == "recurring" else "Repeat"), callback_data="rlist:recurring"
            ),
            InlineKeyboardButton(
                text=("• Done •" if filter_mode == "done" else "Done"), callback_data="rlist:done"
            ),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(text="💤 Snooze All +1h", callback_data="rlist:bulk_snz"),
            InlineKeyboardButton(text="🧹 Clear Completed", callback_data="rlist:bulk_clear"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def notebook_list_kb(user_id: int, tag_filter: str = "all") -> InlineKeyboardMarkup:
    user_notes = [n for n in db.notes.values() if n["user_id"] == user_id]
    if tag_filter == "pinned":
        user_notes = [n for n in user_notes if n.get("pinned")]
    elif tag_filter != "all":
        user_notes = [n for n in user_notes if tag_filter in n.get("tag", "")]

    user_notes.sort(key=lambda x: (1 if x.get("pinned") else 0, x["updated_at"]), reverse=True)

    rows = []
    for n in user_notes[:12]:
        pin_icon = "📌 " if n.get("pinned") else ""
        m_icon = f"{MEDIA_BADGES.get(n.get('media_type'), '').split()[0]} " if n.get("file_id") else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{pin_icon}{m_icon}{n['tag']} | {n['title'][:25]}",
                    callback_data=f"n:view:{n['id']}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(text="➕ New Note / File", callback_data="n:new"),
            InlineKeyboardButton(text="📌 Pinned", callback_data="nlist:pinned"),
            InlineKeyboardButton(text="📚 All Notes", callback_data="nlist:all"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def note_detail_kb(note_id: int, is_pinned: bool) -> InlineKeyboardMarkup:
    pin_lbl = "📍 Unpin" if is_pinned else "📌 Pin to Top"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="➕ Append Text/Media", callback_data=f"n:append:{note_id}"),
                InlineKeyboardButton(text="✏️ Replace Content", callback_data=f"n:edit:{note_id}"),
            ],
            [
                InlineKeyboardButton(text="🏷 Change Tag", callback_data=f"n:tag:{note_id}"),
                InlineKeyboardButton(text=pin_lbl, callback_data=f"n:pin:{note_id}"),
            ],
            [
                InlineKeyboardButton(text="⏰ Turn into Reminder", callback_data=f"n:torem:{note_id}"),
                InlineKeyboardButton(text="🗑 Delete Note", callback_data=f"n:del:{note_id}"),
            ],
            [InlineKeyboardButton(text="⬅️ Back to Notebook", callback_data="nlist:all")],
        ]
    )


# =====================================================================
# 7. START & HELP HANDLERS
# =====================================================================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    u = await db.ensure_user(message.from_user)
    sign = "+" if u["tz_offset"] >= 0 else ""
    text = (
        f"👋 Welcome to <b>Nudgelet</b> (<code>@HeyNudgeletBot</code>), <b>{html.escape(u['full_name'])}</b>!\n\n"
        f"I am your Supabase-powered <b>Smart Reminder Engine</b> and <b>Multimedia Notebook / Draftbook</b>.\n\n"
        f"⚡ <b>Set Reminders (Text, Voice, Photos & Files!):</b>\n"
        f"1️⃣ Tap <b>⏰ New Reminder</b> and send text, a 🎤 voice note, 🖼 photo, or 📎 file.\n"
        f"2️⃣ Use <code>/remind 20m Check the oven</code> or <code>/remind 18:30 Call Mom</code>\n"
        f"3️⃣ Just type naturally or drop any photo/voice/file directly in chat!\n\n"
        f"📓 <b>Notebook & Draftbook:</b>\n"
        f"Save text drafts, voice memos, photos, or documents, append to drafts over time, "
        f"and convert any note (with its attachment!) into a reminder in one tap.\n\n"
        f"🌍 Timezone: <b>UTC{sign}{u['tz_offset']}</b>"
    )
    await message.answer(text, reply_markup=main_reply_kb())


@router.message(Command("help"))
async def cmd_help(message: Message):
    await db.ensure_user(message.from_user)
    text = (
        "📖 <b>Nudgelet Command & Feature Guide</b>\n\n"
        "<b>⏰ Reminders (Text + Media Attachments):</b>\n"
        "• <code>/remind 15m Drink water</code>\n"
        "• <code>/remind 19:00 Review math notes</code>\n"
        "• Send any <b>Photo, Voice Note, Video, or File</b> to schedule a media nudge!\n"
        "• <code>/list</code> — View & filter all reminders\n\n"
        "<b>📓 Notebook & Drafts:</b>\n"
        "• <code>/note Your note text</code> — Instant note save\n"
        "• <code>/draft Your draft text</code> — Save to draftbook\n"
        "• <code>/notes</code> — Open your Multimedia Notebook\n\n"
        "<b>🛠 Tools:</b>\n"
        "• <code>/search keyword</code> — Search reminders & notes\n"
        "• <code>/stats</code> — View streaks & completion rate\n"
        "• <code>/settings</code> — Timezone, Quiet Hours, Auto-Pin, Digest\n\n"
        "<b>👑 Admin Commands:</b>\n"
        "• <code>/users</code> — Total stats & registered user directory\n"
        "• <code>/broadcast message</code> — Broadcast to all users (or reply to media)\n"
        "• <code>/dm user_id message</code> — Private message any user"
    )
    await message.answer(text, reply_markup=main_reply_kb())


# =====================================================================
# 8. ADMIN COMMANDS (/users, /broadcast, /dm)
# =====================================================================
def is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


@router.message(Command("users"))
async def cmd_admin_users(message: Message):
    await db.ensure_user(message.from_user)
    if not is_admin(message.from_user.id):
        await message.answer("⛔ You do not have permission to run <code>/users</code>.")
        return

    total_users = len(db.users)
    active_rems = sum(1 for r in db.reminders.values() if r["status"] == "active")
    done_rems = sum(1 for r in db.reminders.values() if r["status"] == "completed")
    total_notes = len(db.notes)

    lines = [
        "👑 <b>Nudgelet Admin — Supabase User Directory</b>",
        f"👥 <b>Total Users:</b> {total_users}",
        f"⏰ <b>Active Reminders:</b> {active_rems} | ✅ <b>Completed:</b> {done_rems}",
        f"📓 <b>Saved Notes/Drafts:</b> {total_notes}\n",
        "<b>Recent Users:</b>",
    ]

    for u in list(db.users.values())[-30:]:
        uname = f"@{u['username']}" if u.get("username") else "no_username"
        sign = "+" if u["tz_offset"] >= 0 else ""
        lines.append(
            f"• <code>{u['id']}</code> | <b>{html.escape(u['full_name'])}</b> ({uname}) — UTC{sign}{u['tz_offset']}"
        )

    await message.answer("\n".join(lines))


@router.message(Command("broadcast"))
async def cmd_admin_broadcast(message: Message, command: CommandObject, bot: Bot):
    await db.ensure_user(message.from_user)
    if not is_admin(message.from_user.id):
        await message.answer("⛔ You do not have permission to run <code>/broadcast</code>.")
        return

    content = (command.args or "").strip()
    if not content and not message.reply_to_message:
        await message.answer(
            "📢 <b>Usage:</b>\n"
            "<code>/broadcast Your announcement message here</code>\n"
            "<i>Or reply to any photo/voice/file/message with</i> <code>/broadcast</code>."
        )
        return

    sent, failed = 0, 0
    status_msg = await message.answer("⏳ Broadcasting to all users...")

    for uid in list(db.users.keys()):
        try:
            if message.reply_to_message:
                await message.reply_to_message.copy_to(uid)
            else:
                await bot.send_message(uid, f"📢 <b>Nudgelet Announcement</b>\n\n{html.escape(content)}")
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            failed += 1

    await status_msg.edit_text(
        f"✅ <b>Broadcast Complete!</b>\n"
        f"• Delivered: <b>{sent}</b>\n"
        f"• Failed/Blocked: <b>{failed}</b>"
    )


@router.message(Command("dm"))
async def cmd_admin_dm(message: Message, command: CommandObject, bot: Bot):
    await db.ensure_user(message.from_user)
    if not is_admin(message.from_user.id):
        await message.answer("⛔ You do not have permission to run <code>/dm</code>.")
        return

    args = (command.args or "").strip().split(maxsplit=1)
    if len(args) < 2 or not args[0].lstrip("-").isdigit():
        await message.answer(
            "✉️ <b>Usage:</b>\n"
            "<code>/dm &lt;user_id&gt; &lt;your private message&gt;</code>\n\n"
            "Example:\n"
            "<code>/dm 123456789 Hey! How is Nudgelet working for you?</code>"
        )
        return

    target_id = int(args[0])
    dm_text = args[1]

    try:
        await bot.send_message(
            target_id,
            f"💌 <b>Message from Nudgelet Admin:</b>\n\n{html.escape(dm_text)}",
        )
        await message.answer(f"✅ Direct message delivered to <code>{target_id}</code>.")
    except Exception as e:
        await message.answer(f"❌ Failed to send DM to <code>{target_id}</code>: {html.escape(str(e))}")


# =====================================================================
# 9. REMINDER CREATION (WIZARD + COMMAND + MEDIA + NLP)
# =====================================================================
@router.message(F.text == "⏰ New Reminder")
async def start_reminder_wizard(message: Message, state: FSMContext):
    await db.ensure_user(message.from_user)
    await state.set_state(NudgeStates.rem_waiting_text)
    await state.update_data(
        priority="normal", recurrence="once", category="Personal", media_type=None, file_id=None
    )
    await message.answer(
        "📝 <b>What should I nudge you about?</b>\n\n"
        "• Type a task (e.g. <code>Call dentist</code> or <code>in 30m Check oven</code>)\n"
        "• Or send a <b>🎤 Voice Note, 🖼 Photo, 🎬 Video, or 📎 File</b> (with or without a caption)!"
    )


@router.message(Command("remind"))
async def cmd_remind_direct(message: Message, command: CommandObject, state: FSMContext):
    await db.ensure_user(message.from_user)
    raw = (command.args or "").strip()

    # Check if command is replying to a media message
    media_type, file_id = None, None
    if message.reply_to_message:
        media_type, file_id, reply_txt = extract_media(message.reply_to_message)
        if not raw:
            raw = reply_txt

    if not raw:
        await start_reminder_wizard(message, state)
        return

    parsed = parse_smart_time(raw, message.from_user.id)
    if parsed:
        due_utc, task_text = parsed
        rem = await db.create_reminder(
            message.from_user.id, task_text, due_utc, media_type=media_type, file_id=file_id
        )
        await send_created_confirmation(message, rem)
    else:
        await state.set_state(NudgeStates.rem_waiting_custom_time)
        await state.update_data(
            rem_text=raw,
            priority="normal",
            recurrence="once",
            category="Personal",
            media_type=media_type,
            file_id=file_id,
        )
        badge = f"{MEDIA_BADGES.get(media_type, '')} " if media_type else ""
        await message.answer(
            f"📌 <b>Task:</b> {badge}{html.escape(raw)}\n\n"
            f"Configure options below or pick when to be nudged:",
            reply_markup=wizard_setup_kb("normal", "once", "Personal"),
        )


@router.message(NudgeStates.rem_waiting_text)
async def wizard_receive_text_or_media(message: Message, state: FSMContext):
    media_type, file_id, raw = extract_media(message)
    if not raw and not file_id:
        await message.answer("⚠️ Please send text, a voice note, photo, or file for your reminder.")
        return

    parsed = parse_smart_time(raw, message.from_user.id)
    if parsed:
        due_utc, task_text = parsed
        data = await state.get_data()
        rem = await db.create_reminder(
            message.from_user.id,
            task_text,
            due_utc,
            recurrence=data.get("recurrence", "once"),
            priority=data.get("priority", "normal"),
            category=data.get("category", "Personal"),
            media_type=media_type,
            file_id=file_id,
        )
        await state.clear()
        await send_created_confirmation(message, rem)
        return

    await state.update_data(
        rem_text=raw,
        priority="normal",
        recurrence="once",
        category="Personal",
        media_type=media_type,
        file_id=file_id,
    )
    await state.set_state(NudgeStates.rem_waiting_custom_time)
    badge = f"{MEDIA_BADGES.get(media_type, '')} " if media_type else ""
    await message.answer(
        f"📌 <b>Task:</b> {badge}{html.escape(raw)}\n\n"
        f"Select priority, repeat cycle, or tap a quick time preset below:",
        reply_markup=wizard_setup_kb("normal", "once", "Personal"),
    )


@router.callback_query(F.data.startswith("wiz:"))
async def handle_wizard_callbacks(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rem_text = data.get("rem_text", "Reminder")
    prio = data.get("priority", "normal")
    rec = data.get("recurrence", "once")
    cat = data.get("category", "Personal")
    media_type = data.get("media_type")
    file_id = data.get("file_id")

    action = call.data.split(":", 1)[1]

    if action == "cancel":
        await state.clear()
        await edit_or_resend(call, "❌ Action cancelled.", force_text_only=True)
        await call.answer()
        return

    if action == "prio":
        order = ["normal", "important", "urgent"]
        prio = order[(order.index(prio) + 1) % len(order)]
        await state.update_data(priority=prio)
        await call.message.edit_reply_markup(reply_markup=wizard_setup_kb(prio, rec, cat))
        await call.answer(f"Priority: {prio.title()}")
        return

    if action == "rec":
        order = ["once", "daily", "weekdays", "weekly", "2h"]
        rec = order[(order.index(rec) + 1) % len(order)]
        await state.update_data(recurrence=rec)
        await call.message.edit_reply_markup(reply_markup=wizard_setup_kb(prio, rec, cat))
        await call.answer(f"Repeat: {rec}")
        return

    if action == "cat":
        order = ["Personal", "Study", "Work", "Fitness"]
        cat = order[(order.index(cat) + 1) % len(order)]
        await state.update_data(category=cat)
        await call.message.edit_reply_markup(reply_markup=wizard_setup_kb(prio, rec, cat))
        await call.answer(f"Category: {cat}")
        return

    if action == "t:custom":
        await state.set_state(NudgeStates.rem_waiting_custom_time)
        await edit_or_resend(
            call,
            f"📌 <b>Task:</b> {html.escape(rem_text)}\n\n"
            f"⌨️ <b>Type when I should nudge you:</b>\n"
            f"• <code>in 45m</code> or <code>+2h</code>\n"
            f"• <code>18:30</code> (today or tomorrow)\n"
            f"• <code>tomorrow 09:00</code>\n"
            f"• <code>2026-10-25 14:00</code>",
            force_text_only=True,
        )
        await call.answer()
        return

    u_now = now_user(call.from_user.id)
    if action == "t:10m":
        due_local = u_now + timedelta(minutes=10)
    elif action == "t:30m":
        due_local = u_now + timedelta(minutes=30)
    elif action == "t:1h":
        due_local = u_now + timedelta(hours=1)
    elif action == "t:3h":
        due_local = u_now + timedelta(hours=3)
    elif action == "t:tonight":
        due_local = u_now.replace(hour=20, minute=0, second=0, microsecond=0)
        if due_local <= u_now:
            due_local += timedelta(days=1)
    elif action == "t:tom9":
        due_local = (u_now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    elif action == "t:mon9":
        days_ahead = (7 - u_now.weekday()) % 7 or 7
        due_local = (u_now + timedelta(days=days_ahead)).replace(hour=9, minute=0, second=0, microsecond=0)
    else:
        await call.answer()
        return

    rem = await db.create_reminder(
        call.from_user.id,
        rem_text,
        due_local.astimezone(timezone.utc),
        recurrence=rec,
        priority=prio,
        category=cat,
        media_type=media_type,
        file_id=file_id,
    )
    await state.clear()
    try:
        await call.message.delete()
    except Exception:
        pass
    await send_created_confirmation(call.message, rem, user_id=call.from_user.id)
    await call.answer("⏰ Reminder scheduled!")


@router.message(NudgeStates.rem_waiting_custom_time)
async def wizard_receive_custom_time(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    data = await state.get_data()
    rem_text = data.get("rem_text", "Reminder")

    parsed = parse_smart_time(f"{raw} {rem_text}", message.from_user.id)
    if not parsed:
        await message.answer(
            "⚠️ Could not understand that time format.\n"
            "Try: <code>in 25m</code>, <code>18:30</code>, <code>tomorrow 09:00</code>, or <code>2026-10-25 14:30</code>."
        )
        return

    due_utc, _ = parsed
    rem = await db.create_reminder(
        message.from_user.id,
        rem_text,
        due_utc,
        recurrence=data.get("recurrence", "once"),
        priority=data.get("priority", "normal"),
        category=data.get("category", "Personal"),
        media_type=data.get("media_type"),
        file_id=data.get("file_id"),
    )
    await state.clear()
    await send_created_confirmation(message, rem)


async def send_created_confirmation(message: Message, rem: Dict[str, Any], user_id: Optional[int] = None):
    uid = user_id or rem["user_id"]
    p_map = {"normal": "🟢 Normal", "important": "🟡 Important", "urgent": "🔴 Urgent (Persistent Nag)"}
    m_badge = f"\n📎 <b>Attachment:</b> {MEDIA_BADGES.get(rem['media_type'])}" if rem.get("media_type") else ""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Edit", callback_data=f"r:edit:{rem['id']}"),
                InlineKeyboardButton(text="🗑 Delete", callback_data=f"r:del:{rem['id']}"),
            ]
        ]
    )
    caption = (
        f"✅ <b>Nudge Scheduled!</b> (#{rem['id']})\n\n"
        f"📌 <b>Task:</b> {html.escape(rem['text'])}{m_badge}\n"
        f"🕒 <b>When:</b> <code>{fmt_dt(rem['due_ts'], uid)}</code>\n"
        f"🔁 <b>Repeat:</b> {rem['recurrence'].title()} | 📂 <b>Category:</b> {rem['category']}\n"
        f"🚦 <b>Priority:</b> {p_map.get(rem['priority'], '🟢 Normal')}"
    )
    await send_smart_media(
        message.bot,
        message.chat.id,
        caption,
        reply_markup=kb,
        media_type=rem.get("media_type"),
        file_id=rem.get("file_id"),
    )


# =====================================================================
# 10. FIRED NUDGE ACTIONS & IN-PLACE EDITING
# =====================================================================
@router.callback_query(F.data.startswith("r:"))
async def handle_reminder_actions(call: CallbackQuery, state: FSMContext, bot: Bot):
    parts = call.data.split(":")
    action = parts[1]
    rem_id = int(parts[2])
    uid = call.from_user.id

    if action == "undo":
        restored = db.deleted_reminders_cache.pop(rem_id, None)
        if restored:
            await db.restore_reminder(restored)
            await edit_or_resend(
                call,
                f"↩️ <b>Restored Reminder #{rem_id}:</b> {html.escape(restored['text'])}\n"
                f"🕒 Due: <code>{fmt_dt(restored['due_ts'], uid)}</code>",
                reply_markup=fired_nudge_kb(rem_id),
                media_type=restored.get("media_type"),
                file_id=restored.get("file_id"),
            )
            await call.answer("Reminder restored!")
        else:
            await call.answer("Undo expired.", show_alert=True)
        return

    rem = db.reminders.get(rem_id)
    if not rem:
        await call.answer("⚠️ This reminder no longer exists.", show_alert=True)
        return

    # 1. DONE
    if action == "done":
        u = await db.ensure_user(call.from_user)
        u["completed_count"] = u.get("completed_count", 0) + 1
        today_str = now_user(uid).strftime("%Y-%m-%d")
        if u.get("last_done_date") != today_str:
            u["streak_days"] = u.get("streak_days", 0) + 1
            u["last_done_date"] = today_str
        await db.save_user(uid)

        if rem.get("pinned_msg_id"):
            try:
                await bot.unpin_chat_message(uid, rem["pinned_msg_id"])
            except Exception:
                pass
            rem["pinned_msg_id"] = None

        next_ts = compute_next_occurrence(rem["due_ts"], rem["recurrence"])
        if next_ts:
            rem["due_ts"] = next_ts
            rem["nag_count"] = 0
            await db.save_reminder(rem_id)
            await edit_or_resend(
                call,
                f"✅ <b>Completed!</b> (<s>{html.escape(rem['text'])}</s>)\n"
                f"🔁 <b>Next recurring nudge:</b> <code>{fmt_dt(next_ts, uid)}</code>",
            )
        else:
            rem["status"] = "completed"
            rem["nag_count"] = 0
            await db.save_reminder(rem_id)
            await edit_or_resend(
                call,
                f"✅ <b>Done!</b> <s>{html.escape(rem['text'])}</s>\n"
                f"🔥 Streak: <b>{u['streak_days']} day(s)</b>",
            )
        await call.answer("✅ Marked as Done!")
        return

    # 2. SNOOZE (+10m, +1h, Tomorrow)
    if action in ("snz10", "snz60", "snztm"):
        now_ts = datetime.now(timezone.utc).timestamp()
        if action == "snz10":
            rem["due_ts"] = now_ts + 600
            lbl = "+10 minutes"
        elif action == "snz60":
            rem["due_ts"] = now_ts + 3600
            lbl = "+1 hour"
        else:
            u_now = now_user(uid)
            tom_dt = (u_now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
            rem["due_ts"] = tom_dt.astimezone(timezone.utc).timestamp()
            lbl = "Tomorrow 09:00"

        rem["status"] = "active"
        rem["nag_count"] = 0
        await db.save_reminder(rem_id)
        await edit_or_resend(
            call,
            f"💤 <b>Snoozed ({lbl})</b>\n"
            f"📌 {html.escape(rem['text'])}\n"
            f"🕒 New Time: <code>{fmt_dt(rem['due_ts'], uid)}</code>",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✏️ Edit", callback_data=f"r:edit:{rem_id}")]]
            ),
        )
        await call.answer(f"💤 Snoozed {lbl}")
        return

    # 3. CUSTOM SNOOZE
    if action == "csnz":
        await state.set_state(NudgeStates.rem_custom_snooze)
        await state.update_data(edit_rem_id=rem_id)
        await call.message.answer(
            f"⏳ <b>Custom Snooze for:</b> {html.escape(rem['text'])}\n"
            f"Type how long to snooze or when to trigger (e.g., <code>25m</code>, <code>4h</code>, <code>19:45</code>):"
        )
        await call.answer()
        return

    # 4. SAVE REMINDER (AND ITS ATTACHMENT) TO NOTEBOOK
    if action == "tonote":
        note = await db.create_note(
            uid,
            rem["text"][:45],
            rem["text"],
            tag="📌 To-Read",
            media_type=rem.get("media_type"),
            file_id=rem.get("file_id"),
        )
        await call.answer(f"📓 Saved to Notebook (#{note['id']})!", show_alert=True)
        return

    # 5. VIEW / EDIT MENU
    if action in ("view", "edit"):
        m_info = f"\n📎 <b>Media:</b> {MEDIA_BADGES.get(rem['media_type'])}" if rem.get("media_type") else ""
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Done", callback_data=f"r:done:{rem_id}"),
                    InlineKeyboardButton(text="💤 +10m", callback_data=f"r:snz10:{rem_id}"),
                    InlineKeyboardButton(text="💤 +1h", callback_data=f"r:snz60:{rem_id}"),
                ],
                [
                    InlineKeyboardButton(text="📅 Tomorrow", callback_data=f"r:snztm:{rem_id}"),
                    InlineKeyboardButton(text="✏️ Edit Text/Media", callback_data=f"r:etext:{rem_id}"),
                    InlineKeyboardButton(text="🕒 Edit Time", callback_data=f"r:etime:{rem_id}"),
                ],
                [
                    InlineKeyboardButton(text="🚦 Cycle Priority", callback_data=f"r:eprio:{rem_id}"),
                    InlineKeyboardButton(text="🔁 Cycle Repeat", callback_data=f"r:erec:{rem_id}"),
                ],
                [
                    InlineKeyboardButton(text="🗑 Delete", callback_data=f"r:del:{rem_id}"),
                    InlineKeyboardButton(text="⬅️ Back to List", callback_data="rlist:active"),
                ],
            ]
        )
        await edit_or_resend(
            call,
            f"🛠 <b>Reminder #{rem_id} Control Panel</b>\n\n"
            f"📌 <b>Text:</b> {html.escape(rem['text'])}{m_info}\n"
            f"🕒 <b>Due:</b> <code>{fmt_dt(rem['due_ts'], uid)}</code>\n"
            f"🚦 <b>Priority:</b> {rem['priority'].title()} | 🔁 <b>Repeat:</b> {rem['recurrence'].title()}\n"
            f"📂 <b>Category:</b> {rem['category']} | <b>Status:</b> {rem['status'].title()}",
            reply_markup=kb,
            media_type=rem.get("media_type"),
            file_id=rem.get("file_id"),
        )
        await call.answer()
        return

    if action == "eprio":
        order = ["normal", "important", "urgent"]
        rem["priority"] = order[(order.index(rem["priority"]) + 1) % len(order)]
        await db.save_reminder(rem_id)
        call.data = f"r:edit:{rem_id}"
        await handle_reminder_actions(call, state, bot)
        return

    if action == "erec":
        order = ["once", "daily", "weekdays", "weekly", "2h"]
        rem["recurrence"] = order[(order.index(rem["recurrence"]) + 1) % len(order)]
        await db.save_reminder(rem_id)
        call.data = f"r:edit:{rem_id}"
        await handle_reminder_actions(call, state, bot)
        return

    if action == "etext":
        await state.set_state(NudgeStates.rem_editing_text)
        await state.update_data(edit_rem_id=rem_id)
        await call.message.answer(
            f"✏️ Send the new text (or a new photo/voice/file attachment) for Reminder #{rem_id}:"
        )
        await call.answer()
        return

    if action == "etime":
        await state.set_state(NudgeStates.rem_editing_time)
        await state.update_data(edit_rem_id=rem_id)
        await call.message.answer(
            f"🕒 Send the new time for Reminder #{rem_id} (e.g., <code>in 45m</code>, <code>18:30</code>, <code>tomorrow 09:00</code>):"
        )
        await call.answer()
        return

    # 6. DELETE WITH INSTANT UNDO
    if action == "del":
        deleted = await db.delete_reminder(rem_id)
        if deleted:
            db.deleted_reminders_cache[rem_id] = deleted
        undo_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="↩️ Undo Delete", callback_data=f"r:undo:{rem_id}"),
                    InlineKeyboardButton(text="📋 My Reminders", callback_data="rlist:active"),
                ]
            ]
        )
        await edit_or_resend(
            call,
            f"🗑 <b>Deleted:</b> <s>{html.escape(rem['text'])}</s>",
            reply_markup=undo_kb,
            force_text_only=True,
        )
        await call.answer("Deleted!")


@router.message(NudgeStates.rem_custom_snooze)
@router.message(NudgeStates.rem_editing_time)
async def receive_edited_or_snoozed_time(message: Message, state: FSMContext):
    data = await state.get_data()
    rem_id = data.get("edit_rem_id")
    rem = db.reminders.get(rem_id)
    if not rem:
        await state.clear()
        await message.answer("⚠️ Reminder not found.")
        return

    parsed = parse_smart_time(f"{message.text} {rem['text']}", message.from_user.id)
    if not parsed:
        await message.answer("⚠️ Invalid time. Try <code>in 30m</code>, <code>19:00</code>, or <code>tomorrow 09:00</code>.")
        return

    due_utc, _ = parsed
    rem["due_ts"] = due_utc.timestamp()
    rem["status"] = "active"
    rem["nag_count"] = 0
    await db.save_reminder(rem_id)
    await state.clear()
    await send_smart_media(
        message.bot,
        message.chat.id,
        f"✅ <b>Updated Reminder #{rem_id}</b>\n"
        f"📌 {html.escape(rem['text'])}\n"
        f"🕒 New Time: <code>{fmt_dt(rem['due_ts'], message.from_user.id)}</code>",
        reply_markup=fired_nudge_kb(rem_id),
        media_type=rem.get("media_type"),
        file_id=rem.get("file_id"),
    )


@router.message(NudgeStates.rem_editing_text)
async def receive_edited_text_or_media(message: Message, state: FSMContext):
    data = await state.get_data()
    rem_id = data.get("edit_rem_id")
    rem = db.reminders.get(rem_id)
    if not rem:
        await state.clear()
        return

    media_type, file_id, raw = extract_media(message)
    if raw:
        rem["text"] = raw
    if file_id:
        rem["media_type"] = media_type
        rem["file_id"] = file_id

    await db.save_reminder(rem_id)
    await state.clear()
    await send_smart_media(
        message.bot,
        message.chat.id,
        f"✅ <b>Updated Reminder #{rem_id}:</b>\n📌 {html.escape(rem['text'])}",
        reply_markup=fired_nudge_kb(rem_id),
        media_type=rem.get("media_type"),
        file_id=rem.get("file_id"),
    )


# =====================================================================
# 11. REMINDER LIST DASHBOARD & BULK ACTIONS
# =====================================================================
@router.message(Command("list"))
@router.message(F.text == "📋 My Reminders")
async def cmd_list_reminders(message: Message):
    await db.ensure_user(message.from_user)
    uid = message.from_user.id
    active_count = sum(1 for r in db.reminders.values() if r["user_id"] == uid and r["status"] == "active")
    await message.answer(
        f"📋 <b>Your Reminders Dashboard</b> ({active_count} active)\n"
        f"Tap any reminder to view its attachment & control panel:",
        reply_markup=reminder_list_kb(uid, "active"),
    )


@router.callback_query(F.data.startswith("rlist:"))
async def handle_rlist_tabs(call: CallbackQuery):
    uid = call.from_user.id
    mode = call.data.split(":", 1)[1]

    if mode == "bulk_snz":
        count = 0
        for r in db.reminders.values():
            if r["user_id"] == uid and r["status"] == "active":
                r["due_ts"] += 3600
                await db.save_reminder(r["id"])
                count += 1
        await call.answer(f"💤 Snoozed {count} active reminder(s) by +1 hour!", show_alert=True)
        mode = "active"

    elif mode == "bulk_clear":
        to_del = [rid for rid, r in db.reminders.items() if r["user_id"] == uid and r["status"] == "completed"]
        for rid in to_del:
            await db.delete_reminder(rid)
        await call.answer(f"🧹 Cleared {len(to_del)} completed reminder(s)!", show_alert=True)
        mode = "active"

    await edit_or_resend(
        call,
        f"📋 <b>Your Reminders — Filter: {mode.title()}</b>",
        reply_markup=reminder_list_kb(uid, mode),
        force_text_only=True,
    )
    await call.answer()


# =====================================================================
# 12. MULTIMEDIA NOTEBOOK & DRAFTBOOK SYSTEM
# =====================================================================
@router.message(Command("notes"))
@router.message(F.text == "📓 Notebook & Drafts")
async def cmd_open_notebook(message: Message):
    await db.ensure_user(message.from_user)
    uid = message.from_user.id
    total = sum(1 for n in db.notes.values() if n["user_id"] == uid)
    await message.answer(
        f"📓 <b>Your Multimedia Notebook & Draftbook</b> ({total} saved)\n\n"
        f"Store text drafts, 🎤 voice memos, 🖼 photos, or 📎 files. "
        f"Append to drafts anytime or convert any note into a scheduled reminder!",
        reply_markup=notebook_list_kb(uid, "all"),
    )


@router.message(F.text == "✍️ Quick Draft")
async def start_quick_draft(message: Message, state: FSMContext):
    await db.ensure_user(message.from_user)
    await state.set_state(NudgeStates.note_waiting_content)
    await message.answer(
        "✍️ <b>Send your Note, Draft, Voice Memo, Photo, or File:</b>\n"
        "<i>(First line or caption becomes the title automatically!)</i>"
    )


@router.message(Command("note", "draft"))
async def cmd_quick_note(message: Message, command: CommandObject, state: FSMContext):
    await db.ensure_user(message.from_user)
    raw = (command.args or "").strip()
    media_type, file_id = None, None

    if message.reply_to_message:
        media_type, file_id, reply_txt = extract_media(message.reply_to_message)
        if not raw:
            raw = reply_txt

    if not raw and not file_id:
        await start_quick_draft(message, state)
        return

    title = raw.split("\n", 1)[0][:50]
    tag = "📝 Draft" if command.command == "draft" else "💡 Idea"
    note = await db.create_note(
        message.from_user.id, title, raw, tag=tag, media_type=media_type, file_id=file_id
    )
    await send_smart_media(
        message.bot,
        message.chat.id,
        f"📓 <b>Saved to Notebook (#{note['id']})</b>\n"
        f"🏷 <b>Tag:</b> {note['tag']}\n"
        f"📌 <b>Title:</b> {html.escape(note['title'])}\n\n"
        f"{html.escape(note['content'])}",
        reply_markup=note_detail_kb(note["id"], note["pinned"]),
        media_type=media_type,
        file_id=file_id,
    )


@router.message(NudgeStates.note_waiting_content)
async def receive_new_note_content(message: Message, state: FSMContext):
    media_type, file_id, raw = extract_media(message)
    if not raw and not file_id:
        await message.answer("⚠️ Please send text, a voice note, photo, or file.")
        return
    await state.clear()
    title = raw.split("\n", 1)[0][:50]
    note = await db.create_note(
        message.from_user.id, title, raw, tag="📝 Draft", media_type=media_type, file_id=file_id
    )
    await send_smart_media(
        message.bot,
        message.chat.id,
        f"📓 <b>Draft Saved (#{note['id']})!</b>\n\n{html.escape(note['content'])}",
        reply_markup=note_detail_kb(note["id"], note["pinned"]),
        media_type=media_type,
        file_id=file_id,
    )


@router.callback_query(F.data.startswith("nlist:"))
async def handle_notebook_list_cb(call: CallbackQuery):
    tag_filter = call.data.split(":", 1)[1]
    await edit_or_resend(
        call,
        f"📓 <b>Notebook & Draftbook ({tag_filter.title()})</b>",
        reply_markup=notebook_list_kb(call.from_user.id, tag_filter),
        force_text_only=True,
    )
    await call.answer()


@router.callback_query(F.data.startswith("n:"))
async def handle_note_actions(call: CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    action = parts[1]
    uid = call.from_user.id

    if action == "new":
        await state.set_state(NudgeStates.note_waiting_content)
        await call.message.answer("✍️ <b>Send text, a voice note, photo, or file for your new Note:</b>")
        await call.answer()
        return

    note_id = int(parts[2])
    note = db.notes.get(note_id)
    if not note:
        await call.answer("⚠️ Note not found.", show_alert=True)
        return

    if action == "view":
        pin_str = "📌 Pinned | " if note.get("pinned") else ""
        m_str = f"\n📎 <b>Attachment:</b> {MEDIA_BADGES.get(note['media_type'])}" if note.get("media_type") else ""
        await edit_or_resend(
            call,
            f"📓 <b>{pin_str}{note['tag']} — #{note['id']}</b>{m_str}\n"
            f"🕒 Updated: <code>{fmt_dt(note['updated_at'], uid)}</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{html.escape(note['content'])}",
            reply_markup=note_detail_kb(note_id, note.get("pinned", False)),
            media_type=note.get("media_type"),
            file_id=note.get("file_id"),
        )
        await call.answer()

    elif action == "pin":
        note["pinned"] = not note.get("pinned", False)
        await db.save_note(note_id)
        call.data = f"n:view:{note_id}"
        await handle_note_actions(call, state)

    elif action == "tag":
        tags = ["📝 Draft", "💡 Idea", "📌 To-Read", "🔑 Snippet"]
        cur = note.get("tag", "📝 Draft")
        note["tag"] = tags[(tags.index(cur) + 1) % len(tags)] if cur in tags else tags[0]
        await db.save_note(note_id)
        call.data = f"n:view:{note_id}"
        await handle_note_actions(call, state)

    elif action == "append":
        await state.set_state(NudgeStates.note_appending_content)
        await state.update_data(active_note_id=note_id)
        await call.message.answer(
            f"➕ <b>Send text or a media attachment to append to Draft #{note_id}</b>:"
        )
        await call.answer()

    elif action == "edit":
        await state.set_state(NudgeStates.note_editing_content)
        await state.update_data(active_note_id=note_id)
        await call.message.answer(f"✏️ <b>Send replacement text or media for Note #{note_id}:</b>")
        await call.answer()

    elif action == "torem":
        await state.set_state(NudgeStates.rem_waiting_custom_time)
        await state.update_data(
            rem_text=note["content"][:200],
            priority="normal",
            recurrence="once",
            category="Personal",
            media_type=note.get("media_type"),
            file_id=note.get("file_id"),
        )
        await edit_or_resend(
            call,
            f"⏰ <b>Convert Note #{note_id} to Reminder:</b>\n"
            f"📌 {html.escape(note['content'][:150])}\n\n"
            f"Pick when I should nudge you:",
            reply_markup=wizard_setup_kb("normal", "once", "Personal"),
            force_text_only=True,
        )
        await call.answer()

    elif action == "del":
        await db.delete_note(note_id)
        await edit_or_resend(
            call,
            f"🗑 Note #{note_id} deleted.",
            reply_markup=notebook_list_kb(uid, "all"),
            force_text_only=True,
        )
        await call.answer("Deleted!")


@router.message(NudgeStates.note_appending_content)
async def receive_note_append(message: Message, state: FSMContext):
    data = await state.get_data()
    nid = data.get("active_note_id")
    note = db.notes.get(nid)
    if not note:
        await state.clear()
        return

    media_type, file_id, raw = extract_media(message)
    if raw:
        note["content"] = f"{note['content']}\n\n{raw}"
    if file_id:
        note["media_type"] = media_type
        note["file_id"] = file_id
    note["updated_at"] = datetime.now(timezone.utc).timestamp()

    await db.save_note(nid)
    await state.clear()
    await send_smart_media(
        message.bot,
        message.chat.id,
        f"✅ <b>Appended to Draft #{nid}!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{html.escape(note['content'])}",
        reply_markup=note_detail_kb(nid, note.get("pinned", False)),
        media_type=note.get("media_type"),
        file_id=note.get("file_id"),
    )


@router.message(NudgeStates.note_editing_content)
async def receive_note_replace(message: Message, state: FSMContext):
    data = await state.get_data()
    nid = data.get("active_note_id")
    note = db.notes.get(nid)
    if not note:
        await state.clear()
        return

    media_type, file_id, raw = extract_media(message)
    if raw:
        note["title"] = raw.split("\n", 1)[0][:50]
        note["content"] = raw
    if file_id:
        note["media_type"] = media_type
        note["file_id"] = file_id
    note["updated_at"] = datetime.now(timezone.utc).timestamp()

    await db.save_note(nid)
    await state.clear()
    await send_smart_media(
        message.bot,
        message.chat.id,
        f"✅ <b>Updated Note #{nid}!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{html.escape(note['content'])}",
        reply_markup=note_detail_kb(nid, note.get("pinned", False)),
        media_type=note.get("media_type"),
        file_id=note.get("file_id"),
    )


# =====================================================================
# 13. SEARCH, STATS & SETTINGS
# =====================================================================
@router.message(Command("search"))
@router.message(F.text == "🔍 Search")
async def cmd_search(message: Message, command: Optional[CommandObject] = None, state: FSMContext = None):
    await db.ensure_user(message.from_user)
    query = (command.args if command else "") or ""
    if not query.strip():
        await state.set_state(NudgeStates.waiting_search_query)
        await message.answer("🔍 <b>Send a keyword to search across all your Reminders and Notes:</b>")
        return
    await execute_search(message, query.strip())


@router.message(NudgeStates.waiting_search_query)
async def receive_search_query(message: Message, state: FSMContext):
    await state.clear()
    await execute_search(message, (message.text or "").strip())


async def execute_search(message: Message, q: str):
    uid = message.from_user.id
    q_low = q.lower()
    rems = [r for r in db.reminders.values() if r["user_id"] == uid and q_low in r["text"].lower()]
    notes = [
        n
        for n in db.notes.values()
        if n["user_id"] == uid and (q_low in n["content"].lower() or q_low in n["title"].lower())
    ]

    rows = []
    for r in rems[:6]:
        rows.append([InlineKeyboardButton(text=f"⏰ {r['text'][:32]}", callback_data=f"r:view:{r['id']}")])
    for n in notes[:6]:
        rows.append([InlineKeyboardButton(text=f"📓 {n['title'][:32]}", callback_data=f"n:view:{n['id']}")])

    await message.answer(
        f"🔍 <b>Search Results for \"{html.escape(q)}\":</b>\n"
        f"Found <b>{len(rems)}</b> reminder(s) and <b>{len(notes)}</b> note(s).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows) if rows else None,
    )


@router.message(Command("stats"))
@router.message(F.text == "📊 Stats")
async def cmd_stats(message: Message):
    u = await db.ensure_user(message.from_user)
    uid = message.from_user.id
    active = sum(1 for r in db.reminders.values() if r["user_id"] == uid and r["status"] == "active")
    notes_cnt = sum(1 for n in db.notes.values() if n["user_id"] == uid)
    await message.answer(
        f"📊 <b>Your Nudgelet Productivity Stats</b>\n\n"
        f"🔥 <b>Current Daily Streak:</b> {u.get('streak_days', 0)} day(s)\n"
        f"✅ <b>Total Nudges Completed:</b> {u.get('completed_count', 0)}\n"
        f"⏰ <b>Active Scheduled Reminders:</b> {active}\n"
        f"📓 <b>Saved Notes & Drafts:</b> {notes_cnt}"
    )


def settings_kb(u: Dict[str, Any]) -> InlineKeyboardMarkup:
    sign = "+" if u["tz_offset"] >= 0 else ""
    pin_str = "ON ✅" if u.get("auto_pin") else "OFF ❌"
    dnd_str = "ON 🌙 (23:00-07:00)" if u.get("dnd_enabled") else "OFF ❌"
    dig_str = "ON 🌅 (08:00)" if u.get("digest_enabled") else "OFF ❌"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏪ TZ -1h", callback_data="set:tz:-1"),
                InlineKeyboardButton(text=f"🌍 UTC{sign}{u['tz_offset']}", callback_data="set:tz:reset"),
                InlineKeyboardButton(text="TZ +1h ⏩", callback_data="set:tz:+1"),
            ],
            [InlineKeyboardButton(text=f"📌 Auto-Pin Nudges in Chat: {pin_str}", callback_data="set:pin")],
            [InlineKeyboardButton(text=f"🌙 Quiet Hours (DND): {dnd_str}", callback_data="set:dnd")],
            [InlineKeyboardButton(text=f"🌅 Morning Daily Digest: {dig_str}", callback_data="set:digest")],
        ]
    )


@router.message(Command("settings"))
@router.message(F.text == "⚙️ Settings")
async def cmd_settings(message: Message):
    u = await db.ensure_user(message.from_user)
    await message.answer(
        f"⚙️ <b>Nudgelet Preferences</b>\n"
        f"🕒 Your Local Time: <code>{now_user(u['id']).strftime('%Y-%m-%d %H:%M')}</code>",
        reply_markup=settings_kb(u),
    )


@router.callback_query(F.data.startswith("set:"))
async def handle_settings_cb(call: CallbackQuery):
    u = await db.ensure_user(call.from_user)
    parts = call.data.split(":")
    key = parts[1]

    if key == "tz":
        op = parts[2]
        if op == "-1":
            u["tz_offset"] = max(-12, u["tz_offset"] - 1)
        elif op == "+1":
            u["tz_offset"] = min(14, u["tz_offset"] + 1)
        else:
            u["tz_offset"] = DEFAULT_TZ_OFFSET
    elif key == "pin":
        u["auto_pin"] = not u.get("auto_pin", False)
    elif key == "dnd":
        u["dnd_enabled"] = not u.get("dnd_enabled", False)
    elif key == "digest":
        u["digest_enabled"] = not u.get("digest_enabled", True)

    await db.save_user(u["id"])
    await call.message.edit_text(
        f"⚙️ <b>Nudgelet Preferences</b>\n"
        f"🕒 Your Local Time: <code>{now_user(u['id']).strftime('%Y-%m-%d %H:%M')}</code>",
        reply_markup=settings_kb(u),
    )
    await call.answer("Settings saved to Supabase!")


# =====================================================================
# 14. CATCH-ALL: MEDIA DROPS, FORWARDED MESSAGES & NLP FALLBACK
# =====================================================================
@router.message()
async def fallback_smart_handler(message: Message, state: FSMContext):
    await db.ensure_user(message.from_user)
    media_type, file_id, raw = extract_media(message)
    if not raw and not file_id:
        return

    # Try smart natural language time parsing first (even if caption on photo/voice/file!)
    parsed = parse_smart_time(raw, message.from_user.id)
    if parsed and not message.forward_origin:
        due_utc, task_text = parsed
        rem = await db.create_reminder(
            message.from_user.id, task_text, due_utc, media_type=media_type, file_id=file_id
        )
        await send_created_confirmation(message, rem)
        return

    # Otherwise offer interactive choice: Schedule Reminder OR Save to Notebook
    await state.update_data(
        rem_text=raw[:250],
        priority="normal",
        recurrence="once",
        category="Personal",
        media_type=media_type,
        file_id=file_id,
    )
    await state.set_state(NudgeStates.rem_waiting_custom_time)
    kb = wizard_setup_kb("normal", "once", "Personal")
    kb.inline_keyboard.insert(
        0, [InlineKeyboardButton(text="📓 Save to Notebook / Drafts Instead", callback_data="fwd:tonote")]
    )
    badge = f"{MEDIA_BADGES.get(media_type, '')} " if media_type else ""
    header = "📨 <b>Forwarded Content Captured!</b>" if message.forward_origin else "💡 <b>Captured!</b>"
    await message.answer(
        f"{header}\n"
        f"📌 {badge}<i>{html.escape(raw[:140])}</i>\n\n"
        f"Pick a time below to schedule a nudge, or save it straight to your Notebook:",
        reply_markup=kb,
    )


@router.callback_query(F.data == "fwd:tonote")
async def handle_save_captured_as_note(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    raw = data.get("rem_text", "Saved Note")
    media_type = data.get("media_type")
    file_id = data.get("file_id")
    await state.clear()
    note = await db.create_note(
        call.from_user.id,
        raw.split("\n", 1)[0][:50],
        raw,
        tag="📝 Draft",
        media_type=media_type,
        file_id=file_id,
    )
    await edit_or_resend(
        call,
        f"📓 <b>Saved to Notebook (#{note['id']})!</b>\n\n{html.escape(note['content'])}",
        reply_markup=note_detail_kb(note["id"], note["pinned"]),
        media_type=media_type,
        file_id=file_id,
    )
    await call.answer("Saved to Supabase Notebook!")


# =====================================================================
# 15. BACKGROUND SCHEDULER + DOWNTIME CATCH-UP ("⚠️ Delayed nudge")
# =====================================================================
async def scheduler_engine(bot: Bot):
    logger.info("Background Scheduler Engine started.")
    while True:
        try:
            now_ts = datetime.now(timezone.utc).timestamp()

            for rem in list(db.reminders.values()):
                if rem["status"] != "active":
                    continue

                uid = rem["user_id"]
                u = db.users.get(uid)
                u_now = now_user(uid)

                if u and u.get("dnd_enabled") and rem["priority"] != "urgent":
                    hr = u_now.hour
                    if hr >= 23 or hr < 7:
                        continue

                if now_ts >= rem["due_ts"]:
                    delay_seconds = now_ts - rem["due_ts"]
                    is_first_fire = rem.get("nag_count", 0) == 0

                    if not is_first_fire and rem["priority"] != "urgent":
                        continue

                    if not is_first_fire and rem["priority"] == "urgent":
                        last_nag_ts = rem.get("last_nag_ts", rem["due_ts"])
                        if now_ts - last_nag_ts < 300 or rem["nag_count"] >= 5:
                            continue

                    if is_first_fire and delay_seconds > 60:
                        header = "⚠️ <b>Delayed nudge</b> <i>(Caught up after downtime)</i>"
                    elif not is_first_fire:
                        header = f"🔴 <b>URGENT FOLLOW-UP NUDGE #{rem['nag_count'] + 1}</b>"
                    else:
                        p_hdr = {
                            "normal": "🔔 <b>Nudgelet Reminder!</b>",
                            "important": "🟡 <b>Important Nudge!</b>",
                            "urgent": "🔴 <b>URGENT NUDGE!</b>",
                        }
                        header = p_hdr.get(rem["priority"], "🔔 <b>Nudgelet Reminder!</b>")

                    msg_text = (
                        f"{header}\n\n"
                        f"📌 <b>{html.escape(rem['text'])}</b>\n"
                        f"📂 {rem['category']} | 🕒 Scheduled: <code>{fmt_dt(rem['due_ts'], uid)}</code>"
                    )

                    try:
                        sent_msg = await send_smart_media(
                            bot,
                            uid,
                            msg_text,
                            reply_markup=fired_nudge_kb(rem["id"]),
                            media_type=rem.get("media_type"),
                            file_id=rem.get("file_id"),
                        )
                        if u and u.get("auto_pin") and is_first_fire:
                            try:
                                await bot.pin_chat_message(uid, sent_msg.message_id, disable_notification=True)
                                rem["pinned_msg_id"] = sent_msg.message_id
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning(f"Failed sending reminder #{rem['id']} to {uid}: {e}")

                    rem["nag_count"] = rem.get("nag_count", 0) + 1
                    rem["last_nag_ts"] = now_ts
                    await db.save_reminder(rem["id"])

            # Daily 08:00 Morning Digest Check
            for uid, u in list(db.users.items()):
                if not u.get("digest_enabled", True):
                    continue
                u_now = now_user(uid)
                today_str = u_now.strftime("%Y-%m-%d")
                if u_now.hour == 8 and u.get("last_digest_date") != today_str:
                    u["last_digest_date"] = today_str
                    await db.save_user(uid)
                    todays_rems = [
                        r
                        for r in db.reminders.values()
                        if r["user_id"] == uid
                        and r["status"] == "active"
                        and datetime.fromtimestamp(r["due_ts"], tz=timezone.utc)
                        .astimezone(get_user_tz(uid))
                        .date()
                        <= u_now.date()
                    ]
                    if todays_rems:
                        lines = [f"🌅 <b>Good Morning! You have {len(todays_rems)} nudge(s) for today:</b>\n"]
                        for r in todays_rems[:10]:
                            lines.append(f"• <code>{fmt_dt(r['due_ts'], uid)[11:]}</code> — {html.escape(r['text'])}")
                        try:
                            await bot.send_message(uid, "\n".join(lines), reply_markup=reminder_list_kb(uid, "today"))
                        except Exception:
                            pass

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(5)


# =====================================================================
# 16. RAILWAY HEALTH-CHECK SERVER & ENTRYPOINT
# =====================================================================
async def health_handler(request):
    return web.Response(text="Nudgelet (@HeyNudgeletBot) + Supabase is alive!")


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health-check web server listening on port {PORT}")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN in .env or Railway environment variables!")
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL in .env or Railway environment variables!")

    await db.init(DATABASE_URL)

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Launch Nudgelet & Main Menu"),
            BotCommand(command="remind", description="Quick reminder (e.g. /remind 15m Tea)"),
            BotCommand(command="list", description="Open Reminders Dashboard"),
            BotCommand(command="note", description="Save a quick Note, Voice, or File"),
            BotCommand(command="notes", description="Open Multimedia Notebook"),
            BotCommand(command="search", description="Search reminders & notes"),
            BotCommand(command="stats", description="Productivity streak & stats"),
            BotCommand(command="settings", description="Timezone, DND & Auto-Pin"),
            BotCommand(command="help", description="Full command guide"),
        ]
    )

    await start_health_server()
    asyncio.create_task(scheduler_engine(bot))

    logger.info("Starting @HeyNudgeletBot polling...")
    await bot.delete_webhook(drop_pending_updates=False)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())