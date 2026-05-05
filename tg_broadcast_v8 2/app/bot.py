import os
import asyncio
from typing import Optional

from aiogram import Bot, Dispatcher
from aiogram.types import Message, ChatMemberUpdated
from aiogram.filters import CommandStart, Command

from .db import upsert_chat, get_setting

def _get_bot_token(conn=None) -> str:
    # Prefer token from DB settings (admin can change it in the panel)
    if conn is not None:
        t = get_setting(conn, "bot_token")
        if t:
            return t.strip()
    t = os.getenv("BOT_TOKEN")
    if not t:
        raise RuntimeError("BOT_TOKEN is not set (and no bot_token in DB settings)")
    return t.strip()

def make_bot(conn=None, token: Optional[str] = None) -> Bot:
    tok = (token or "").strip() if token else _get_bot_token(conn)
    return Bot(token=tok)

def setup_handlers(dp: Dispatcher, conn):
    @dp.message(CommandStart())
    async def start(message: Message):
        await message.answer("Привет! Добавь меня в групповой чат и дай права отправки. Чат появится в веб-панели.")

    @dp.message(Command("whereami"))
    async def whereami(message: Message):
        chat = message.chat
        title = getattr(chat, "title", None) or "(без названия)"
        await message.answer(f"chat_id: {chat.id}\nНазвание: {title}\nТип: {chat.type}")

    @dp.message(Command("addchat"))
    async def addchat(message: Message):
        """Принудительно сохранить текущий чат в БД.

        Полезно, если по какой-то причине не отработало событие my_chat_member
        (например, бот был добавлен в чат давно или Telegram не прислал апдейт).
        """
        chat = message.chat

        # Title logic: groups/supergroups have .title; private chats use user's name
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or f"chat {chat.id}"
        if chat.type == "private" and message.from_user is not None:
            fn = getattr(message.from_user, "first_name", None) or ""
            ln = getattr(message.from_user, "last_name", None) or ""
            name = (fn + " " + ln).strip()
            if name:
                title = name

        upsert_chat(conn, chat.id, title, chat.type)
        await message.answer(
            "✅ Чат сохранён в базе.\n"
            f"chat_id: {chat.id}\n"
            f"Название: {title}\n"
            f"Тип: {chat.type}"
        )


    @dp.my_chat_member()
    async def on_my_chat_member(update: ChatMemberUpdated):
        chat = update.chat
        if chat.type in ("group", "supergroup"):
            upsert_chat(conn, chat.id, chat.title or f"chat {chat.id}", chat.type)

async def run_bot_polling(conn, token: Optional[str] = None):
    bot = make_bot(conn=conn, token=token)
    dp = Dispatcher()
    setup_handlers(dp, conn)
    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        # Graceful stop
        try:
            await bot.session.close()
        except Exception:
            pass
        raise
    finally:
        try:
            await bot.session.close()
        except Exception:
            pass
