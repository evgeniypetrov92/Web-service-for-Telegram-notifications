import os
import time
import mimetypes
from pathlib import Path
from typing import Optional, Dict, Any, List

import requests

from .db import upsert_chat, get_setting

MAX_API_BASE = os.getenv("MAX_API_BASE", "https://platform-api.max.ru")

def _get_max_token(conn=None) -> str:
    if conn is not None:
        t = get_setting(conn, "max_bot_token")
        if t:
            return t.strip()
    t = os.getenv("MAX_BOT_TOKEN", "")
    if not t:
        raise RuntimeError("MAX_BOT_TOKEN is not set (and no max_bot_token in DB settings)")
    return t.strip()

def _headers(token: str) -> Dict[str, str]:
    return {"Authorization": token}

def max_api_get(path: str, token: str, params: Optional[dict] = None) -> dict:
    r = requests.get(f"{MAX_API_BASE}{path}", headers=_headers(token), params=params or {}, timeout=95)
    r.raise_for_status()
    return r.json() if r.content else {}

def max_api_post(path: str, token: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
    r = requests.post(f"{MAX_API_BASE}{path}", headers={**_headers(token), "Content-Type": "application/json"}, params=params or {}, json=json_body or {}, timeout=95)
    r.raise_for_status()
    return r.json() if r.content else {}

def max_get_me(conn=None, token: Optional[str] = None) -> dict:
    tok = token or _get_max_token(conn)
    return max_api_get("/me", tok)

def sync_max_chats(conn, token: Optional[str] = None) -> int:
    tok = token or _get_max_token(conn)
    count = 0
    marker = None
    while True:
        params = {}
        if marker is not None:
            params["marker"] = marker
        data = max_api_get("/chats", tok, params=params)
        items = data.get("chats") or data.get("items") or []
        for chat in items:
            chat_id = chat.get("chat_id") or chat.get("id")
            if not chat_id:
                continue
            title = chat.get("title") or f"max chat {chat_id}"
            upsert_chat(conn, int(chat_id), title, "max_chat")
            count += 1
        marker = data.get("marker")
        if not marker or not items:
            break
    return count

def _extract_chat_from_update(update: dict) -> Optional[dict]:
    chat_id = update.get("chat_id")
    title = None
    chat_type = "max_chat"
    chat = update.get("chat") or {}
    if isinstance(chat, dict):
        chat_id = chat_id or chat.get("chat_id") or chat.get("id")
        title = chat.get("title")
        chat_type = chat.get("type") or chat_type
    message = update.get("message") or {}
    if isinstance(message, dict):
        chat_id = chat_id or message.get("chat_id")
        title = title or message.get("chat_title")
        body = message.get("body") or {}
        if isinstance(body, dict):
            title = title or body.get("chat_title")
    user = update.get("user") or {}
    if not title and isinstance(user, dict):
        title = user.get("username") or user.get("first_name") or user.get("name")
        if user:
            chat_type = "max_user"
    if not chat_id:
        return None
    return {"chat_id": int(chat_id), "title": title or f"max chat {chat_id}", "chat_type": chat_type}


def _extract_text_from_update(update: dict) -> str:
    message = update.get("message") or {}
    if isinstance(message, dict):
        body = message.get("body") or {}
        if isinstance(body, dict):
            for key in ("text", "markdown", "html", "caption", "message"):
                value = body.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        for key in ("text", "message", "command"):
            value = message.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    for key in ("text", "message", "command"):
        value = update.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""

def _is_addchat_command(text: str) -> bool:
    if not text:
        return False
    first = text.strip().split()[0].lower()
    return first.startswith("/addchat") or first == "addchat"


def run_max_polling(conn, stop_event, token: Optional[str] = None):
    tok = token or _get_max_token(conn)
    marker = None
    while not stop_event.is_set():
        try:
            params = {
                "timeout": 30,
                "limit": 100,
                "types": "bot_started,message_created,bot_added,chat_title_changed",
            }
            if marker is not None:
                params["marker"] = marker

            data = max_api_get("/updates", tok, params=params)
            marker = data.get("marker", marker)

            for upd in data.get("updates", []) or []:
                chat_info = _extract_chat_from_update(upd)
                if chat_info:
                    upsert_chat(conn, chat_info["chat_id"], chat_info["title"], chat_info["chat_type"])

                text_value = _extract_text_from_update(upd)
                if chat_info and _is_addchat_command(text_value):
                    upsert_chat(conn, chat_info["chat_id"], chat_info["title"], chat_info["chat_type"])
                    try:
                        reply_text = f"Чат добавлен в базу. chat_id={chat_info['chat_id']}"
                        send_message(tok, chat_info["chat_id"], text=reply_text)
                    except Exception as e:
                        print("MAX /addchat reply failed:", e)

        except Exception as e:
            print("MAX polling error:", e)
            time.sleep(5)

def upload_file(token: str, file_path: str, mime_type: str = "") -> dict:
    mime = mime_type or mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    main = mime.split("/")[0].lower()
    utype = "image" if main == "image" else "file"

    meta = max_api_post("/uploads", token, params={"type": utype})
    upload_url = meta.get("url")
    if not upload_url:
        raise RuntimeError(f"MAX upload URL was not returned. meta={meta}")

    with open(file_path, "rb") as f:
        r = requests.post(
            upload_url,
            headers=_headers(token),
            files={"data": (Path(file_path).name, f, mime)},
            timeout=180,
        )

    body_text = r.text[:1500] if r.text else ""
    print("MAX upload status:", r.status_code)
    print("MAX upload body:", body_text)

    r.raise_for_status()

    try:
        payload = r.json() if r.content else {}
    except Exception:
        raise RuntimeError(f"MAX upload returned non-JSON body: {body_text}")

    if not isinstance(payload, dict) or not payload:
        raise RuntimeError(f"MAX upload returned empty payload: {payload}")

    return {"type": utype, "payload": payload}

def send_message(token: str, chat_id: int, text: str = "", html: str = "", file_paths: Optional[List[dict]] = None) -> dict:
    body: Dict[str, Any] = {}
    msg = (html or text or "").strip()
    if msg:
        body["text"] = msg[:4000]
        if html:
            body["format"] = "html"
    attachments = []
    for item in file_paths or []:
        attachments.append(upload_file(token, item["file_path"], item.get("mime_type") or ""))
    if attachments:
        body["attachments"] = attachments
        time.sleep(2)
    if not body:
        raise RuntimeError("Empty MAX message")
    return max_api_post("/messages", token, params={"chat_id": int(chat_id)}, json_body=body)
