import os, shutil, mimetypes, asyncio, re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from html.parser import HTMLParser
from html import escape as html_escape
from io import BytesIO

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from openpyxl import Workbook

from aiogram.types import FSInputFile

from .db import (
    connect, init_db,
    list_chats, set_chat_blocked, delete_chat,
    create_broadcast, add_broadcast_file, get_broadcast_files,
    log_send, list_logs, list_broadcast_summaries, get_broadcast_summary, list_broadcast_logs,
    list_groups, create_group, delete_group, get_group,
    set_group_members, list_group_members, get_group_chat_ids,
    get_user_by_id, get_user_by_username, list_users, create_user, delete_user, update_user_password,
    get_setting, set_setting,
)
from .auth import verify_password, set_session_cookie, clear_session_cookie, get_session_data
from .bot import run_bot_polling, make_bot
from .max_routes import register_max_routes, start_max_polling, max_conn, MAX_DB_PATH
from .max_backup_routes import register_max_backup_routes

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "app.db"))

app = FastAPI()

@app.middleware("http")
async def disable_admin_cache(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/admin"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

conn = connect(DB_PATH)
init_db(conn)


# --- Rich text (Quill) -> Telegram HTML (safe subset) ---
# Telegram HTML supports: <b>, <i>, <u>, <s>, <a href>, <code>, <pre>
# Quill produces a wider HTML; we down-convert to a conservative Telegram-friendly subset.


class _QuillToTelegramHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out: List[str] = []
        self.list_stack: List[dict] = []  # [{'type':'ul'|'ol','n':int}]
        self._in_pre = False
        self._a_stack: List[bool] = []

    def _ensure_newline(self):
        if not self.out:
            return
        if not self.out[-1].endswith("\n"):
            self.out.append("\n")

    def handle_starttag(self, tag, attrs):
        a = dict(attrs or [])
        tag = tag.lower()

        if tag in ("strong", "b"):
            self.out.append("<b>")
        elif tag in ("em", "i"):
            self.out.append("<i>")
        elif tag == "u":
            self.out.append("<u>")
        elif tag in ("s", "strike", "del"):
            self.out.append("<s>")
        elif tag == "a":
            href = (a.get("href") or "").strip()
            if href:
                self.out.append(f'<a href="{html_escape(href, quote=True)}">')
                self._a_stack.append(True)
            else:
                self._a_stack.append(False)
        elif tag == "br":
            self.out.append("\n")
        elif tag in ("p", "div"):
            # Block element: newline will be appended on endtag
            pass
        elif tag == "ul":
            self.list_stack.append({"type": "ul", "n": 0})
        elif tag == "ol":
            self.list_stack.append({"type": "ol", "n": 0})
        elif tag == "li":
            self._ensure_newline()
            indent = 0
            cls = a.get("class") or ""
            m = re.search(r"ql-indent-(\d+)", cls)
            if m:
                try:
                    indent = int(m.group(1))
                except Exception:
                    indent = 0

            prefix = "• "
            if self.list_stack and self.list_stack[-1]["type"] == "ol":
                self.list_stack[-1]["n"] += 1
                prefix = f"{self.list_stack[-1]['n']}. "

            self.out.append(("  " * max(0, indent)) + prefix)
        elif tag == "pre":
            self._ensure_newline()
            self.out.append("<pre>")
            self._in_pre = True
        elif tag == "code":
            self.out.append("<code>")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in ("strong", "b"):
            self.out.append("</b>")
        elif tag in ("em", "i"):
            self.out.append("</i>")
        elif tag == "u":
            self.out.append("</u>")
        elif tag in ("s", "strike", "del"):
            self.out.append("</s>")
        elif tag == "a":
            if self._a_stack:
                opened = self._a_stack.pop()
                if opened:
                    self.out.append("</a>")
        elif tag in ("p", "div"):
            self._ensure_newline()
        elif tag in ("ul", "ol"):
            if self.list_stack:
                self.list_stack.pop()
            self._ensure_newline()
        elif tag == "li":
            self._ensure_newline()
        elif tag == "pre":
            self.out.append("</pre>")
            self._in_pre = False
            self._ensure_newline()
        elif tag == "code":
            self.out.append("</code>")

    def handle_data(self, data):
        if not data:
            return
        data = data.replace("\xa0", " ")
        self.out.append(html_escape(data, quote=False))


def quill_html_to_telegram_html(quill_html: str) -> str:
    src = (quill_html or "").strip()
    if not src:
        return ""

    p = _QuillToTelegramHTML()
    try:
        p.feed(src)
        p.close()
    except Exception:
        # Fallback: strip tags to plain text
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", src)).strip()

    out = "".join(p.out)
    out = re.sub(r"[ \t]+\n", "\n", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    out = out.strip()

    # Telegram message limit is 4096 chars; keep a conservative cap (tags included)
    if len(out) > 4000:
        out = out[:3997].rstrip() + "…"
    return out

# NOTE: init_db() already ensures the required schema and creates default users.
# A previous version referenced ensure_password_hash(), but it is not needed.

@app.on_event("startup")
async def startup():
    # Clear stale active broadcast marker (in case of restart)
    try:
        set_setting(conn, "active_broadcast_id", "")
        set_setting(conn, "active_broadcast_cancel", "0")
    except Exception:
        pass

    # Start Telegram bot polling in background
    app.state.bot_task_lock = asyncio.Lock()
    app.state.bot_task = asyncio.create_task(run_bot_polling(conn))

    # Active broadcast task (in-memory)
    app.state.active_broadcast_task = None
    app.state.active_broadcast_id = None

    # Start MAX polling in background (separate DB/state)
    start_max_polling(app)

async def restart_bot_polling() -> None:
    """Restart polling to apply a new bot token without restarting the whole service."""
    if not hasattr(app.state, "bot_task_lock"):
        app.state.bot_task_lock = asyncio.Lock()
    async with app.state.bot_task_lock:
        task = getattr(app.state, "bot_task", None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                # ignore errors during shutdown
                pass
        app.state.bot_task = asyncio.create_task(run_bot_polling(conn))

@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse(url="/admin/compose", status_code=302)


@app.get("/admin/platform/telegram")
async def admin_platform_telegram():
    return RedirectResponse(url=f"/admin/compose?ts={int(datetime.utcnow().timestamp())}", status_code=302)

@app.get("/admin/platform/max")
async def admin_platform_max():
    return RedirectResponse(url=f"/admin/max/compose?ts={int(datetime.utcnow().timestamp())}", status_code=302)


def fmt_dt(value: str) -> str:
    """Format stored datetime as Moscow time (MSK) in dd.mm.yyyy HH:MM:SS.

    We store datetimes as naive UTC ISO strings (datetime.utcnow().isoformat()).
    """
    if not value:
        return ""
    msk = timezone(timedelta(hours=3))
    try:
        v = value.replace("Z", "")
        dt = datetime.fromisoformat(v)
    except Exception:
        try:
            dt = datetime.fromisoformat(value.replace(" ", "T"))
        except Exception:
            return str(value)

    # Assume stored value is UTC if tzinfo is missing
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(msk).strftime("%d.%m.%Y %H:%M:%S")



def strip_html_text(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def build_broadcast_preview(text_value: str, html_value: str, limit: int = 140) -> str:
    source = (text_value or "").strip() or strip_html_text(html_value or "")
    source = re.sub(r"\s+", " ", source).strip()
    if not source:
        return "Без текста"
    return source if len(source) <= limit else (source[:limit - 1].rstrip() + "…")


def summarize_broadcast_row(row: dict, active_broadcast_id: str = "") -> dict:
    item = dict(row)
    bid = str(item.get("broadcast_id", "") or "")
    item["created_at_fmt"] = fmt_dt(item.get("created_at", ""))
    item["last_event_at_fmt"] = fmt_dt(item.get("last_event_at", ""))
    item["message_preview"] = build_broadcast_preview(item.get("text", ""), item.get("html", ""))
    ok_count = int(item.get("ok_count") or 0)
    error_count = int(item.get("error_count") or 0)
    skipped_count = int(item.get("skipped_count") or 0)
    total_chat_rows = int(item.get("total_chat_rows") or 0)
    item["recipient_count"] = total_chat_rows
    if int(item.get("cancelled_count") or 0) > 0:
        item["status_key"] = "cancelled"
        item["status_label"] = "Отменена"
    elif int(item.get("service_error_count") or 0) > 0 and total_chat_rows == 0:
        item["status_key"] = "error"
        item["status_label"] = "Ошибка"
    elif active_broadcast_id and bid == str(active_broadcast_id):
        item["status_key"] = "running"
        item["status_label"] = "Выполняется"
    else:
        item["status_key"] = "done"
        item["status_label"] = "Завершена"
    item["success_rate"] = round((ok_count / total_chat_rows) * 100, 1) if total_chat_rows else 0
    return item

def msk_date_to_utc_iso(date_str: str, end: bool = False) -> str:
    """Convert YYYY-MM-DD in MSK (UTC+3) to naive UTC ISO string for SQLite filtering."""
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None
    if end:
        msk_dt = datetime(d.year, d.month, d.day, 23, 59, 59, 999999)
    else:
        msk_dt = datetime(d.year, d.month, d.day, 0, 0, 0, 0)
    utc_dt = msk_dt - timedelta(hours=3)  # MSK -> UTC
    return utc_dt.isoformat()

    # If timezone is missing, assume UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(msk)
    return dt.strftime("%d.%m.%Y %H:%M:%S")

def current_user(request: Request):
    sess = get_session_data(request)
    if not sess:
        return None
    return get_user_by_id(conn, int(sess.get("uid")))


def require_auth(request: Request, roles=None):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/admin/login", status_code=302)
    if roles and user.get("role") not in roles:
        return templates.TemplateResponse(
            "forbidden.html",
            {"request": request, "user": user, "message": "Недостаточно прав"},
            status_code=403,
        )
    request.state.user = user
    return None


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_get(request: Request):
    init_db(conn)
    user = current_user(request)
    if user:
        return RedirectResponse(url="/admin/compose", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/admin/login", response_class=HTMLResponse)
async def admin_login_post(request: Request):
    form = await request.form()
    username = (form.get("login") or "").strip()
    password = form.get("password") or ""

    init_db(conn)
    user = get_user_by_username(conn, username)
    if not user or not verify_password(password, user["password_hash"]):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Неверный логин или пароль"})

    resp = RedirectResponse(url="/admin/compose", status_code=302)
    set_session_cookie(resp, user)
    return resp

@app.get("/admin/logout")
async def admin_logout(request: Request):
    resp = RedirectResponse(url="/admin/login", status_code=302)
    clear_session_cookie(resp)
    return resp





@app.get("/admin/chats", response_class=HTMLResponse)
async def chats_page(request: Request):
    redir = require_auth(request)
    if redir: return redir
    return templates.TemplateResponse("chats.html", {"request": request, "user": request.state.user,
        "chats": list_chats(conn, include_blocked=True),
        "active": "chats",
        "title": "Чаты"
    })

@app.post("/admin/chats/block")
async def chat_block_toggle(request: Request, chat_id: int = Form(...), blocked: int = Form(...)):
    redir = require_auth(request, roles=['admin'])
    if redir: return redir
    set_chat_blocked(conn, int(chat_id), bool(int(blocked)))
    return RedirectResponse(url="/admin/chats", status_code=302)

@app.post("/admin/chats/delete")
async def chat_delete(request: Request, chat_id: int = Form(...)):
    redir = require_auth(request, roles=['admin'])
    if redir: return redir
    delete_chat(conn, int(chat_id))
    return RedirectResponse(url="/admin/chats", status_code=302)

@app.get("/admin/groups", response_class=HTMLResponse)
async def groups_page(request: Request):
    redir = require_auth(request)
    if redir: return redir
    return templates.TemplateResponse("groups.html", {"request": request, "user": request.state.user,
        "groups": list_groups(conn),
        "chats": list_chats(conn, include_blocked=True),
        "active": "groups",
        "title": "Группировки"
    })

@app.post("/admin/groups/create")
async def groups_create(request: Request, name: str = Form(...)):
    redir = require_auth(request)
    if redir: return redir
    if (name or "").strip():
        create_group(conn, name.strip())
    return RedirectResponse(url="/admin/groups", status_code=302)

@app.post("/admin/groups/delete")
async def groups_delete(request: Request, group_id: int = Form(...)):
    redir = require_auth(request, roles=['admin'])
    if redir: return redir
    delete_group(conn, int(group_id))
    return RedirectResponse(url="/admin/groups", status_code=302)

@app.get("/admin/groups/edit", response_class=HTMLResponse)
async def group_edit_page(request: Request, group_id: int):
    redir = require_auth(request)
    if redir: return redir
    group = get_group(conn, int(group_id))
    if not group:
        return RedirectResponse(url="/admin/groups", status_code=302)
    members = set(int(r["chat_id"]) for r in list_group_members(conn, int(group_id)))
    return templates.TemplateResponse("group_edit.html", {"request": request, "user": request.state.user,
        "group": group,
        "chats": list_chats(conn, include_blocked=True),
        "members": members,
        "active": "groups",
        "title": "Редактирование"
    })

@app.post("/admin/groups/edit")
async def group_edit_save(request: Request, group_id: int = Form(...), chat_ids: List[int] = Form([])):
    redir = require_auth(request)
    if redir: return redir
    set_group_members(conn, int(group_id), chat_ids or [])
    return RedirectResponse(url="/admin/groups", status_code=302)

def get_active_broadcast_state():
    bid = (get_setting(conn, "active_broadcast_id") or "").strip()
    if not bid:
        return None

    def _int(key: str, default: int = 0) -> int:
        try:
            return int((get_setting(conn, key) or str(default)).strip())
        except Exception:
            return default

    return {
        "id": bid,
        "started_at": (get_setting(conn, "active_broadcast_started_at") or "").strip(),
        "total": _int("active_broadcast_total", 0),
        "ok": _int("active_broadcast_ok", 0),
        "err": _int("active_broadcast_err", 0),
        "skipped": _int("active_broadcast_skipped", 0),
        "cancel": ((get_setting(conn, "active_broadcast_cancel") or "0").strip() == "1"),
    }

async def _broadcast_worker(bid: int, final_ids: List[int], bfiles: list, msg_html: str, msg_plain: str) -> None:
    bot = make_bot(conn=conn)
    ok = 0
    err = 0

    async def _send_text(cid: int):
        if msg_html:
            try:
                await bot.send_message(cid, msg_html, parse_mode="HTML")
                return
            except Exception:
                pass
        if msg_plain:
            await bot.send_message(cid, msg_plain)

    def _pick_caption():
        # Telegram captions are limited (~1024 chars). Keep a conservative cap for HTML.
        if msg_html and len(msg_html) <= 900:
            return "HTML", msg_html
        if msg_plain and len(msg_plain) <= 1024:
            return None, msg_plain
        return None, None

    async def _send_file(cid: int, bf: dict, caption=None, parse_mode=None):
        path = bf["file_path"]
        name = bf["file_name"]
        mime = (bf["mime_type"] or "").lower()
        if mime.startswith("image/"):
            await bot.send_photo(cid, photo=FSInputFile(path, filename=name), caption=caption, parse_mode=parse_mode)
        else:
            await bot.send_document(cid, document=FSInputFile(path, filename=name), caption=caption, parse_mode=parse_mode)

    cancelled = False
    try:
        for cid in final_ids:
            # Cancel requested?
            if (get_setting(conn, "active_broadcast_cancel") or "0").strip() == "1":
                cancelled = True
                break

            try:
                if bfiles:
                    pm, cap = _pick_caption()
                    if cap:
                        # 1st file with caption (text + media in one message)
                        try:
                            await _send_file(cid, bfiles[0], caption=cap, parse_mode=pm)
                        except Exception:
                            # fallback to plain caption or separate send
                            if pm == "HTML" and msg_plain and len(msg_plain) <= 1024:
                                try:
                                    await _send_file(cid, bfiles[0], caption=msg_plain, parse_mode=None)
                                except Exception:
                                    await _send_text(cid)
                                    await _send_file(cid, bfiles[0])
                            else:
                                await _send_text(cid)
                                await _send_file(cid, bfiles[0])

                        # Remaining files as separate messages
                        for bf in bfiles[1:]:
                            await _send_file(cid, bf)
                    else:
                        # Text too long for caption -> send text then files
                        await _send_text(cid)
                        for bf in bfiles:
                            await _send_file(cid, bf)
                else:
                    await _send_text(cid)

                log_send(conn, bid, cid, "OK", "")
                ok += 1
                set_setting(conn, "active_broadcast_ok", str(ok))
            except Exception as e:
                log_send(conn, bid, cid, "ERROR", str(e)[:1000])
                err += 1
                set_setting(conn, "active_broadcast_err", str(err))
    except asyncio.CancelledError:
        cancelled = True
    finally:
        # Final marker
        if cancelled:
            set_setting(conn, "active_broadcast_cancel", "1")
            log_send(conn, bid, 0, "CANCELLED", f"Отменено пользователем. OK={ok}, ERR={err}")
        else:
            log_send(conn, bid, 0, "DONE", f"Завершено. OK={ok}, ERR={err}")

        # Clear active broadcast flag (keep counters for inspection)
        set_setting(conn, "active_broadcast_id", "")
        set_setting(conn, "active_broadcast_finished_at", datetime.utcnow().isoformat())
        try:
            set_setting(conn, "active_broadcast_cancel", "0")
        except Exception:
            pass

        # Clear in-memory task refs
        app.state.active_broadcast_task = None
        app.state.active_broadcast_id = None

        # Close bot session if available
        try:
            await bot.session.close()
        except Exception:
            pass

@app.get("/admin/compose", response_class=HTMLResponse)
async def compose_page(request: Request, err: str = ""):
    redir = require_auth(request)
    if redir: 
        return redir

    error = None
    if err == "select":
        error = "Выберите хотя бы один чат или группировку для рассылки."
    elif err == "running":
        active_id = (get_setting(conn, "active_broadcast_id") or "").strip()
        error = f"Сейчас уже выполняется рассылка #{active_id}. Дождитесь завершения или отмените её в Истории."

    return templates.TemplateResponse(
        "compose.html",
        {
            "request": request,
            "user": request.state.user,
            "chats": list_chats(conn, include_blocked=True),
            "groups": list_groups(conn),
            "active_broadcast": get_active_broadcast_state(),
            "error": error,
            "active": "compose",
            "title": "Рассылка",
        },
    )

@app.get("/admin/history", response_class=HTMLResponse)
async def history_page(request: Request, date_from: str = "", date_to: str = ""):
    redir = require_auth(request)
    if redir:
        return redir

    dt_from = msk_date_to_utc_iso((date_from or "").strip(), end=False)
    dt_to = msk_date_to_utc_iso((date_to or "").strip(), end=True)
    active = get_active_broadcast_state()
    active_id = str(active.get("id")) if active else ""

    broadcasts = [
        summarize_broadcast_row(dict(row), active_id)
        for row in list_broadcast_summaries(conn, 500, dt_from=dt_from, dt_to=dt_to)
    ]

    return templates.TemplateResponse(
        "history.html",
        {
            "request": request,
            "user": request.state.user,
            "broadcasts": broadcasts,
            "date_from": (date_from or "").strip(),
            "date_to": (date_to or "").strip(),
            "active_broadcast": active,
            "active": "history",
            "title": "История отправки",
        },
    )


@app.get("/admin/history/{broadcast_id}", response_class=HTMLResponse)
async def history_detail_page(request: Request, broadcast_id: int):
    redir = require_auth(request)
    if redir:
        return redir

    summary_row = get_broadcast_summary(conn, broadcast_id)
    if not summary_row:
        return RedirectResponse(url="/admin/history", status_code=302)

    active = get_active_broadcast_state()
    active_id = str(active.get("id")) if active else ""
    broadcast = summarize_broadcast_row(dict(summary_row), active_id)
    broadcast["message_full"] = (broadcast.get("text") or "").strip() or strip_html_text(broadcast.get("html") or "") or "—"
    broadcast["files"] = [dict(x) for x in get_broadcast_files(conn, broadcast_id)]

    logs = [dict(l) for l in list_broadcast_logs(conn, broadcast_id, 10000)]
    for l in logs:
        l["created_at_fmt"] = fmt_dt(l.get("created_at", ""))
        l["is_service"] = int(l.get("chat_id") or 0) == 0

    return templates.TemplateResponse(
        "history_detail.html",
        {
            "request": request,
            "user": request.state.user,
            "broadcast": broadcast,
            "logs": logs,
            "active_broadcast": active,
            "active": "history",
            "title": f"Рассылка #{broadcast_id}",
        },
    )


@app.get("/admin/history/export")
async def history_export(request: Request, date_from: str = "", date_to: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/admin/login", status_code=302)

    dt_from = msk_date_to_utc_iso((date_from or "").strip(), end=False)
    dt_to = msk_date_to_utc_iso((date_to or "").strip(), end=True)
    active = get_active_broadcast_state()
    active_id = str(active.get("id")) if active else ""

    rows = [
        summarize_broadcast_row(dict(r), active_id)
        for r in list_broadcast_summaries(conn, 100000, dt_from=dt_from, dt_to=dt_to)
    ]

    wb = Workbook()
    ws = wb.active
    ws.title = "Broadcasts"
    ws.append(["Рассылка", "Создана (МСК)", "Отправитель", "Статус", "Получателей", "OK", "Ошибок", "Пропущено", "Файлов", "Текст"])
    for r in rows:
        ws.append([
            r.get("broadcast_id", ""),
            r.get("created_at_fmt", ""),
            r.get("broadcast_user", "") or "",
            r.get("status_label", ""),
            r.get("recipient_count", 0),
            r.get("ok_count", 0),
            r.get("error_count", 0),
            r.get("skipped_count", 0),
            r.get("file_count", 0),
            r.get("message_preview", ""),
        ])

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    fname = "history.xlsx"
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/admin/history/{broadcast_id}/export")
async def history_detail_export(request: Request, broadcast_id: int):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/admin/login", status_code=302)

    summary_row = get_broadcast_summary(conn, broadcast_id)
    if not summary_row:
        return RedirectResponse(url="/admin/history", status_code=302)

    rows = [dict(l) for l in list_broadcast_logs(conn, broadcast_id, 100000)]
    for r in rows:
        r["created_at_fmt"] = fmt_dt(r.get("created_at", ""))
        r["is_service"] = int(r.get("chat_id") or 0) == 0

    wb = Workbook()
    ws = wb.active
    ws.title = f"Broadcast {broadcast_id}"
    ws.append(["Время (МСК)", "Рассылка", "Chat ID", "Чат", "Статус", "Детали"])
    for r in rows:
        ws.append([
            r.get("created_at_fmt", ""),
            broadcast_id,
            "" if r.get("is_service") else r.get("chat_id", ""),
            "Служебно" if r.get("is_service") else (r.get("chat_title", "") or ""),
            r.get("status", "") or "",
            r.get("details", "") or "",
        ])

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    fname = f"history_{broadcast_id}.xlsx"
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )

@app.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings(request: Request):
    init_db(conn)
    r = require_auth(request)
    if r:
        return r
    stored_token = (get_setting(conn, "bot_token") or "").strip()
    bot_token_status = "не задан" if not stored_token else (stored_token[:6] + "…" + stored_token[-4:] if len(stored_token) > 12 else "установлен")
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "user": request.state.user,
            "is_admin": request.state.user["role"] == "admin",
            "bot_token_status": bot_token_status,
            "has_bot_token": bool(stored_token),
            "ok": None,
            "error": None,
            "active": "settings",
        },
    )


@app.post("/admin/settings/password", response_class=HTMLResponse)
async def admin_settings_password(request: Request):
    init_db(conn)
    r = require_auth(request)
    if r:
        return r

    form = await request.form()
    current_pw = form.get("current_password") or ""
    new_pw = form.get("new_password") or ""
    new_pw2 = form.get("new_password2") or ""

    user = get_user_by_id(conn, request.state.user["id"])
    if not user or not verify_password(current_pw, user["password_hash"]):
        return templates.TemplateResponse("settings.html", {"request": request, "user": request.state.user, "is_admin": request.state.user["role"]=="admin", "ok": None, "error": "Текущий пароль неверный", "active":"settings"})

    if not new_pw or len(new_pw) < 4:
        return templates.TemplateResponse("settings.html", {"request": request, "user": request.state.user, "is_admin": request.state.user["role"]=="admin", "ok": None, "error": "Новый пароль слишком короткий", "active":"settings"})

    if new_pw != new_pw2:
        return templates.TemplateResponse("settings.html", {"request": request, "user": request.state.user, "is_admin": request.state.user["role"]=="admin", "ok": None, "error": "Пароли не совпадают", "active":"settings"})

    update_user_password(conn, request.state.user["id"], new_pw)
    return templates.TemplateResponse("settings.html", {"request": request, "user": request.state.user, "is_admin": request.state.user["role"]=="admin", "ok": "Пароль обновлён", "error": None, "active":"settings"})


@app.post("/admin/settings/bot_token", response_class=HTMLResponse)
async def admin_settings_bot_token(request: Request):
    init_db(conn)
    r = require_auth(request)
    if r:
        return r
    if request.state.user["role"] != "admin":
        return RedirectResponse(url="/admin/compose", status_code=302)

    form = await request.form()
    token1 = (form.get("bot_token") or "").strip()
    token2 = (form.get("bot_token2") or "").strip()

    stored_token = (get_setting(conn, "bot_token") or "").strip()
    bot_token_status = "не задан" if not stored_token else (stored_token[:6] + "…" + stored_token[-4:] if len(stored_token) > 12 else "установлен")

    if not token1 or not token2:
        return templates.TemplateResponse(
            "settings.html",
            {"request": request, "user": request.state.user, "is_admin": True,
             "bot_token_status": bot_token_status, "has_bot_token": bool(stored_token),
             "ok": None, "error": "Токен не может быть пустым", "active": "settings"},
        )

    if token1 != token2:
        return templates.TemplateResponse(
            "settings.html",
            {"request": request, "user": request.state.user, "is_admin": True,
             "bot_token_status": bot_token_status, "has_bot_token": bool(stored_token),
             "ok": None, "error": "Токены не совпадают", "active": "settings"},
        )

    # Persist and apply immediately
    set_setting(conn, "bot_token", token1)
    await restart_bot_polling()

    stored_token = (get_setting(conn, "bot_token") or "").strip()
    bot_token_status = "не задан" if not stored_token else (stored_token[:6] + "…" + stored_token[-4:] if len(stored_token) > 12 else "установлен")

    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "user": request.state.user, "is_admin": True,
         "bot_token_status": bot_token_status, "has_bot_token": bool(stored_token),
         "ok": "Токен применён. Бот перезапущен.", "error": None, "active": "settings"},
    )

@app.post("/admin/send")
async def send_broadcast(
    request: Request,
    html: str = Form(""),
    text: str = Form(""),
    group_ids: List[int] = Form([]),
    chat_ids: List[int] = Form([]),
    files: List[UploadFile] = File(default=[]),
):
    redir = require_auth(request)
    if redir:
        return redir

    # Require explicit recipients selection
    if not (chat_ids or group_ids):
        return RedirectResponse(url="/admin/compose?err=select", status_code=302)

    # Prevent starting a second broadcast while one is running
    active_id = (get_setting(conn, "active_broadcast_id") or "").strip()
    if active_id:
        return RedirectResponse(url="/admin/compose?err=running", status_code=302)

    # Resolve recipients
    resolved = set(int(cid) for cid in (chat_ids or []))
    for gid in (group_ids or []):
        for cid in get_group_chat_ids(conn, int(gid)):
            resolved.add(int(cid))

    if not resolved:
        return RedirectResponse(url="/admin/compose?err=select", status_code=302)

    # Filter out blocked chats
    blocked_ids = set(
        int(r["chat_id"])
        for r in list_chats(conn, include_blocked=True)
        if int(r["blocked"] or 0) == 1
    )
    final_ids = [cid for cid in sorted(resolved) if cid not in blocked_ids]

    # Log skipped blocked recipients (if explicitly selected)
    skipped = [cid for cid in sorted(resolved) if cid in blocked_ids]
    # If all selected are blocked, do not start
    if not final_ids:
        # Keep a small note in logs for debugging
        bid_tmp = create_broadcast(conn, html=html, text=text, created_by=request.state.user.get('username',''))
        for cid in skipped:
            log_send(conn, bid_tmp, cid, "SKIPPED", "chat is blocked in panel")
        log_send(conn, bid_tmp, 0, "ERROR", "No available chats (all selected are blocked)")
        return RedirectResponse(url="/admin/history", status_code=302)

    bid = create_broadcast(conn, html=html, text=text, created_by=request.state.user.get('username',''))

    for cid in skipped:
        log_send(conn, bid, cid, "SKIPPED", "chat is blocked in panel")

    # Save files
    for f in files or []:
        if not f.filename:
            continue
        safe_name = f"{bid}__{Path(f.filename).name}"
        dest = UPLOAD_DIR / safe_name
        with dest.open("wb") as out:
            shutil.copyfileobj(f.file, out)
        mime = f.content_type or mimetypes.guess_type(str(dest))[0] or "application/octet-stream"
        add_broadcast_file(conn, bid, str(dest), Path(f.filename).name, mime)

    # Prepare message + files
    msg_plain = (text or "").strip()
    msg_html = quill_html_to_telegram_html(html)
    bfiles = get_broadcast_files(conn, bid)

    # Mark active broadcast (for UI + cancel)
    set_setting(conn, "active_broadcast_id", str(bid))
    set_setting(conn, "active_broadcast_cancel", "0")
    set_setting(conn, "active_broadcast_started_at", datetime.utcnow().isoformat())
    set_setting(conn, "active_broadcast_total", str(len(final_ids)))
    set_setting(conn, "active_broadcast_ok", "0")
    set_setting(conn, "active_broadcast_err", "0")
    set_setting(conn, "active_broadcast_skipped", str(len(skipped)))

    # Start worker in background
    task = asyncio.create_task(_broadcast_worker(bid, final_ids, bfiles, msg_html, msg_plain))
    app.state.active_broadcast_task = task
    app.state.active_broadcast_id = bid

    return RedirectResponse(url="/admin/history", status_code=302)



@app.get("/admin/broadcast/status")
async def broadcast_status(request: Request):
    user = current_user(request)
    if not user:
        return JSONResponse({"active_broadcast": None}, status_code=401)
    return JSONResponse({"active_broadcast": get_active_broadcast_state()})

@app.post("/admin/broadcast/cancel")
async def cancel_broadcast(request: Request):
    redir = require_auth(request)
    if redir:
        return redir

    active_id = (get_setting(conn, "active_broadcast_id") or "").strip()
    if active_id:
        set_setting(conn, "active_broadcast_cancel", "1")
        # Best-effort: also cancel task if it's available
        task = getattr(app.state, "active_broadcast_task", None)
        if task and not task.done():
            task.cancel()

    return RedirectResponse(url="/admin/history", status_code=302)

@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request):
    init_db(conn)
    r = require_auth(request, roles=["admin"])
    if r:
        return r

    users = list_users(conn)
    return templates.TemplateResponse("users.html", {"request": request, "user": request.state.user, "users": users, "error": None, "ok": None, "active": "users"})


@app.post("/admin/users/create", response_class=HTMLResponse)
async def admin_users_create(request: Request):
    init_db(conn)
    r = require_auth(request, roles=["admin"])
    if r:
        return r

    form = await request.form()
    username = (form.get("username") or "").strip()
    password = (form.get("password") or "").strip()
    password2 = (form.get("password2") or "").strip()
    role = (form.get("role") or "manager").strip()

    # Basic validation
    if not username or not password:
        users = list_users(conn)
        return templates.TemplateResponse(
            "users.html",
            {
                "request": request,
                "user": request.state.user,
                "users": users,
                "error": "Укажи логин и пароль",
                "ok": None,
                "active": "users",
            },
        )
    if password != password2:
        users = list_users(conn)
        return templates.TemplateResponse(
            "users.html",
            {
                "request": request,
                "user": request.state.user,
                "users": users,
                "error": "Пароли не совпадают",
                "ok": None,
                "active": "users",
            },
        )

    try:
        create_user(conn, username, password, role=role)
        users = list_users(conn)
        return templates.TemplateResponse(
            "users.html",
            {
                "request": request,
                "user": request.state.user,
                "users": users,
                "error": None,
                "ok": "Пользователь создан",
                "active": "users",
            },
        )
    except Exception as e:
        users = list_users(conn)
        msg = "Не удалось создать пользователя"
        if "UNIQUE" in str(e).upper():
            msg = "Такой логин уже существует"
        return templates.TemplateResponse(
            "users.html",
            {
                "request": request,
                "user": request.state.user,
                "users": users,
                "error": msg,
                "ok": None,
                "active": "users",
            },
        )


@app.post("/admin/users/delete", response_class=HTMLResponse)
async def admin_users_delete(request: Request):
    init_db(conn)
    r = require_auth(request, roles=["admin"])
    if r:
        return r

    form = await request.form()
    user_id = int(form.get("user_id") or 0)
    if user_id == request.state.user["id"]:
        users = list_users(conn)
        return templates.TemplateResponse("users.html", {"request": request, "user": request.state.user, "users": users, "error": "Нельзя удалить текущего пользователя", "ok": None, "active": "users"})

    delete_user(conn, user_id)
    users = list_users(conn)
    return templates.TemplateResponse(
        "users.html",
        {
            "request": request,
            "user": request.state.user,
            "users": users,
            "error": None,
            "ok": "Пользователь удалён",
            "active": "users",
        },
    )


# --- DB backup routes ---
from .backup_routes import register_backup_routes
register_backup_routes(app, templates, conn, DB_PATH)
register_max_routes(app, templates)
register_max_backup_routes(app, templates, max_conn, MAX_DB_PATH)
