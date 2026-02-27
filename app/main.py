import os, shutil, mimetypes, asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from aiogram.types import FSInputFile

from .db import (
    connect, init_db,
    list_chats, set_chat_blocked,
    create_broadcast, add_broadcast_file, get_broadcast_files,
    log_send, list_logs,
    list_groups, create_group, delete_group, get_group,
    set_group_members, list_group_members, get_group_chat_ids,
    get_user_by_id, get_user_by_username, list_users, create_user, delete_user, update_user_password,
    get_setting, set_setting,
)
from .auth import verify_password, set_session_cookie, clear_session_cookie, get_session_data
from .bot import run_bot_polling, make_bot

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "app.db"))

app = FastAPI()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

conn = connect(DB_PATH)
init_db(conn)

# NOTE: init_db() already ensures the required schema and creates default users.
# A previous version referenced ensure_password_hash(), but it is not needed.

@app.on_event("startup")
async def startup():
    # Start Telegram bot polling in background
    app.state.bot_task_lock = asyncio.Lock()
    app.state.bot_task = asyncio.create_task(run_bot_polling(conn))

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

def fmt_dt(value: str) -> str:
    """Format stored datetime as Moscow time (MSK) in dd.mm.yyyy HH:MM:SS."""
    if not value:
        return ""
    msk = timezone(timedelta(hours=3))
    try:
        v = value.replace("Z", "")
        dt = datetime.fromisoformat(v)
    except Exception:
        try:
            # handle already space separated
            dt = datetime.fromisoformat(value.replace(" ", "T"))
        except Exception:
            return value

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

@app.get("/admin/compose", response_class=HTMLResponse)
async def compose_page(request: Request):
    redir = require_auth(request)
    if redir: return redir
    return templates.TemplateResponse("compose.html", {"request": request, "user": request.state.user,
        "chats": list_chats(conn, include_blocked=True),
        "groups": list_groups(conn),
        "active": "compose",
        "title": "Рассылка"
    })

@app.get("/admin/history", response_class=HTMLResponse)
async def history_page(request: Request):
    redir = require_auth(request)
    if redir: return redir
    logs = [dict(l) for l in list_logs(conn, 300)]
    for l in logs:
        l["created_at_fmt"] = fmt_dt(l.get("created_at",""))
    return templates.TemplateResponse("history.html", {"request": request, "user": request.state.user,
        "logs": logs,
        "active": "history",
        "title": "История отправки"
    })

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
    if redir: return redir

    # Resolve recipients
    resolved = set(int(cid) for cid in (chat_ids or []))
    for gid in (group_ids or []):
        for cid in get_group_chat_ids(conn, int(gid)):
            resolved.add(int(cid))

    # If none selected => all non-blocked chats
    if not resolved:
        resolved = set(int(r["chat_id"]) for r in list_chats(conn, include_blocked=False))

    # Filter out blocked chats
    blocked_ids = set(int(r["chat_id"]) for r in list_chats(conn, include_blocked=True) if int(r["blocked"] or 0) == 1)
    final_ids = [cid for cid in sorted(resolved) if cid not in blocked_ids]

    bid = create_broadcast(conn, html=html, text=text)

    for f in files or []:
        if not f.filename:
            continue
        safe_name = f"{bid}__{Path(f.filename).name}"
        dest = UPLOAD_DIR / safe_name
        with dest.open("wb") as out:
            shutil.copyfileobj(f.file, out)
        mime = f.content_type or mimetypes.guess_type(str(dest))[0] or "application/octet-stream"
        add_broadcast_file(conn, bid, str(dest), Path(f.filename).name, mime)

    bot = make_bot(conn=conn)
    msg = (text or "").strip()
    bfiles = get_broadcast_files(conn, bid)

    # Log skipped blocked recipients (if explicitly selected)
    skipped = [cid for cid in sorted(resolved) if cid in blocked_ids]
    for cid in skipped:
        log_send(conn, bid, cid, "SKIPPED", "chat is blocked in panel")

    for cid in final_ids:
        try:
            if msg:
                await bot.send_message(cid, msg)
            for bf in bfiles:
                path = bf["file_path"]
                name = bf["file_name"]
                mime = (bf["mime_type"] or "").lower()
                if mime.startswith("image/"):
                    await bot.send_photo(cid, photo=FSInputFile(path, filename=name))
                else:
                    await bot.send_document(cid, document=FSInputFile(path, filename=name))
            log_send(conn, bid, cid, "OK", "")
        except Exception as e:
            log_send(conn, bid, cid, "ERROR", str(e)[:1000])

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

