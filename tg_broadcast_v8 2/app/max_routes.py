import asyncio
import mimetypes
import os
import re
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import List

from fastapi import Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from io import BytesIO

from .auth import get_session_data
from .db import connect, init_db, list_chats, set_chat_blocked, delete_chat, create_broadcast, add_broadcast_file, get_broadcast_files, log_send, list_logs, list_broadcast_summaries, get_broadcast_summary, list_broadcast_logs, list_groups, create_group, delete_group, get_group, set_group_members, list_group_members, get_group_chat_ids, get_user_by_id, get_setting, set_setting
from .max_bot import run_max_polling, send_message, max_get_me, sync_max_chats

BASE_DIR = Path(__file__).resolve().parent
MAX_DB_PATH = os.getenv("MAX_DB_PATH", str(BASE_DIR / "app_max.db"))
MAIN_DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "app.db"))
MAX_UPLOAD_DIR = Path(os.getenv("MAX_UPLOAD_DIR", str(BASE_DIR / "uploads_max")))
MAX_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
max_conn = connect(MAX_DB_PATH)
main_auth_conn = connect(MAIN_DB_PATH)
init_db(max_conn)

def fmt_dt(value: str) -> str:
    from datetime import datetime, timedelta, timezone
    if not value: return ""
    msk = timezone(timedelta(hours=3))
    try: dt = datetime.fromisoformat(value.replace("Z", ""))
    except Exception:
        try: dt = datetime.fromisoformat(value.replace(" ", "T"))
        except Exception: return str(value)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(msk).strftime("%d.%m.%Y %H:%M:%S")

def msk_date_to_utc_iso(date_str: str, end: bool = False) -> str:
    from datetime import datetime, timedelta
    if not date_str: return None
    try: d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception: return None
    msk_dt = datetime(d.year, d.month, d.day, 23,59,59,999999) if end else datetime(d.year, d.month, d.day, 0,0,0,0)
    return (msk_dt - timedelta(hours=3)).isoformat()


def strip_html_text(value: str) -> str:
    if not value: return ""
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def build_broadcast_preview(text_value: str, html_value: str, limit: int = 140) -> str:
    source = (text_value or "").strip() or strip_html_text(html_value or "")
    source = re.sub(r"\s+", " ", source).strip()
    if not source: return "Без текста"
    return source if len(source) <= limit else (source[:limit - 1].rstrip() + "…")


def summarize_broadcast_row(row: dict, active_broadcast_id: str = "") -> dict:
    item = dict(row)
    bid = str(item.get("broadcast_id", "") or "")
    item["created_at_fmt"] = fmt_dt(item.get("created_at", ""))
    item["last_event_at_fmt"] = fmt_dt(item.get("last_event_at", ""))
    item["message_preview"] = build_broadcast_preview(item.get("text", ""), item.get("html", ""))
    total_chat_rows = int(item.get("total_chat_rows") or 0)
    item["recipient_count"] = total_chat_rows
    if int(item.get("cancelled_count") or 0) > 0:
        item["status_key"] = "cancelled"; item["status_label"] = "Отменена"
    elif int(item.get("service_error_count") or 0) > 0 and total_chat_rows == 0:
        item["status_key"] = "error"; item["status_label"] = "Ошибка"
    elif active_broadcast_id and bid == str(active_broadcast_id):
        item["status_key"] = "running"; item["status_label"] = "Выполняется"
    else:
        item["status_key"] = "done"; item["status_label"] = "Завершена"
    ok_count = int(item.get("ok_count") or 0)
    item["success_rate"] = round((ok_count / total_chat_rows) * 100, 1) if total_chat_rows else 0
    return item

def current_user(request: Request):
    sess = get_session_data(request)
    if not sess:
        return None
    return get_user_by_id(main_auth_conn, int(sess.get("uid")))

def require_auth(request: Request, roles=None):
    user = current_user(request)
    if not user: return RedirectResponse(url="/admin/login", status_code=302)
    if roles and user.get("role") not in roles: return RedirectResponse(url="/admin/max/compose", status_code=302)
    request.state.user = user
    return None

def route_ctx(request: Request, **extra):
    base = {"request": request, "user": request.state.user, "route_base": "/admin/max", "platform": "max", "platform_name": "MAX", "active_platform": "max"}
    base.update(extra)
    return base

def get_active_broadcast_state():
    bid = (get_setting(max_conn, "active_broadcast_id") or "").strip()
    if not bid: return None
    def _int(key: str, default: int = 0):
        try: return int((get_setting(max_conn, key) or str(default)).strip())
        except Exception: return default
    return {"id": bid, "started_at": (get_setting(max_conn, "active_broadcast_started_at") or "").strip(), "total": _int("active_broadcast_total"), "ok": _int("active_broadcast_ok"), "err": _int("active_broadcast_err"), "skipped": _int("active_broadcast_skipped"), "cancel": ((get_setting(max_conn, "active_broadcast_cancel") or "0").strip() == "1")}

async def _broadcast_worker(bid: int, final_ids: List[int], bfiles: list, msg_html: str, msg_plain: str):
    token = (get_setting(max_conn, "max_bot_token") or "").strip()
    cancelled = False
    ok = err = 0
    try:
        for cid in final_ids:
            if (get_setting(max_conn, "active_broadcast_cancel") or "0").strip() == "1":
                cancelled = True; break
            try:
                send_message(token, cid, text=msg_plain, html=msg_html, file_paths=[dict(x) for x in bfiles])
                log_send(max_conn, bid, cid, "OK", "")
                ok += 1; set_setting(max_conn, "active_broadcast_ok", str(ok))
            except Exception as e:
                log_send(max_conn, bid, cid, "ERROR", str(e)[:1000])
                err += 1; set_setting(max_conn, "active_broadcast_err", str(err))
            await asyncio.sleep(0.35)
    except asyncio.CancelledError:
        cancelled = True; raise
    finally:
        set_setting(max_conn, "active_broadcast_id", "")
        set_setting(max_conn, "active_broadcast_cancel", "0")
        set_setting(max_conn, "active_broadcast_started_at", "")
        if cancelled:
            log_send(max_conn, bid, 0, "CANCELLED", "Рассылка MAX остановлена пользователем")
        else:
            log_send(max_conn, bid, 0, "DONE", f"Завершено. OK={ok}, ERR={err}")

def start_max_polling(app):
    stop_event = threading.Event()
    thread = threading.Thread(target=run_max_polling, args=(max_conn, stop_event), daemon=True)
    thread.start()
    app.state.max_polling_stop = stop_event
    app.state.max_polling_thread = thread

def register_max_routes(app, templates: Jinja2Templates):
    @app.get("/admin/max/compose", response_class=HTMLResponse)
    async def max_compose(request: Request, err: str = ""):
        r = require_auth(request)
        if r: return r
        error = "Выберите хотя бы один чат или одну группировку." if err == "select" else ("Сейчас уже выполняется рассылка MAX. Дождитесь завершения или отмените её." if err == "running" else None)
        return templates.TemplateResponse("compose.html", route_ctx(request, chats=list_chats(max_conn, include_blocked=True), groups=list_groups(max_conn), active_broadcast=get_active_broadcast_state(), error=error, active="compose", title="Рассылка MAX"))

    @app.post("/admin/max/send")
    async def max_send(request: Request, html: str = Form(""), text: str = Form(""), group_ids: List[int] = Form([]), chat_ids: List[int] = Form([]), files: List[UploadFile] = File(default=[])):
        r = require_auth(request)
        if r: return r
        if not (chat_ids or group_ids): return RedirectResponse(url="/admin/max/compose?err=select", status_code=302)
        if (get_setting(max_conn, "active_broadcast_id") or "").strip(): return RedirectResponse(url="/admin/max/compose?err=running", status_code=302)
        resolved = set(int(cid) for cid in (chat_ids or []))
        for gid in (group_ids or []):
            for cid in get_group_chat_ids(max_conn, int(gid)): resolved.add(int(cid))
        blocked_ids = set(int(r["chat_id"]) for r in list_chats(max_conn, include_blocked=True) if int(r["blocked"] or 0) == 1)
        final_ids = [cid for cid in sorted(resolved) if cid not in blocked_ids]
        skipped = [cid for cid in sorted(resolved) if cid in blocked_ids]
        if not final_ids:
            bid_tmp = create_broadcast(max_conn, html=html, text=text, created_by=request.state.user.get("username", ""))
            for cid in skipped: log_send(max_conn, bid_tmp, cid, "SKIPPED", "chat is blocked in panel")
            log_send(max_conn, bid_tmp, 0, "ERROR", "No available MAX chats (all selected are blocked)")
            return RedirectResponse(url="/admin/max/history", status_code=302)
        bid = create_broadcast(max_conn, html=html, text=text, created_by=request.state.user.get("username", ""))
        for cid in skipped: log_send(max_conn, bid, cid, "SKIPPED", "chat is blocked in panel")
        for f in files or []:
            if not f.filename: continue
            safe_name = f"{bid}__{Path(f.filename).name}"
            dest = MAX_UPLOAD_DIR / safe_name
            with dest.open("wb") as out: shutil.copyfileobj(f.file, out)
            mime = f.content_type or mimetypes.guess_type(str(dest))[0] or "application/octet-stream"
            add_broadcast_file(max_conn, bid, str(dest), Path(f.filename).name, mime)
        msg_plain = (text or "").strip(); msg_html = (html or "").strip(); bfiles = get_broadcast_files(max_conn, bid)
        set_setting(max_conn, "active_broadcast_id", str(bid)); set_setting(max_conn, "active_broadcast_cancel", "0"); set_setting(max_conn, "active_broadcast_started_at", datetime.utcnow().isoformat()); set_setting(max_conn, "active_broadcast_total", str(len(final_ids))); set_setting(max_conn, "active_broadcast_ok", "0"); set_setting(max_conn, "active_broadcast_err", "0"); set_setting(max_conn, "active_broadcast_skipped", str(len(skipped)))
        app.state.max_active_broadcast_task = asyncio.create_task(_broadcast_worker(bid, final_ids, bfiles, msg_html, msg_plain))
        return RedirectResponse(url="/admin/max/history", status_code=302)

    @app.get("/admin/max/history", response_class=HTMLResponse)
    async def max_history(request: Request, date_from: str = "", date_to: str = ""):
        r = require_auth(request)
        if r: return r
        dt_from = msk_date_to_utc_iso((date_from or "").strip(), end=False); dt_to = msk_date_to_utc_iso((date_to or "").strip(), end=True)
        active = get_active_broadcast_state()
        active_id = str(active.get("id")) if active else ""
        broadcasts = [summarize_broadcast_row(dict(row), active_id) for row in list_broadcast_summaries(max_conn, 500, dt_from=dt_from, dt_to=dt_to)]
        return templates.TemplateResponse("history.html", route_ctx(request, broadcasts=broadcasts, date_from=date_from, date_to=date_to, active_broadcast=active, active="history", title="История MAX"))

    @app.get("/admin/max/history/{broadcast_id}", response_class=HTMLResponse)
    async def max_history_detail(request: Request, broadcast_id: int):
        r = require_auth(request)
        if r: return r
        summary_row = get_broadcast_summary(max_conn, broadcast_id)
        if not summary_row: return RedirectResponse(url="/admin/max/history", status_code=302)
        active = get_active_broadcast_state()
        active_id = str(active.get("id")) if active else ""
        broadcast = summarize_broadcast_row(dict(summary_row), active_id)
        broadcast["message_full"] = (broadcast.get("text") or "").strip() or strip_html_text(broadcast.get("html") or "") or "—"
        broadcast["files"] = [dict(x) for x in get_broadcast_files(max_conn, broadcast_id)]
        logs = [dict(l) for l in list_broadcast_logs(max_conn, broadcast_id, 10000)]
        for l in logs:
            l["created_at_fmt"] = fmt_dt(l.get("created_at", ""))
            l["is_service"] = int(l.get("chat_id") or 0) == 0
        return templates.TemplateResponse("history_detail.html", route_ctx(request, broadcast=broadcast, logs=logs, active_broadcast=active, active="history", title=f"Рассылка MAX #{broadcast_id}"))

    @app.get("/admin/max/history/export")
    async def max_history_export(request: Request, date_from: str = "", date_to: str = ""):
        user = current_user(request)
        if not user: return RedirectResponse(url="/admin/login", status_code=302)
        dt_from = msk_date_to_utc_iso((date_from or "").strip(), end=False); dt_to = msk_date_to_utc_iso((date_to or "").strip(), end=True)
        active = get_active_broadcast_state()
        active_id = str(active.get("id")) if active else ""
        rows = [summarize_broadcast_row(dict(r), active_id) for r in list_broadcast_summaries(max_conn, 100000, dt_from=dt_from, dt_to=dt_to)]
        wb = Workbook(); ws = wb.active; ws.title = "History MAX"
        ws.append(["Рассылка", "Создана (МСК)", "Отправитель", "Статус", "Получателей", "OK", "Ошибок", "Пропущено", "Файлов", "Текст"])
        for r in rows:
            ws.append([r.get("broadcast_id", ""), r.get("created_at_fmt", ""), r.get("broadcast_user", "") or "", r.get("status_label", ""), r.get("recipient_count", 0), r.get("ok_count", 0), r.get("error_count", 0), r.get("skipped_count", 0), r.get("file_count", 0), r.get("message_preview", "")])
        bio = BytesIO(); wb.save(bio); bio.seek(0)
        return StreamingResponse(bio, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": 'attachment; filename="max_history.xlsx"'})

    @app.get("/admin/max/history/{broadcast_id}/export")
    async def max_history_detail_export(request: Request, broadcast_id: int):
        user = current_user(request)
        if not user: return RedirectResponse(url="/admin/login", status_code=302)
        summary_row = get_broadcast_summary(max_conn, broadcast_id)
        if not summary_row: return RedirectResponse(url="/admin/max/history", status_code=302)
        rows = [dict(l) for l in list_broadcast_logs(max_conn, broadcast_id, 100000)]
        for r in rows:
            r["created_at_fmt"] = fmt_dt(r.get("created_at", ""))
            r["is_service"] = int(r.get("chat_id") or 0) == 0
        wb = Workbook(); ws = wb.active; ws.title = f"Broadcast {broadcast_id}"
        ws.append(["Время (МСК)", "Рассылка", "Chat ID", "Чат", "Статус", "Детали"])
        for r in rows:
            ws.append([r.get("created_at_fmt", ""), broadcast_id, "" if r.get("is_service") else r.get("chat_id", ""), "Служебно" if r.get("is_service") else (r.get("chat_title", "") or ""), r.get("status", "") or "", r.get("details", "") or ""])
        bio = BytesIO(); wb.save(bio); bio.seek(0)
        return StreamingResponse(bio, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f'attachment; filename="max_history_{broadcast_id}.xlsx"'})

    @app.get("/admin/max/chats", response_class=HTMLResponse)
    async def max_chats(request: Request):
        r = require_auth(request)
        if r: return r
        return templates.TemplateResponse("chats.html", route_ctx(request, chats=list_chats(max_conn, include_blocked=True), active="chats", title="Чаты MAX"))

    @app.post("/admin/max/chats/block")
    async def max_chat_block(request: Request, chat_id: int = Form(...), blocked: int = Form(...)):
        r = require_auth(request, roles=["admin"])
        if r: return r
        set_chat_blocked(max_conn, int(chat_id), bool(int(blocked)))
        return RedirectResponse(url="/admin/max/chats", status_code=302)

    @app.post("/admin/max/chats/delete")
    async def max_chat_delete(request: Request, chat_id: int = Form(...)):
        r = require_auth(request, roles=["admin"])
        if r: return r
        delete_chat(max_conn, int(chat_id))
        return RedirectResponse(url="/admin/max/chats", status_code=302)

    @app.post("/admin/max/chats/sync")
    async def max_chat_sync(request: Request):
        r = require_auth(request, roles=["admin"])
        if r: return r
        try: sync_max_chats(max_conn)
        except Exception: pass
        return RedirectResponse(url="/admin/max/chats", status_code=302)

    @app.get("/admin/max/groups", response_class=HTMLResponse)
    async def max_groups(request: Request):
        r = require_auth(request)
        if r: return r
        return templates.TemplateResponse("groups.html", route_ctx(request, groups=list_groups(max_conn), chats=list_chats(max_conn, include_blocked=True), active="groups", title="Группировки MAX"))

    @app.post("/admin/max/groups/create")
    async def max_groups_create(request: Request, name: str = Form(...)):
        r = require_auth(request)
        if r: return r
        if (name or "").strip(): create_group(max_conn, name.strip())
        return RedirectResponse(url="/admin/max/groups", status_code=302)

    @app.post("/admin/max/groups/delete")
    async def max_groups_delete(request: Request, group_id: int = Form(...)):
        r = require_auth(request, roles=["admin"])
        if r: return r
        delete_group(max_conn, int(group_id))
        return RedirectResponse(url="/admin/max/groups", status_code=302)

    @app.get("/admin/max/groups/edit", response_class=HTMLResponse)
    async def max_group_edit(request: Request, group_id: int):
        r = require_auth(request)
        if r: return r
        group = get_group(max_conn, int(group_id))
        if not group: return RedirectResponse(url="/admin/max/groups", status_code=302)
        members = set(int(r["chat_id"]) for r in list_group_members(max_conn, int(group_id)))
        return templates.TemplateResponse("group_edit.html", route_ctx(request, group=group, chats=list_chats(max_conn, include_blocked=True), members=members, active="groups", title="Редактирование MAX"))

    @app.post("/admin/max/groups/edit")
    async def max_group_edit_save(request: Request, group_id: int = Form(...), chat_ids: List[int] = Form([])):
        r = require_auth(request)
        if r: return r
        set_group_members(max_conn, int(group_id), chat_ids or [])
        return RedirectResponse(url=f"/admin/max/groups/edit?group_id={int(group_id)}", status_code=302)

    @app.post("/admin/max/broadcast/cancel")
    async def max_cancel_broadcast(request: Request):
        r = require_auth(request)
        if r: return r
        if (get_setting(max_conn, "active_broadcast_id") or "").strip():
            set_setting(max_conn, "active_broadcast_cancel", "1")
            task = getattr(app.state, "max_active_broadcast_task", None)
            if task and not task.done(): task.cancel()
        return RedirectResponse(url="/admin/max/history", status_code=302)

    @app.get("/admin/max/broadcast/status")
    async def max_broadcast_status(request: Request):
        user = current_user(request)
        if not user: return JSONResponse({"active_broadcast": None}, status_code=401)
        return JSONResponse({"active_broadcast": get_active_broadcast_state()})

    @app.get("/admin/max/settings", response_class=HTMLResponse)
    async def max_settings(request: Request):
        r = require_auth(request)
        if r: return r
        stored_token = (get_setting(max_conn, "max_bot_token") or "").strip()
        bot_token_status = "не задан" if not stored_token else (stored_token[:6] + "…" + stored_token[-4:] if len(stored_token) > 12 else "установлен")
        bot_info = None
        if stored_token:
            try: bot_info = max_get_me(max_conn)
            except Exception: bot_info = None
        return templates.TemplateResponse("settings.html", route_ctx(request, is_admin=request.state.user["role"] == "admin", bot_token_status=bot_token_status, has_bot_token=bool(stored_token), ok=None, error=None, active="settings", bot_label="Токен MAX-бота", bot_form_action="/admin/max/settings/bot_token", backup_download_url="/admin/max/settings/backup/download", backup_upload_url="/admin/max/settings/backup/upload", bot_restart_hint="Для MAX long polling запускается внутри сервиса. После применения токена он начнет использоваться без изменения Telegram-части.", bot_info=bot_info, title="Настройки MAX"))

    @app.post("/admin/max/settings/bot_token", response_class=HTMLResponse)
    async def max_settings_token(request: Request):
        r = require_auth(request, roles=["admin"])
        if r: return r
        form = await request.form(); token1 = (form.get("bot_token") or "").strip(); token2 = (form.get("bot_token2") or "").strip()
        stored_token = (get_setting(max_conn, "max_bot_token") or "").strip()
        bot_token_status = "не задан" if not stored_token else (stored_token[:6] + "…" + stored_token[-4:] if len(stored_token) > 12 else "установлен")
        common = dict(is_admin=True, bot_token_status=bot_token_status, has_bot_token=bool(stored_token), active="settings", bot_label="Токен MAX-бота", bot_form_action="/admin/max/settings/bot_token", backup_download_url="/admin/max/settings/backup/download", backup_upload_url="/admin/max/settings/backup/upload", bot_restart_hint="Для MAX long polling запускается внутри сервиса. После применения токена он начнет использоваться без изменения Telegram-части.")
        if not token1 or not token2: return templates.TemplateResponse("settings.html", route_ctx(request, ok=None, error="Токен не может быть пустым", **common))
        if token1 != token2: return templates.TemplateResponse("settings.html", route_ctx(request, ok=None, error="Токены не совпадают", **common))
        set_setting(max_conn, "max_bot_token", token1)
        try:
            max_get_me(max_conn); ok = "Токен MAX применён."
        except Exception as e:
            ok = f"Токен сохранён, но проверка API вернула ошибку: {str(e)[:300]}"
        stored_token = (get_setting(max_conn, "max_bot_token") or "").strip()
        bot_token_status = "не задан" if not stored_token else (stored_token[:6] + "…" + stored_token[-4:] if len(stored_token) > 12 else "установлен")
        common.update(bot_token_status=bot_token_status, has_bot_token=bool(stored_token))
        return templates.TemplateResponse("settings.html", route_ctx(request, ok=ok, error=None, **common))
