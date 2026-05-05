import os
import shutil
import sqlite3
from datetime import datetime

from fastapi import Request, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates

from .auth import get_session_data
from .db import get_user_by_id, get_setting, connect

REQUIRED_TABLES = {"chats", "users", "broadcasts", "broadcast_files", "logs", "settings", "chat_groups", "chat_group_members"}

MAIN_DB_PATH = os.getenv("DB_PATH", "app/app.db")
main_auth_conn = connect(MAIN_DB_PATH)

def _require_admin(request: Request, conn):
    sess = get_session_data(request)
    if not sess: return RedirectResponse(url="/admin/login", status_code=302)
    user = get_user_by_id(main_auth_conn, int(sess.get("uid")))
    if not user: return RedirectResponse(url="/admin/login", status_code=302)
    request.state.user = user
    if user.get("role") != "admin": return RedirectResponse(url="/admin/max/compose", status_code=302)
    return None

def _bot_token_status(conn):
    stored_token = (get_setting(conn, "max_bot_token") or "").strip()
    if not stored_token: return "не задан", False
    if len(stored_token) > 12: return stored_token[:6] + "…" + stored_token[-4:], True
    return "установлен", True

def register_max_backup_routes(app, templates: Jinja2Templates, conn, db_path: str):
    @app.get("/admin/max/settings/backup/download")
    async def max_backup_download(request: Request):
        r = _require_admin(request, conn)
        if r: return r
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        tmp_path = f"/tmp/max_broadcast_backup_{ts}.db"
        src = sqlite3.connect(db_path, check_same_thread=False); dst = sqlite3.connect(tmp_path)
        try: src.backup(dst)
        finally:
            try: dst.close()
            finally: src.close()
        bg = BackgroundTasks(); bg.add_task(lambda p=tmp_path: os.path.exists(p) and os.remove(p))
        return FileResponse(tmp_path, filename=f"max_broadcast_backup_{ts}.db", media_type="application/octet-stream", background=bg)

    @app.post("/admin/max/settings/backup/upload")
    async def max_backup_upload(request: Request, background_tasks: BackgroundTasks, db_file: UploadFile = File(...), confirm: str = Form("")):
        r = _require_admin(request, conn)
        if r: return r
        bt_status, has_token = _bot_token_status(conn)
        common = {"request": request, "user": request.state.user, "route_base": "/admin/max", "platform": "max", "platform_name": "MAX", "active_platform": "max", "is_admin": True, "bot_token_status": bt_status, "has_bot_token": has_token, "active": "settings", "bot_label": "Токен MAX-бота", "bot_form_action": "/admin/max/settings/bot_token", "backup_download_url": "/admin/max/settings/backup/download", "backup_upload_url": "/admin/max/settings/backup/upload", "bot_restart_hint": "После восстановления MAX-база будет заменена. Telegram-база не изменится."}
        if confirm != "yes": return templates.TemplateResponse("settings.html", {**common, "ok": None, "error": "Подтверди восстановление: поставь галочку перед загрузкой."}, status_code=400)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S"); tmp_path = f"/tmp/max_broadcast_upload_{ts}.db"
        with open(tmp_path, "wb") as f: shutil.copyfileobj(db_file.file, f)
        try:
            test = sqlite3.connect(tmp_path)
            try:
                chk = test.execute("PRAGMA quick_check;").fetchone(); chk_val = (chk[0] if chk else "").lower()
                if chk_val != "ok": raise RuntimeError(f"SQLite check failed: {chk_val}")
                tables = {row[0] for row in test.execute("SELECT name FROM sqlite_master WHERE type='table';")}
                missing = REQUIRED_TABLES - tables
                if missing: raise RuntimeError("В бэкапе не хватает таблиц: " + ", ".join(sorted(missing)))
            finally: test.close()
        except Exception as e:
            try: os.remove(tmp_path)
            except Exception: pass
            return templates.TemplateResponse("settings.html", {**common, "ok": None, "error": f"Файл бэкапа не принят: {e}"}, status_code=400)
        try:
            if os.path.exists(db_path): shutil.copy2(db_path, f"{db_path}.bak_{ts}")
        except Exception: pass
        try: os.replace(tmp_path, db_path)
        except Exception as e:
            try: os.remove(tmp_path)
            except Exception: pass
            return templates.TemplateResponse("settings.html", {**common, "ok": None, "error": f"Не удалось применить бэкап: {e}"}, status_code=500)
        background_tasks.add_task(lambda: os._exit(0))
        return templates.TemplateResponse("settings.html", {**common, "ok": "MAX-бэкап загружен и применён. Сервис перезапускается…", "error": None})
