import os
import shutil
import sqlite3
from datetime import datetime

from fastapi import Request, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates

from .auth import get_session_data
from .db import get_user_by_id, get_setting

REQUIRED_TABLES = {
    "chats", "users", "broadcasts", "broadcast_files",
    "logs", "settings", "chat_groups", "chat_group_members",
}

def _require_admin(request: Request, conn):
    sess = get_session_data(request)
    if not sess:
        return RedirectResponse(url="/admin/login", status_code=302)
    user = get_user_by_id(conn, int(sess.get("uid")))
    if not user:
        return RedirectResponse(url="/admin/login", status_code=302)
    request.state.user = user
    if user.get("role") != "admin":
        return RedirectResponse(url="/admin/compose", status_code=302)
    return None

def _bot_token_status(conn):
    stored_token = (get_setting(conn, "bot_token") or "").strip()
    if not stored_token:
        return "не задан", False
    if len(stored_token) > 12:
        return stored_token[:6] + "…" + stored_token[-4:], True
    return "установлен", True

def register_backup_routes(app, templates: Jinja2Templates, conn, db_path: str):
    """
    Registers:
      - GET  /admin/settings/backup/download
      - POST /admin/settings/backup/upload
    Notes:
      - Upload triggers a process restart via os._exit(0) (expected to be restarted by systemd).
      - Intended for admin only.
    """

    @app.get("/admin/settings/backup/download")
    async def admin_backup_download(request: Request):
        r = _require_admin(request, conn)
        if r:
            return r

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        tmp_path = f"/tmp/tg_broadcast_backup_{ts}.db"

        # Create a consistent snapshot using sqlite backup API
        src = sqlite3.connect(db_path, check_same_thread=False)
        dst = sqlite3.connect(tmp_path)
        try:
            src.backup(dst)
        finally:
            try:
                dst.close()
            finally:
                src.close()

        bt_status, has_token = _bot_token_status(conn)
        # Serve file and remove it afterwards
        bg = BackgroundTasks()
        bg.add_task(lambda p=tmp_path: os.path.exists(p) and os.remove(p))
        return FileResponse(
            tmp_path,
            filename=f"tg_broadcast_backup_{ts}.db",
            media_type="application/octet-stream",
            background=bg,
        )

    @app.post("/admin/settings/backup/upload")
    async def admin_backup_upload(
        request: Request,
        background_tasks: BackgroundTasks,
        db_file: UploadFile = File(...),
        confirm: str = Form(""),
    ):
        r = _require_admin(request, conn)
        if r:
            return r

        bt_status, has_token = _bot_token_status(conn)

        if confirm != "yes":
            return templates.TemplateResponse(
                "settings.html",
                {
                    "request": request,
                    "user": request.state.user,
                    "is_admin": True,
                    "bot_token_status": bt_status,
                    "has_bot_token": has_token,
                    "ok": None,
                    "error": "Подтверди восстановление: поставь галочку перед загрузкой.",
                    "active": "settings",
                },
                status_code=400,
            )

        # Save upload to /tmp
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        tmp_path = f"/tmp/tg_broadcast_upload_{ts}.db"
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(db_file.file, f)

        # Validate SQLite file
        try:
            test = sqlite3.connect(tmp_path)
            try:
                chk = test.execute("PRAGMA quick_check;").fetchone()
                chk_val = (chk[0] if chk else "").lower()
                if chk_val != "ok":
                    raise RuntimeError(f"SQLite check failed: {chk_val}")

                tables = {row[0] for row in test.execute("SELECT name FROM sqlite_master WHERE type='table';")}
                missing = REQUIRED_TABLES - tables
                if missing:
                    raise RuntimeError("В бэкапе не хватает таблиц: " + ", ".join(sorted(missing)))
            finally:
                test.close()
        except Exception as e:
            # cleanup
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            return templates.TemplateResponse(
                "settings.html",
                {
                    "request": request,
                    "user": request.state.user,
                    "is_admin": True,
                    "bot_token_status": bt_status,
                    "has_bot_token": has_token,
                    "ok": None,
                    "error": f"Файл бэкапа не принят: {e}",
                    "active": "settings",
                },
                status_code=400,
            )

        # Backup current DB on disk (best-effort)
        try:
            if os.path.exists(db_path):
                shutil.copy2(db_path, f"{db_path}.bak_{ts}")
        except Exception:
            # ignore backup failures
            pass

        # Replace DB file
        try:
            os.replace(tmp_path, db_path)
        except Exception as e:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            return templates.TemplateResponse(
                "settings.html",
                {
                    "request": request,
                    "user": request.state.user,
                    "is_admin": True,
                    "bot_token_status": bt_status,
                    "has_bot_token": has_token,
                    "ok": None,
                    "error": f"Не удалось применить бэкап: {e}",
                    "active": "settings",
                },
                status_code=500,
            )

        # Restart process after response (systemd should bring it back)
        background_tasks.add_task(lambda: os._exit(0))

        return templates.TemplateResponse(
            "settings.html",
            {
                "request": request,
                "user": request.state.user,
                "is_admin": True,
                "bot_token_status": bt_status,
                "has_bot_token": has_token,
                "ok": "Бэкап загружен и применён. Сервис перезапускается… (страницу можно обновить через 5–10 сек).",
                "error": None,
                "active": "settings",
            },
        )
