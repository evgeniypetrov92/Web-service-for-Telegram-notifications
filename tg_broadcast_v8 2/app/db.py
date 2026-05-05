import sqlite3
import os
from datetime import datetime

def connect(db_path: str):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _has_column(conn, table: str, col: str) -> bool:
    rows = conn.cursor().execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS chats (
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        chat_type TEXT,
        added_at TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        created_by TEXT,
        html TEXT,
        text TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS broadcast_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        broadcast_id INTEGER,
        file_path TEXT,
        file_name TEXT,
        mime_type TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        broadcast_id INTEGER,
        chat_id INTEGER,
        status TEXT,
        details TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS chat_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        created_at TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS chat_group_members (
        group_id INTEGER NOT NULL,
        chat_id INTEGER NOT NULL,
        PRIMARY KEY(group_id, chat_id)
    );""")
    init_users(conn)
    _ensure_default_admin(conn)

    conn.commit()

    # lightweight migration
    if _has_column(conn, "chats", "blocked") is False:
        conn.cursor().execute("ALTER TABLE chats ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    if _has_column(conn, "broadcasts", "created_by") is False:
        conn.cursor().execute("ALTER TABLE broadcasts ADD COLUMN created_by TEXT")
        conn.commit()


def upsert_chat(conn, chat_id: int, title: str, chat_type: str):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO chats(chat_id, title, chat_type, added_at, blocked)
    VALUES(?,?,?,?,COALESCE((SELECT blocked FROM chats WHERE chat_id=?), 0))
    ON CONFLICT(chat_id) DO UPDATE
      SET title=excluded.title, chat_type=excluded.chat_type, added_at=excluded.added_at;
    """, (chat_id, title, chat_type, datetime.utcnow().isoformat(), chat_id))
    conn.commit()

def list_chats(conn, include_blocked=True):
    if include_blocked:
        return conn.cursor().execute("SELECT * FROM chats ORDER BY added_at DESC").fetchall()
    return conn.cursor().execute("SELECT * FROM chats WHERE blocked=0 ORDER BY added_at DESC").fetchall()

def set_chat_blocked(conn, chat_id: int, blocked: bool):
    conn.cursor().execute("UPDATE chats SET blocked=? WHERE chat_id=?", (1 if blocked else 0, int(chat_id)))
    conn.commit()

def delete_chat(conn, chat_id: int):
    """Delete chat from directory.

    Also removes memberships in chat_group_members.
    We intentionally keep historical logs/broadcasts (logs are not FK constrained).
    """
    cur = conn.cursor()
    cid = int(chat_id)
    cur.execute("DELETE FROM chat_group_members WHERE chat_id=?", (cid,))
    cur.execute("DELETE FROM chats WHERE chat_id=?", (cid,))
    conn.commit()

def create_broadcast(conn, html: str, text: str, created_by: str = ""):
    cur = conn.cursor()
    cur.execute("INSERT INTO broadcasts(created_at, created_by, html, text) VALUES(?,?,?,?)", (datetime.utcnow().isoformat(), created_by, html, text))
    conn.commit()
    return cur.lastrowid

def add_broadcast_file(conn, broadcast_id: int, file_path: str, file_name: str, mime_type: str):
    cur = conn.cursor()
    cur.execute("INSERT INTO broadcast_files(broadcast_id, file_path, file_name, mime_type) VALUES(?,?,?,?)", (broadcast_id, file_path, file_name, mime_type))
    conn.commit()

def get_broadcast_files(conn, broadcast_id: int):
    return conn.cursor().execute("SELECT * FROM broadcast_files WHERE broadcast_id=? ORDER BY id", (broadcast_id,)).fetchall()

def log_send(conn, broadcast_id: int, chat_id: int, status: str, details: str = ""):
    cur = conn.cursor()
    cur.execute("INSERT INTO logs(created_at, broadcast_id, chat_id, status, details) VALUES(?,?,?,?,?)",
                (datetime.utcnow().isoformat(), broadcast_id, chat_id, status, details))
    conn.commit()

def list_logs(conn, limit: int = 200, dt_from: str = None, dt_to: str = None):
    sql = """
      SELECT l.*, c.title AS chat_title, b.created_by AS broadcast_user
      FROM logs l
      LEFT JOIN chats c ON c.chat_id=l.chat_id
      LEFT JOIN broadcasts b ON b.id=l.broadcast_id
      WHERE 1=1
    """
    params = []
    if dt_from:
        sql += " AND l.created_at >= ?"
        params.append(dt_from)
    if dt_to:
        sql += " AND l.created_at <= ?"
        params.append(dt_to)

    sql += " ORDER BY l.id DESC LIMIT ?"
    params.append(limit)

    return conn.cursor().execute(sql, tuple(params)).fetchall()



def list_broadcast_summaries(conn, limit: int = 200, dt_from: str = None, dt_to: str = None):
    sql = """
      SELECT
        b.id AS broadcast_id,
        b.created_at,
        b.created_by AS broadcast_user,
        b.html,
        b.text,
        COALESCE(fs.file_count, 0) AS file_count,
        COALESCE(ls.ok_count, 0) AS ok_count,
        COALESCE(ls.error_count, 0) AS error_count,
        COALESCE(ls.skipped_count, 0) AS skipped_count,
        COALESCE(ls.total_chat_rows, 0) AS total_chat_rows,
        COALESCE(ls.service_error_count, 0) AS service_error_count,
        COALESCE(ls.cancelled_count, 0) AS cancelled_count,
        COALESCE(ls.done_count, 0) AS done_count,
        COALESCE(ls.last_event_at, b.created_at) AS last_event_at
      FROM broadcasts b
      LEFT JOIN (
        SELECT
          broadcast_id,
          SUM(CASE WHEN chat_id != 0 AND UPPER(COALESCE(status, '')) = 'OK' THEN 1 ELSE 0 END) AS ok_count,
          SUM(CASE WHEN chat_id != 0 AND UPPER(COALESCE(status, '')) = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
          SUM(CASE WHEN chat_id != 0 AND UPPER(COALESCE(status, '')) = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped_count,
          SUM(CASE WHEN chat_id != 0 THEN 1 ELSE 0 END) AS total_chat_rows,
          SUM(CASE WHEN chat_id = 0 AND UPPER(COALESCE(status, '')) = 'ERROR' THEN 1 ELSE 0 END) AS service_error_count,
          SUM(CASE WHEN chat_id = 0 AND UPPER(COALESCE(status, '')) = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count,
          SUM(CASE WHEN chat_id = 0 AND UPPER(COALESCE(status, '')) = 'DONE' THEN 1 ELSE 0 END) AS done_count,
          MAX(created_at) AS last_event_at
        FROM logs
        GROUP BY broadcast_id
      ) ls ON ls.broadcast_id = b.id
      LEFT JOIN (
        SELECT broadcast_id, COUNT(*) AS file_count
        FROM broadcast_files
        GROUP BY broadcast_id
      ) fs ON fs.broadcast_id = b.id
      WHERE 1=1
    """
    params = []
    if dt_from:
        sql += " AND b.created_at >= ?"
        params.append(dt_from)
    if dt_to:
        sql += " AND b.created_at <= ?"
        params.append(dt_to)

    sql += " ORDER BY b.id DESC LIMIT ?"
    params.append(limit)
    return conn.cursor().execute(sql, tuple(params)).fetchall()


def get_broadcast_summary(conn, broadcast_id: int):
    rows = list_broadcast_summaries(conn, limit=1)
    row = conn.cursor().execute(
        """
        SELECT
          b.id AS broadcast_id,
          b.created_at,
          b.created_by AS broadcast_user,
          b.html,
          b.text,
          COALESCE(fs.file_count, 0) AS file_count,
          COALESCE(ls.ok_count, 0) AS ok_count,
          COALESCE(ls.error_count, 0) AS error_count,
          COALESCE(ls.skipped_count, 0) AS skipped_count,
          COALESCE(ls.total_chat_rows, 0) AS total_chat_rows,
          COALESCE(ls.service_error_count, 0) AS service_error_count,
          COALESCE(ls.cancelled_count, 0) AS cancelled_count,
          COALESCE(ls.done_count, 0) AS done_count,
          COALESCE(ls.last_event_at, b.created_at) AS last_event_at
        FROM broadcasts b
        LEFT JOIN (
          SELECT
            broadcast_id,
            SUM(CASE WHEN chat_id != 0 AND UPPER(COALESCE(status, '')) = 'OK' THEN 1 ELSE 0 END) AS ok_count,
            SUM(CASE WHEN chat_id != 0 AND UPPER(COALESCE(status, '')) = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
            SUM(CASE WHEN chat_id != 0 AND UPPER(COALESCE(status, '')) = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped_count,
            SUM(CASE WHEN chat_id != 0 THEN 1 ELSE 0 END) AS total_chat_rows,
            SUM(CASE WHEN chat_id = 0 AND UPPER(COALESCE(status, '')) = 'ERROR' THEN 1 ELSE 0 END) AS service_error_count,
            SUM(CASE WHEN chat_id = 0 AND UPPER(COALESCE(status, '')) = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count,
            SUM(CASE WHEN chat_id = 0 AND UPPER(COALESCE(status, '')) = 'DONE' THEN 1 ELSE 0 END) AS done_count,
            MAX(created_at) AS last_event_at
          FROM logs
          GROUP BY broadcast_id
        ) ls ON ls.broadcast_id = b.id
        LEFT JOIN (
          SELECT broadcast_id, COUNT(*) AS file_count
          FROM broadcast_files
          GROUP BY broadcast_id
        ) fs ON fs.broadcast_id = b.id
        WHERE b.id = ?
        """,
        (int(broadcast_id),)
    ).fetchone()
    return row


def list_broadcast_logs(conn, broadcast_id: int, limit: int = 10000):
    sql = """
      SELECT l.*, c.title AS chat_title, b.created_by AS broadcast_user
      FROM logs l
      LEFT JOIN chats c ON c.chat_id=l.chat_id
      LEFT JOIN broadcasts b ON b.id=l.broadcast_id
      WHERE l.broadcast_id = ?
      ORDER BY CASE WHEN l.chat_id = 0 THEN 1 ELSE 0 END, l.id ASC
      LIMIT ?
    """
    return conn.cursor().execute(sql, (int(broadcast_id), int(limit))).fetchall()

def get_setting(conn, key: str):
    row = conn.cursor().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None

def set_setting(conn, key: str, value: str):
    conn.cursor().execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()

def list_groups(conn):
    return conn.cursor().execute("SELECT * FROM chat_groups ORDER BY id DESC").fetchall()

def create_group(conn, name: str):
    cur = conn.cursor()
    cur.execute("INSERT INTO chat_groups(name, created_at) VALUES(?,?)", (name, datetime.utcnow().isoformat()))
    conn.commit()
    return cur.lastrowid

def delete_group(conn, group_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM chat_group_members WHERE group_id=?", (group_id,))
    cur.execute("DELETE FROM chat_groups WHERE id=?", (group_id,))
    conn.commit()

def get_group(conn, group_id: int):
    return conn.cursor().execute("SELECT * FROM chat_groups WHERE id=?", (group_id,)).fetchone()

def set_group_members(conn, group_id: int, chat_ids):
    cur = conn.cursor()
    cur.execute("DELETE FROM chat_group_members WHERE group_id=?", (group_id,))
    cur.executemany("INSERT OR IGNORE INTO chat_group_members(group_id, chat_id) VALUES(?,?)",
                    [(int(group_id), int(cid)) for cid in (chat_ids or [])])
    conn.commit()

def list_group_members(conn, group_id: int):
    return conn.cursor().execute("""
      SELECT m.chat_id, c.title, c.chat_type, c.blocked
      FROM chat_group_members m LEFT JOIN chats c ON c.chat_id=m.chat_id
      WHERE m.group_id=? ORDER BY c.title
    """, (group_id,)).fetchall()

def get_group_chat_ids(conn, group_id: int):
    rows = conn.cursor().execute("SELECT chat_id FROM chat_group_members WHERE group_id=?", (group_id,)).fetchall()
    return [int(r["chat_id"]) for r in rows]


# ---------------------------
# Users / Roles
# ---------------------------

def init_users(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin','manager')),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    conn.commit()


def _ensure_default_admin(conn):
    """
    Create first admin user if users table is empty.
    Backward compatible: if settings has admin_password_hash, reuse it.
    """
    from .auth import hash_password
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM users")
    count = cur.fetchone()[0] or 0
    if count > 0:
        return

    admin_login = (os.environ.get("ADMIN_LOGIN", "admin") or "admin").strip()
    admin_password = os.environ.get("ADMIN_PASSWORD", "maxmarketing") or "maxmarketing"

    old_hash = get_setting(conn, "admin_password_hash")
    password_hash = old_hash if old_hash else hash_password(admin_password)

    cur.execute(
        "INSERT INTO users(username, password_hash, role) VALUES(?,?,?)",
        (admin_login, password_hash, "admin"),
    )
    conn.commit()


def get_user_by_id(conn, user_id: int):
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, password_hash, role, created_at FROM users WHERE id=?",
        (int(user_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "username": row[1], "password_hash": row[2], "role": row[3], "created_at": row[4]}


def get_user_by_username(conn, username: str):
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, password_hash, role, created_at FROM users WHERE username=?",
        ((username or "").strip(),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "username": row[1], "password_hash": row[2], "role": row[3], "created_at": row[4]}


def list_users(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, username, role, created_at FROM users ORDER BY id ASC")
    rows = cur.fetchall() or []
    return [{"id": r[0], "username": r[1], "role": r[2], "created_at": r[3]} for r in rows]


def create_user(conn, username: str, password: str, role: str = "manager"):
    from .auth import hash_password
    role = role if role in ("admin", "manager") else "manager"
    username = (username or "").strip()
    if not username:
        raise ValueError("username_empty")
    if len(username) > 64:
        raise ValueError("username_too_long")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users(username, password_hash, role) VALUES(?,?,?)",
        (username, hash_password(password or ""), role),
    )
    conn.commit()


def delete_user(conn, user_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id=?", (int(user_id),))
    conn.commit()


def update_user_password(conn, user_id: int, new_password: str):
    from .auth import hash_password
    cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(new_password or ""), int(user_id)))
    conn.commit()
