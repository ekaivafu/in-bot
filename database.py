"""
database.py
───────────
Database persistence layer.
Supports Neon / PostgreSQL via DATABASE_URL (production/Render) with automatic
thread-safe connection pooling, reconnect resilience, and SQLite fallback.

Tables:
  • users     - Tracks user IDs, usernames, display names, first/last seen
  • downloads - Tracks all download attempts (user, URL, success/fail, timestamp)
  • settings  - Key-value configuration store (maintenance mode, alerts, etc.)
  • channels  - Multi-channel force-subscription configurations
"""

import os
import sqlite3
import threading
import logging
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

USE_POSTGRES = bool(DATABASE_URL and ("postgresql://" in DATABASE_URL or "postgres://" in DATABASE_URL))
DB_PATH = Path("bot_data.db")
_sqlite_lock = threading.Lock()

# ── PostgreSQL Setup ───────────────────────────────────────────────────────────
_pg_pool = None
_pg_lock = threading.Lock()

if USE_POSTGRES:
    try:
        from psycopg2.pool import ThreadedConnectionPool
        import psycopg2
        _pg_pool = ThreadedConnectionPool(1, 10, dsn=DATABASE_URL)
        logger.info("Connected to PostgreSQL / Neon Database pool")
    except Exception as exc:
        logger.warning("Failed to initialize PostgreSQL pool: %s. Falling back to SQLite.", exc)
        USE_POSTGRES = False


@contextmanager
def get_db_cursor():
    """
    Context manager yielding a database cursor (Postgres or SQLite).
    Handles commit, rollback, and returning connection to pool.
    """
    global _pg_pool, USE_POSTGRES
    if USE_POSTGRES and _pg_pool:
        conn = None
        with _pg_lock:
            try:
                conn = _pg_pool.getconn()
            except Exception:
                try:
                    import psycopg2
                    from psycopg2.pool import ThreadedConnectionPool
                    _pg_pool = ThreadedConnectionPool(1, 10, dsn=DATABASE_URL)
                    conn = _pg_pool.getconn()
                except Exception as exc:
                    logger.error("Failed to acquire PostgreSQL connection: %s", exc)
                    raise

        # Check connection liveness
        try:
            import psycopg2
            if conn.closed != 0:
                conn = psycopg2.connect(DATABASE_URL)
        except Exception:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)

        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            with _pg_lock:
                try:
                    _pg_pool.putconn(conn)
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
    else:
        with _sqlite_lock:
            conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            try:
                cur = conn.cursor()
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()


# ── Internal Migration ─────────────────────────────────────────────────────────
def _migrate_from_sqlite() -> None:
    """One-time migration of existing data from bot_data.db into PostgreSQL."""
    if not DB_PATH.exists():
        return
    try:
        sq_conn = sqlite3.connect(str(DB_PATH))
        sq_cur = sq_conn.cursor()

        with get_db_cursor() as pg_cur:
            # Users
            try:
                sq_cur.execute("SELECT user_id, username, first_name, joined_at FROM users")
                for r in sq_cur.fetchall():
                    pg_cur.execute("""
                        INSERT INTO users (user_id, username, first_name, joined_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id) DO NOTHING
                    """, r)
            except Exception:
                pass

            # Channels
            try:
                sq_cur.execute("SELECT chat_id, title, invite_link FROM channels")
                for r in sq_cur.fetchall():
                    pg_cur.execute("""
                        INSERT INTO channels (chat_id, title, invite_link)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (chat_id) DO NOTHING
                    """, r)
            except Exception:
                pass

            # Settings
            try:
                sq_cur.execute("SELECT key, value FROM settings")
                for r in sq_cur.fetchall():
                    pg_cur.execute("""
                        INSERT INTO settings (key, value)
                        VALUES (%s, %s)
                        ON CONFLICT (key) DO NOTHING
                    """, r)
            except Exception:
                pass

            # Downloads
            try:
                sq_cur.execute("SELECT user_id, url, success, ts FROM downloads")
                for r in sq_cur.fetchall():
                    pg_cur.execute("""
                        INSERT INTO downloads (user_id, url, success, ts)
                        VALUES (%s, %s, %s, %s)
                    """, r)
            except Exception:
                pass

            # Mark migration complete
            pg_cur.execute("INSERT INTO settings (key, value) VALUES ('sqlite_migrated', '1') ON CONFLICT (key) DO NOTHING")

        sq_conn.close()
        logger.info("Successfully migrated existing SQLite data to PostgreSQL / Neon!")
    except Exception as exc:
        logger.warning("Error during SQLite migration: %s", exc)


# ── Init ───────────────────────────────────────────────────────────────────────
def init_db() -> None:
    if USE_POSTGRES:
        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id    BIGINT PRIMARY KEY,
                    username   TEXT DEFAULT '',
                    first_name TEXT DEFAULT '',
                    joined_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_seen  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS downloads (
                    id        SERIAL PRIMARY KEY,
                    user_id   BIGINT NOT NULL,
                    url       TEXT DEFAULT '',
                    success   INTEGER NOT NULL DEFAULT 0,
                    ts        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS channels (
                    id          SERIAL PRIMARY KEY,
                    chat_id     TEXT UNIQUE NOT NULL,
                    title       TEXT NOT NULL,
                    invite_link TEXT DEFAULT '',
                    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)

            # Check if migration needed
            cur.execute("SELECT value FROM settings WHERE key='sqlite_migrated'")
            migrated = cur.fetchone()
            if not migrated and DB_PATH.exists():
                _migrate_from_sqlite()

            # Restore cookies.txt from database if missing from disk (e.g. Render restart/redeploy)
            try:
                cur.execute("SELECT value FROM settings WHERE key='active_cookies'")
                crow = cur.fetchone()
                if crow and crow[0] and not Path("cookies.txt").exists():
                    Path("cookies.txt").write_text(crow[0], encoding="utf-8")
                    logger.info("Restored cookies.txt from database")
            except Exception as e:
                logger.warning("Failed to restore cookies: %s", e)

        logger.info("Database initialized (Neon PostgreSQL mode)")
    else:
        with get_db_cursor() as cur:
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id    INTEGER PRIMARY KEY,
                    username   TEXT    DEFAULT '',
                    first_name TEXT    DEFAULT '',
                    joined_at  TEXT    NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS downloads (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id   INTEGER NOT NULL,
                    url       TEXT    DEFAULT '',
                    success   INTEGER NOT NULL DEFAULT 0,
                    ts        TEXT    NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS channels (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id     TEXT UNIQUE NOT NULL,
                    title       TEXT NOT NULL,
                    invite_link TEXT DEFAULT '',
                    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
                );
            """)
        logger.info("Database initialized (SQLite mode)")


# ── Users ──────────────────────────────────────────────────────────────────────
def upsert_user(user_id: int, username: Optional[str], first_name: str) -> None:
    if USE_POSTGRES:
        with get_db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_seen)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    username   = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_seen  = NOW()
                """,
                (user_id, username or "", first_name or ""),
            )
    else:
        with get_db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, username, first_name)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username   = excluded.username,
                    first_name = excluded.first_name
                """,
                (user_id, username or "", first_name or ""),
            )


def get_all_user_ids() -> list[int]:
    with get_db_cursor() as cur:
        cur.execute("SELECT user_id FROM users")
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


# ── Downloads ──────────────────────────────────────────────────────────────────
def log_download(user_id: int, url: str, success: bool) -> None:
    placeholder = "%s" if USE_POSTGRES else "?"
    time_fn = "NOW()" if USE_POSTGRES else "datetime('now')"
    with get_db_cursor() as cur:
        cur.execute(
            f"INSERT INTO downloads (user_id, url, success, ts) VALUES ({placeholder}, {placeholder}, {placeholder}, {time_fn})",
            (user_id, url, 1 if success else 0),
        )


# ── Stats ──────────────────────────────────────────────────────────────────────
def get_stats() -> dict:
    today = date.today()
    if USE_POSTGRES:
        with get_db_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            total_users = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM downloads WHERE success=1")
            total_ok = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM downloads WHERE success=1 AND DATE(ts)=%s", (today,))
            today_ok = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM downloads WHERE success=0")
            total_fail = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM downloads WHERE success=0 AND DATE(ts)=%s", (today,))
            today_fail = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT user_id) FROM downloads WHERE DATE(ts)=%s", (today,))
            active_today = cur.fetchone()[0]
    else:
        today_str = today.isoformat()
        with get_db_cursor() as cur:
            total_users   = cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            total_ok      = cur.execute("SELECT COUNT(*) FROM downloads WHERE success=1").fetchone()[0]
            today_ok      = cur.execute(
                "SELECT COUNT(*) FROM downloads WHERE success=1 AND date(ts)=?", (today_str,)
            ).fetchone()[0]
            total_fail    = cur.execute("SELECT COUNT(*) FROM downloads WHERE success=0").fetchone()[0]
            today_fail    = cur.execute(
                "SELECT COUNT(*) FROM downloads WHERE success=0 AND date(ts)=?", (today_str,)
            ).fetchone()[0]
            active_today  = cur.execute(
                "SELECT COUNT(DISTINCT user_id) FROM downloads WHERE date(ts)=?", (today_str,)
            ).fetchone()[0]

    return {
        "total_users":  int(total_users or 0),
        "total_ok":     int(total_ok or 0),
        "today_ok":     int(today_ok or 0),
        "total_fail":   int(total_fail or 0),
        "today_fail":   int(today_fail or 0),
        "active_today": int(active_today or 0),
    }


# ── Settings ───────────────────────────────────────────────────────────────────
def get_setting(key: str, default: str = "") -> str:
    placeholder = "%s" if USE_POSTGRES else "?"
    with get_db_cursor() as cur:
        cur.execute(f"SELECT value FROM settings WHERE key={placeholder}", (key,))
        row = cur.fetchone()
    return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    if USE_POSTGRES:
        with get_db_cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                (key, str(value)),
            )
    else:
        with get_db_cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )


# ── Channels (Multi-channel Force-Sub) ─────────────────────────────────────────
def add_channel(chat_id: str, title: str, invite_link: str = "") -> None:
    if USE_POSTGRES:
        with get_db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO channels (chat_id, title, invite_link)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    invite_link = CASE WHEN EXCLUDED.invite_link != '' THEN EXCLUDED.invite_link ELSE channels.invite_link END
                """,
                (str(chat_id).strip(), title.strip(), (invite_link or "").strip()),
            )
    else:
        with get_db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO channels (chat_id, title, invite_link)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    title = excluded.title,
                    invite_link = CASE WHEN excluded.invite_link != '' THEN excluded.invite_link ELSE channels.invite_link END
                """,
                (str(chat_id).strip(), title.strip(), (invite_link or "").strip()),
            )


def update_channel_link(chat_id: str, invite_link: str) -> None:
    placeholder = "%s" if USE_POSTGRES else "?"
    with get_db_cursor() as cur:
        cur.execute(
            f"UPDATE channels SET invite_link={placeholder} WHERE chat_id={placeholder}",
            (invite_link.strip(), str(chat_id).strip()),
        )


def remove_channel(chat_id: str) -> bool:
    placeholder = "%s" if USE_POSTGRES else "?"
    with get_db_cursor() as cur:
        cur.execute(f"DELETE FROM channels WHERE chat_id={placeholder}", (str(chat_id).strip(),))
        deleted = cur.rowcount > 0
    return deleted


def get_all_channels() -> list[dict]:
    with get_db_cursor() as cur:
        cur.execute("SELECT id, chat_id, title, invite_link, created_at FROM channels ORDER BY id ASC")
        rows = cur.fetchall()
    return [
        {
            "id": r[0],
            "chat_id": str(r[1]),
            "title": str(r[2]),
            "invite_link": str(r[3] or ""),
            "created_at": str(r[4]),
        }
        for r in rows
    ]


def get_channel_count() -> int:
    with get_db_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM channels")
        count = cur.fetchone()[0]
    return int(count or 0)


def clear_all_channels() -> None:
    with get_db_cursor() as cur:
        cur.execute("DELETE FROM channels")
