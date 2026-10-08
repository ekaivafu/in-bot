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
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
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

# ── High Performance In-Memory Caches ──────────────────────────────────────────
# Settings and channels rarely change. Caching them eliminates multiple 300-800ms
# round trips to Neon PostgreSQL on every single user message and /start command.
_cache_lock = threading.Lock()
_settings_cache: dict[str, str] = {}
_channels_cache: list[dict] | None = None
_user_seen_cache: dict[int, float] = {}

# ── PostgreSQL Setup ───────────────────────────────────────────────────────────
_pg_pool = None
_pg_lock = threading.Lock()

if USE_POSTGRES:
    try:
        from psycopg2.pool import ThreadedConnectionPool
        import psycopg2
        _pg_pool = ThreadedConnectionPool(
            1, 10,
            dsn=DATABASE_URL,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        logger.info("Connected to PostgreSQL / Neon Database pool (keepalives enabled)")
    except Exception as exc:
        logger.warning("Failed to initialize PostgreSQL pool: %s. Falling back to SQLite.", exc)
        USE_POSTGRES = False


@contextmanager
def get_db_cursor():
    """
    Context manager yielding a database cursor (Postgres or SQLite).
    Handles commit, rollback, connection liveness, and SSL resets from Neon cold-starts.
    """
    global _pg_pool, USE_POSTGRES
    if USE_POSTGRES:
        import psycopg2

        def _fresh_conn():
            return psycopg2.connect(
                DATABASE_URL,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )

        conn = None
        # Try to get a pooled connection, recreate pool on failure
        with _pg_lock:
            try:
                if _pg_pool:
                    conn = _pg_pool.getconn()
            except Exception:
                pass
            if conn is None:
                try:
                    from psycopg2.pool import ThreadedConnectionPool
                    _pg_pool = ThreadedConnectionPool(
                        1, 10,
                        dsn=DATABASE_URL,
                        keepalives=1,
                        keepalives_idle=30,
                        keepalives_interval=10,
                        keepalives_count=5,
                    )
                    conn = _pg_pool.getconn()
                except Exception as exc:
                    logger.error("Failed to acquire PostgreSQL connection from pool: %s", exc)
                    # Last resort: direct connection
                    conn = _fresh_conn()

        # Ping to verify the connection is alive (catches SSL resets from Neon sleep)
        try:
            conn.cursor().execute("SELECT 1")
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = _fresh_conn()

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
                    if _pg_pool:
                        _pg_pool.putconn(conn)
                    else:
                        conn.close()
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
        # Retry up to 3 times — Neon wakes from sleep and the first SSL connection
        # sometimes drops before init queries can execute.
        last_exc = None
        for attempt in range(1, 4):
            try:
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

                        CREATE TABLE IF NOT EXISTS media_cache (
                            id            SERIAL PRIMARY KEY,
                            shortcode     TEXT UNIQUE NOT NULL,
                            video_file_id TEXT DEFAULT '',
                            audio_file_id TEXT DEFAULT '',
                            caption       TEXT DEFAULT '',
                            created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        );

                        CREATE TABLE IF NOT EXISTS creator_watchlist (
                            id         SERIAL PRIMARY KEY,
                            username   TEXT UNIQUE NOT NULL,
                            added_by   BIGINT DEFAULT 0,
                            active     BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        );

                        CREATE TABLE IF NOT EXISTS scout_seen_reels (
                            id            SERIAL PRIMARY KEY,
                            shortcode     TEXT UNIQUE NOT NULL,
                            creator       TEXT NOT NULL,
                            likes         INTEGER DEFAULT 0,
                            posted_date   TEXT DEFAULT '',
                            discovered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            status        TEXT DEFAULT 'passed'
                        );

                        CREATE TABLE IF NOT EXISTS scout_queue (
                            id            SERIAL PRIMARY KEY,
                            shortcode     TEXT NOT NULL,
                            creator       TEXT NOT NULL,
                            likes         INTEGER DEFAULT 0,
                            video_file_id TEXT DEFAULT '',
                            caption       TEXT DEFAULT '',
                            created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            dispatched    BOOLEAN DEFAULT FALSE
                        );

                        CREATE TABLE IF NOT EXISTS user_watchlist (
                            id         SERIAL PRIMARY KEY,
                            user_id    BIGINT NOT NULL,
                            username   TEXT NOT NULL,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            UNIQUE(user_id, username)
                        );

                        CREATE TABLE IF NOT EXISTS user_scout_limits (
                            user_id      BIGINT PRIMARY KEY,
                            custom_limit INTEGER NOT NULL DEFAULT 1
                        );

                        CREATE TABLE IF NOT EXISTS child_servers (
                            id                SERIAL PRIMARY KEY,
                            name              TEXT NOT NULL,
                            render_service_id TEXT DEFAULT '',
                            url               TEXT DEFAULT '',
                            uptimerobot_id    TEXT DEFAULT '',
                            active            BOOLEAN DEFAULT TRUE,
                            created_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        );

                        ALTER TABLE creator_watchlist ADD COLUMN IF NOT EXISTS assigned_worker_id INTEGER DEFAULT NULL;
                        ALTER TABLE creator_watchlist ADD COLUMN IF NOT EXISTS min_likes INTEGER DEFAULT 5000;
                        ALTER TABLE creator_watchlist ADD COLUMN IF NOT EXISTS max_days REAL DEFAULT 5.0;
                        ALTER TABLE creator_watchlist ADD COLUMN IF NOT EXISTS last_scouted_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;
                        ALTER TABLE creator_watchlist ADD COLUMN IF NOT EXISTS last_scout_status TEXT DEFAULT '';
                        ALTER TABLE creator_watchlist ADD COLUMN IF NOT EXISTS reels_checked_count INTEGER DEFAULT 0;
                        ALTER TABLE creator_watchlist ADD COLUMN IF NOT EXISTS max_likes_found INTEGER DEFAULT 0;
                        ALTER TABLE user_watchlist ADD COLUMN IF NOT EXISTS min_likes INTEGER DEFAULT 5000;
                        ALTER TABLE user_watchlist ADD COLUMN IF NOT EXISTS max_days REAL DEFAULT 5.0;
                        ALTER TABLE child_servers ADD COLUMN IF NOT EXISTS last_heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW();
                    """)

                    # Check if migration needed
                    cur.execute("SELECT value FROM settings WHERE key='sqlite_migrated'")
                    migrated = cur.fetchone()
                    if not migrated and DB_PATH.exists():
                        _migrate_from_sqlite()

                    # Restore cookies.txt from database if missing from disk
                    try:
                        cur.execute("SELECT value FROM settings WHERE key='active_cookies'")
                        crow = cur.fetchone()
                        if crow and crow[0] and not Path("cookies.txt").exists():
                            Path("cookies.txt").write_text(crow[0], encoding="utf-8")
                            logger.info("Restored cookies.txt from database")
                    except Exception as e:
                        logger.warning("Failed to restore cookies: %s", e)

                    _load_caches(cur)
                logger.info("Database initialized (Neon PostgreSQL mode with in-memory caching)")
                return  # success — exit retry loop
            except Exception as exc:
                last_exc = exc
                logger.warning("init_db attempt %d/3 failed: %s", attempt, exc)
                if attempt < 3:
                    time.sleep(2 * attempt)  # 2s, 4s
        raise RuntimeError(f"init_db failed after 3 attempts: {last_exc}") from last_exc
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

                CREATE TABLE IF NOT EXISTS media_cache (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    shortcode     TEXT UNIQUE NOT NULL,
                    video_file_id TEXT DEFAULT '',
                    audio_file_id TEXT DEFAULT '',
                    caption       TEXT DEFAULT '',
                    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS creator_watchlist (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    username   TEXT UNIQUE NOT NULL,
                    added_by   INTEGER DEFAULT 0,
                    active     INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS scout_seen_reels (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    shortcode     TEXT UNIQUE NOT NULL,
                    creator       TEXT NOT NULL,
                    likes         INTEGER DEFAULT 0,
                    posted_date   TEXT DEFAULT '',
                    discovered_at TEXT NOT NULL DEFAULT (datetime('now')),
                    status        TEXT DEFAULT 'passed'
                );

                CREATE TABLE IF NOT EXISTS scout_queue (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    shortcode     TEXT NOT NULL,
                    creator       TEXT NOT NULL,
                    likes         INTEGER DEFAULT 0,
                    video_file_id TEXT DEFAULT '',
                    caption       TEXT DEFAULT '',
                    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                    dispatched    INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS user_watchlist (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    username   TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(user_id, username)
                );

                CREATE TABLE IF NOT EXISTS user_scout_limits (
                    user_id      INTEGER PRIMARY KEY,
                    custom_limit INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS child_servers (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    name              TEXT NOT NULL,
                    render_service_id TEXT DEFAULT '',
                    url               TEXT DEFAULT '',
                    uptimerobot_id    TEXT DEFAULT '',
                    active            INTEGER DEFAULT 1,
                    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
                );
            """)

            for stmt in [
                "ALTER TABLE creator_watchlist ADD COLUMN assigned_worker_id INTEGER DEFAULT NULL",
                "ALTER TABLE creator_watchlist ADD COLUMN min_likes INTEGER DEFAULT 5000",
                "ALTER TABLE creator_watchlist ADD COLUMN max_days REAL DEFAULT 5.0",
                "ALTER TABLE creator_watchlist ADD COLUMN last_scouted_at TEXT DEFAULT NULL",
                "ALTER TABLE creator_watchlist ADD COLUMN last_scout_status TEXT DEFAULT ''",
                "ALTER TABLE creator_watchlist ADD COLUMN reels_checked_count INTEGER DEFAULT 0",
                "ALTER TABLE creator_watchlist ADD COLUMN max_likes_found INTEGER DEFAULT 0",
                "ALTER TABLE user_watchlist ADD COLUMN min_likes INTEGER DEFAULT 5000",
                "ALTER TABLE user_watchlist ADD COLUMN max_days REAL DEFAULT 5.0",
                "ALTER TABLE child_servers ADD COLUMN last_heartbeat TEXT DEFAULT NULL",
            ]:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
            _load_caches(cur)
        logger.info("Database initialized (SQLite mode with in-memory caching)")


def _load_caches(cur) -> None:
    """Pre-load settings and channels into memory for instant lookup."""
    global _settings_cache, _channels_cache
    with _cache_lock:
        try:
            cur.execute("SELECT key, value FROM settings")
            for k, v in cur.fetchall():
                _settings_cache[k] = v or ""
        except Exception as e:
            logger.warning("Failed to load settings cache: %s", e)

        try:
            cur.execute("SELECT id, chat_id, title, invite_link, created_at FROM channels ORDER BY id ASC")
            _channels_cache = [
                {
                    "id": r[0],
                    "chat_id": str(r[1]),
                    "title": str(r[2]),
                    "invite_link": str(r[3] or ""),
                    "created_at": str(r[4]),
                }
                for r in cur.fetchall()
            ]
        except Exception as e:
            logger.warning("Failed to load channels cache: %s", e)


_channel_change_callbacks: list = []


def register_channel_change_callback(callback) -> None:
    """Register a callback invoked when channels are added, removed, or updated."""
    if callback not in _channel_change_callbacks:
        _channel_change_callbacks.append(callback)


def _reload_channels_cache() -> None:
    """Reload channel cache after an add, update, or remove operation."""
    global _channels_cache
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT id, chat_id, title, invite_link, created_at FROM channels ORDER BY id ASC")
            rows = cur.fetchall()
        with _cache_lock:
            _channels_cache = [
                {
                    "id": r[0],
                    "chat_id": str(r[1]),
                    "title": str(r[2]),
                    "invite_link": str(r[3] or ""),
                    "created_at": str(r[4]),
                }
                for r in rows
            ]
        for cb in _channel_change_callbacks:
            try:
                cb()
            except Exception:
                pass
    except Exception as exc:
        logger.warning("Failed to reload channels cache: %s", exc)


# ── Users ──────────────────────────────────────────────────────────────────────
def upsert_user(user_id: int, username: Optional[str], first_name: str) -> None:
    now = time.time()
    last = _user_seen_cache.get(user_id, 0)
    # Throttle DB writes: only write once every 10 minutes per active user
    if now - last < 600:
        return
    _user_seen_cache[user_id] = now

    try:
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
    except Exception as exc:
        logger.warning("upsert_user failed for %s: %s", user_id, exc)


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


# ── Settings (Instant In-Memory Cache) ──────────────────────────────────────────
def get_setting(key: str, default: str = "") -> str:
    global _settings_cache
    with _cache_lock:
        if key in _settings_cache:
            return _settings_cache[key]
    placeholder = "%s" if USE_POSTGRES else "?"
    try:
        with get_db_cursor() as cur:
            cur.execute(f"SELECT value FROM settings WHERE key={placeholder}", (key,))
            row = cur.fetchone()
        val = row[0] if row else default
        with _cache_lock:
            _settings_cache[key] = val
        return val
    except Exception:
        return default


def set_setting(key: str, value: str) -> None:
    global _settings_cache
    with _cache_lock:
        _settings_cache[key] = str(value)
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


# ── Channels (Instant In-Memory Cache) ─────────────────────────────────────────
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
    _reload_channels_cache()


def update_channel_link(chat_id: str, invite_link: str) -> None:
    placeholder = "%s" if USE_POSTGRES else "?"
    with get_db_cursor() as cur:
        cur.execute(
            f"UPDATE channels SET invite_link={placeholder} WHERE chat_id={placeholder}",
            (invite_link.strip(), str(chat_id).strip()),
        )
    _reload_channels_cache()


def remove_channel(chat_id: str) -> bool:
    placeholder = "%s" if USE_POSTGRES else "?"
    with get_db_cursor() as cur:
        cur.execute(f"DELETE FROM channels WHERE chat_id={placeholder}", (str(chat_id).strip(),))
        deleted = cur.rowcount > 0
    _reload_channels_cache()
    return deleted


def get_all_channels() -> list[dict]:
    global _channels_cache
    with _cache_lock:
        if _channels_cache is not None:
            return list(_channels_cache)
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT id, chat_id, title, invite_link, created_at FROM channels ORDER BY id ASC")
            rows = cur.fetchall()
        channels = [
            {
                "id": r[0],
                "chat_id": str(r[1]),
                "title": str(r[2]),
                "invite_link": str(r[3] or ""),
                "created_at": str(r[4]),
            }
            for r in rows
        ]
        with _cache_lock:
            _channels_cache = list(channels)
        return channels
    except Exception:
        return []


def get_channel_count() -> int:
    global _channels_cache
    with _cache_lock:
        if _channels_cache is not None:
            return len(_channels_cache)
    return len(get_all_channels())


def clear_all_channels() -> None:
    with get_db_cursor() as cur:
        cur.execute("DELETE FROM channels")
    _reload_channels_cache()


# ── Media Cache (Instant Re-send & Audio Extraction) ───────────────────────────
def get_cached_media(shortcode: str) -> dict | None:
    """Retrieve cached Telegram file_ids and caption for an Instagram shortcode."""
    if not shortcode:
        return None
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"SELECT video_file_id, audio_file_id, caption FROM media_cache WHERE shortcode={placeholder}",
                (str(shortcode).strip(),),
            )
            row = cur.fetchone()
            if row:
                return {
                    "video_file_id": str(row[0] or ""),
                    "audio_file_id": str(row[1] or ""),
                    "caption": str(row[2] or ""),
                }
    except Exception as exc:
        logger.warning("get_cached_media error for %s: %s", shortcode, exc)
    return None


def set_cached_media(shortcode: str, video_file_id: str = "", audio_file_id: str = "", caption: str = "") -> None:
    """Save or update cached Telegram file_ids and caption."""
    if not shortcode:
        return
    sc = str(shortcode).strip()
    vid = str(video_file_id or "").strip()
    aud = str(audio_file_id or "").strip()
    cap = str(caption or "").strip()
    try:
        if USE_POSTGRES:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO media_cache (shortcode, video_file_id, audio_file_id, caption)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (shortcode) DO UPDATE SET
                        video_file_id = CASE WHEN EXCLUDED.video_file_id != '' THEN EXCLUDED.video_file_id ELSE media_cache.video_file_id END,
                        audio_file_id = CASE WHEN EXCLUDED.audio_file_id != '' THEN EXCLUDED.audio_file_id ELSE media_cache.audio_file_id END,
                        caption       = CASE WHEN EXCLUDED.caption != '' THEN EXCLUDED.caption ELSE media_cache.caption END
                    """,
                    (sc, vid, aud, cap),
                )
        else:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO media_cache (shortcode, video_file_id, audio_file_id, caption)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(shortcode) DO UPDATE SET
                        video_file_id = CASE WHEN excluded.video_file_id != '' THEN excluded.video_file_id ELSE media_cache.video_file_id END,
                        audio_file_id = CASE WHEN excluded.audio_file_id != '' THEN excluded.audio_file_id ELSE media_cache.audio_file_id END,
                        caption       = CASE WHEN excluded.caption != '' THEN excluded.caption ELSE media_cache.caption END
                    """,
                    (sc, vid, aud, cap),
                )
    except Exception as exc:
        logger.warning("set_cached_media error for %s: %s", shortcode, exc)


def update_cached_audio(shortcode: str, audio_file_id: str) -> None:
    """Update only the audio_file_id for an existing cached media item."""
    if not shortcode or not audio_file_id:
        return
    sc = str(shortcode).strip()
    aud = str(audio_file_id).strip()
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"UPDATE media_cache SET audio_file_id={placeholder} WHERE shortcode={placeholder}",
                (aud, sc),
            )
    except Exception as exc:
        logger.warning("update_cached_audio error for %s: %s", shortcode, exc)


# ── Scout & Watchlist Helpers (for Child Worker) ──────────────────────────────
def add_watchlist_creator(username: str, added_by: int = 0, min_likes: int = 5000, max_days: float = 5.0) -> bool:
    """Add a creator username to the scout watchlist and assign to least-loaded worker."""
    clean_user = username.strip().lstrip("@").lower()
    if not clean_user:
        return False
    if min_likes < 1:
        min_likes = 1
    if max_days < 0.1:
        max_days = 0.1
    try:
        with get_db_cursor() as cur:
            if USE_POSTGRES:
                cur.execute(
                    """
                    INSERT INTO creator_watchlist (username, added_by, active, min_likes, max_days)
                    VALUES (%s, %s, TRUE, %s, %s)
                    ON CONFLICT (username) DO UPDATE SET active = TRUE,
                        min_likes = LEAST(creator_watchlist.min_likes, EXCLUDED.min_likes),
                        max_days = GREATEST(creator_watchlist.max_days, EXCLUDED.max_days)
                    """,
                    (clean_user, added_by, min_likes, max_days),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO creator_watchlist (username, added_by, active, min_likes, max_days)
                    VALUES (?, ?, 1, ?, ?)
                    ON CONFLICT (username) DO UPDATE SET active = 1,
                        min_likes = MIN(COALESCE(creator_watchlist.min_likes, 5000), excluded.min_likes),
                        max_days = MAX(COALESCE(creator_watchlist.max_days, 5.0), excluded.max_days)
                    """,
                    (clean_user, added_by, min_likes, max_days),
                )
        # Assign to least-loaded child server if cluster is active
        assign_creator_to_least_loaded_worker(clean_user)
        return True
    except Exception as exc:
        logger.warning("add_watchlist_creator error for %s: %s", clean_user, exc)
        return False


def remove_watchlist_creator(username: str) -> bool:
    """Deactivate or remove a creator from the scout watchlist."""
    clean_user = username.strip().lstrip("@").lower()
    if not clean_user:
        return False
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        inactive_val = "FALSE" if USE_POSTGRES else "0"
        with get_db_cursor() as cur:
            cur.execute(
                f"UPDATE creator_watchlist SET active = {inactive_val}, assigned_worker_id = NULL WHERE username = {placeholder}",
                (clean_user,),
            )
            return True
    except Exception as exc:
        logger.warning("remove_watchlist_creator error for %s: %s", clean_user, exc)
        return False


def get_active_watchlist() -> list[str]:
    """Return all active creator usernames to scout."""
    try:
        with get_db_cursor() as cur:
            active_clause = "active = TRUE" if USE_POSTGRES else "active = 1"
            cur.execute(f"SELECT username FROM creator_watchlist WHERE {active_clause} ORDER BY id ASC")
            rows = cur.fetchall()
            return [r[0] for r in rows if r and r[0]]
    except Exception as exc:
        logger.warning("get_active_watchlist error: %s", exc)
        return []


def _format_time_ago(dt: any) -> str:
    """Helper to convert datetime/timestamp to human readable time ago."""
    if not dt:
        return "Never"
    try:
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = (datetime.now(timezone.utc) - dt).total_seconds()
            if delta < 60:
                return "Just now"
            elif delta < 3600:
                return f"{int(delta // 60)}m ago"
            elif delta < 86400:
                return f"{int(delta // 3600)}h ago"
            else:
                return f"{int(delta // 86400)}d ago"
    except Exception:
        pass
    return "Never"


def get_active_watchlist_with_targets() -> list[dict]:
    """Return all active creators with targets and live scout inspection status."""
    try:
        with get_db_cursor() as cur:
            active_clause = "active = TRUE" if USE_POSTGRES else "active = 1"
            cur.execute(
                f"""
                SELECT
                    username,
                    COALESCE(min_likes, 5000),
                    COALESCE(max_days, 5.0),
                    COALESCE(assigned_worker_id, 0),
                    last_scouted_at,
                    COALESCE(reels_checked_count, 0),
                    COALESCE(max_likes_found, 0),
                    COALESCE(last_scout_status, '')
                FROM creator_watchlist
                WHERE {active_clause}
                ORDER BY id ASC
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "username": r[0],
                    "min_likes": int(r[1]),
                    "max_days": float(r[2]),
                    "worker_id": r[3],
                    "last_scouted_at": r[4],
                    "reels_count": int(r[5] or 0),
                    "max_likes": int(r[6] or 0),
                    "status_note": r[7] or "",
                    "time_ago": _format_time_ago(r[4]),
                }
                for r in rows
            ]
    except Exception as exc:
        logger.warning("get_active_watchlist_with_targets error: %s", exc)
        return []


def get_creator_target_thresholds(creator: str) -> dict:
    """
    Computes effective target thresholds for a creator.
    Uses the lowest min_likes and highest max_days among all active subscribers,
    falling back to creator_watchlist settings or default (5000 likes, 5.0 days).
    """
    clean_user = creator.strip().lstrip("@").lower()
    default_res = {"min_likes": 5000, "max_days": 5.0}
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            # 1. Check all user subscribers
            cur.execute(
                f"""
                SELECT MIN(min_likes), MAX(max_days)
                FROM user_watchlist
                WHERE username = {placeholder}
                """,
                (clean_user,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return {
                    "min_likes": int(row[0]),
                    "max_days": float(row[1]) if row[1] is not None else 5.0,
                }

            # 2. Check creator_watchlist directly
            cur.execute(
                f"""
                SELECT min_likes, max_days
                FROM creator_watchlist
                WHERE username = {placeholder}
                LIMIT 1
                """,
                (clean_user,),
            )
            row_c = cur.fetchone()
            if row_c and row_c[0] is not None:
                return {
                    "min_likes": int(row_c[0]),
                    "max_days": float(row_c[1]) if row_c[1] is not None else 5.0,
                }
        return default_res
    except Exception as exc:
        logger.warning("get_creator_target_thresholds error for %s: %s", clean_user, exc)
        return default_res


def set_creator_global_targets(creator: str, min_likes: int, max_days: float) -> bool:
    """Admin function: update target min_likes and max_days in creator_watchlist."""
    clean_user = creator.strip().lstrip("@").lower()
    if min_likes < 1:
        min_likes = 1
    if max_days < 0.1:
        max_days = 0.1
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"""
                UPDATE creator_watchlist
                SET min_likes = {placeholder}, max_days = {placeholder}
                WHERE username = {placeholder}
                """,
                (min_likes, max_days, clean_user),
            )
            return True
    except Exception as exc:
        logger.warning("set_creator_global_targets error for %s: %s", clean_user, exc)
        return False


def is_reel_seen(shortcode: str) -> bool:
    """Check if shortcode was already discovered/processed by the scout."""
    if not shortcode:
        return True
    sc = shortcode.strip()
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(f"SELECT 1 FROM scout_seen_reels WHERE shortcode = {placeholder} LIMIT 1", (sc,))
            return cur.fetchone() is not None
    except Exception as exc:
        logger.warning("is_reel_seen error for %s: %s", sc, exc)
        return False


def record_seen_reel(shortcode: str, creator: str, likes: int = 0, posted_date: str = "", status: str = "passed") -> None:
    """Record reel in scout_seen_reels to guarantee zero duplicates."""
    if not shortcode:
        return
    sc = shortcode.strip()
    c = creator.strip().lstrip("@").lower()
    try:
        if USE_POSTGRES:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scout_seen_reels (shortcode, creator, likes, posted_date, status)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (shortcode) DO NOTHING
                    """,
                    (sc, c, likes, posted_date, status),
                )
        else:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO scout_seen_reels (shortcode, creator, likes, posted_date, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (sc, c, likes, posted_date, status),
                )
    except Exception as exc:
        logger.warning("record_seen_reel error for %s: %s", sc, exc)


def enqueue_viral_reel(shortcode: str, creator: str, likes: int, video_file_id: str, caption: str = "") -> bool:
    """Add a qualified downloaded viral reel to the dispatch queue."""
    if not shortcode or not video_file_id:
        return False
    sc = shortcode.strip()
    c = creator.strip().lstrip("@").lower()
    try:
        if USE_POSTGRES:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scout_queue (shortcode, creator, likes, video_file_id, caption, dispatched)
                    VALUES (%s, %s, %s, %s, %s, FALSE)
                    """,
                    (sc, c, likes, video_file_id, caption),
                )
        else:
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scout_queue (shortcode, creator, likes, video_file_id, caption, dispatched)
                    VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (sc, c, likes, video_file_id, caption),
                )
        return True
    except Exception as exc:
        logger.warning("enqueue_viral_reel error for %s: %s", sc, exc)
        return False


def get_pending_viral_reels(limit: int = 5) -> list[dict]:
    """Retrieve pending viral reels from queue ready for dispatch."""
    try:
        with get_db_cursor() as cur:
            dispatched_clause = "dispatched = FALSE" if USE_POSTGRES else "dispatched = 0"
            placeholder = "%s" if USE_POSTGRES else "?"
            cur.execute(
                f"""
                SELECT id, shortcode, creator, likes, video_file_id, caption
                FROM scout_queue
                WHERE {dispatched_clause}
                ORDER BY id ASC
                LIMIT {placeholder}
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "shortcode": r[1],
                    "creator": r[2],
                    "likes": r[3],
                    "video_file_id": r[4],
                    "caption": r[5],
                }
                for r in rows
            ]
    except Exception as exc:
        logger.warning("get_pending_viral_reels error: %s", exc)
        return []


def mark_viral_reel_dispatched(queue_id: int) -> None:
    """Mark a queued reel as dispatched."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        dispatched_val = "TRUE" if USE_POSTGRES else "1"
        with get_db_cursor() as cur:
            cur.execute(f"UPDATE scout_queue SET dispatched = {dispatched_val} WHERE id = {placeholder}", (queue_id,))
    except Exception as exc:
        logger.warning("mark_viral_reel_dispatched error for id %s: %s", queue_id, exc)


# ── User Watchlist & Limit Management ──────────────────────────────────────────
def get_user_scout_limit(user_id: int) -> int:
    """Get max creator watchlist slots for a user (default 1)."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(f"SELECT custom_limit FROM user_scout_limits WHERE user_id = {placeholder}", (user_id,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 1
    except Exception as exc:
        logger.warning("get_user_scout_limit error for %s: %s", user_id, exc)
        return 1


def set_user_scout_limit(user_id: int, limit: int) -> bool:
    """Admin function: Set max creator slots for a specific user."""
    if limit < 1:
        limit = 1
    try:
        with get_db_cursor() as cur:
            if USE_POSTGRES:
                cur.execute(
                    """
                    INSERT INTO user_scout_limits (user_id, custom_limit)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET custom_limit = EXCLUDED.custom_limit
                    """,
                    (user_id, limit),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO user_scout_limits (user_id, custom_limit)
                    VALUES (?, ?)
                    ON CONFLICT (user_id) DO UPDATE SET custom_limit = excluded.custom_limit
                    """,
                    (user_id, limit),
                )
        return True
    except Exception as exc:
        logger.warning("set_user_scout_limit error for %s: %s", user_id, exc)
        return False


def get_user_info(user_id: int) -> Optional[dict]:
    """Retrieve user details (username, first_name) from the users table."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(f"SELECT user_id, username, first_name FROM users WHERE user_id = {placeholder} LIMIT 1", (user_id,))
            row = cur.fetchone()
            if row:
                return {
                    "user_id": int(row[0]),
                    "username": row[1] or "",
                    "first_name": row[2] or "",
                }
            return None
    except Exception as exc:
        logger.warning("get_user_info error for %s: %s", user_id, exc)
        return None


def get_user_id_by_username(username: str) -> Optional[int]:
    """Lookup telegram user_id by their telegram username (with or without @)."""
    clean_username = username.strip().lstrip("@").lower()
    if not clean_username:
        return None
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(f"SELECT user_id FROM users WHERE LOWER(username) = {placeholder} LIMIT 1", (clean_username,))
            row = cur.fetchone()
            return int(row[0]) if row else None
    except Exception as exc:
        logger.warning("get_user_id_by_username error for %s: %s", clean_username, exc)
        return None


def get_all_custom_limits() -> list[dict]:
    """Retrieve all users who have custom slot limits configured."""
    try:
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT l.user_id, l.custom_limit, u.username, u.first_name 
                FROM user_scout_limits l
                LEFT JOIN users u ON l.user_id = u.user_id
                ORDER BY l.custom_limit DESC
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "user_id": int(r[0]),
                    "limit": int(r[1]),
                    "username": r[2] or "",
                    "first_name": r[3] or "",
                }
                for r in rows
            ]
    except Exception as exc:
        logger.warning("get_all_custom_limits error: %s", exc)
        return []



def get_user_watchlist(user_id: int) -> list[str]:
    """Retrieve all creator usernames actively tracked by a specific user."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"SELECT username FROM user_watchlist WHERE user_id = {placeholder} ORDER BY id ASC",
                (user_id,),
            )
            rows = cur.fetchall()
            return [r[0] for r in rows if r and r[0]]
    except Exception as exc:
        logger.warning("get_user_watchlist error for %s: %s", user_id, exc)
        return []


def get_user_watchlist_count(user_id: int) -> int:
    """Return count of creators monitored by user."""
    return len(get_user_watchlist(user_id))


def get_user_watchlist_details(user_id: int) -> list[dict]:
    """Retrieve all creator accounts tracked by user with their targets and live scout status."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    u.username,
                    COALESCE(u.min_likes, 5000),
                    COALESCE(u.max_days, 5.0),
                    c.last_scouted_at,
                    COALESCE(c.reels_checked_count, 0),
                    COALESCE(c.max_likes_found, 0),
                    COALESCE(c.last_scout_status, ''),
                    COALESCE(c.assigned_worker_id, 0)
                FROM user_watchlist u
                LEFT JOIN creator_watchlist c ON LOWER(u.username) = LOWER(c.username)
                WHERE u.user_id = {placeholder}
                ORDER BY u.id ASC
                """,
                (user_id,),
            )
            rows = cur.fetchall()
            return [
                {
                    "username": r[0],
                    "min_likes": int(r[1]),
                    "max_days": float(r[2]),
                    "last_scouted_at": r[3],
                    "reels_count": int(r[4] or 0),
                    "max_likes": int(r[5] or 0),
                    "status_note": r[6] or "",
                    "worker_id": r[7],
                    "time_ago": _format_time_ago(r[3]),
                }
                for r in rows
            ]
    except Exception as exc:
        logger.warning("get_user_watchlist_details error for %s: %s", user_id, exc)
        return []


def record_creator_scout_activity(
    creator: str,
    reels_count: int = 0,
    max_likes: int = 0,
    status_note: str = "",
    worker_id: int | None = None,
) -> bool:
    """Record that a creator's profile was actively inspected by scout worker."""
    clean_user = creator.strip().lstrip("@").lower()
    if not clean_user:
        return False
    try:
        now_ts = datetime.now(timezone.utc)
        placeholder = "%s" if USE_POSTGRES else "?"
        worker_clause = f", assigned_worker_id = {placeholder}" if worker_id is not None else ""
        params = [now_ts, reels_count, max_likes, status_note]
        if worker_id is not None:
            params.append(worker_id)
        params.append(clean_user)

        with get_db_cursor() as cur:
            cur.execute(
                f"""
                UPDATE creator_watchlist
                SET last_scouted_at = {placeholder},
                    reels_checked_count = {placeholder},
                    max_likes_found = {placeholder},
                    last_scout_status = {placeholder}
                    {worker_clause}
                WHERE username = {placeholder}
                """,
                tuple(params),
            )
        return True
    except Exception as exc:
        logger.warning("record_creator_scout_activity error for %s: %s", clean_user, exc)
        return False


def get_creator_scout_activity(creator: str) -> dict:
    """Retrieve the latest scout activity and time ago for a creator."""
    clean_user = creator.strip().lstrip("@").lower()
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"""
                SELECT last_scouted_at, reels_checked_count, max_likes_found, last_scout_status, assigned_worker_id
                FROM creator_watchlist
                WHERE username = {placeholder}
                LIMIT 1
                """,
                (clean_user,),
            )
            row = cur.fetchone()
            if row:
                return {
                    "last_scouted_at": row[0],
                    "reels_count": int(row[1] or 0),
                    "max_likes": int(row[2] or 0),
                    "status_note": row[3] or "",
                    "worker_id": row[4],
                    "time_ago": _format_time_ago(row[0]),
                }
        return {"last_scouted_at": None, "reels_count": 0, "max_likes": 0, "status_note": "", "worker_id": None, "time_ago": "Never"}
    except Exception as exc:
        logger.warning("get_creator_scout_activity error for %s: %s", clean_user, exc)
        return {"last_scouted_at": None, "reels_count": 0, "max_likes": 0, "status_note": "", "worker_id": None, "time_ago": "Never"}


def record_worker_heartbeat(worker_id: int | None) -> bool:
    """Record alive heartbeat timestamp for child server."""
    if worker_id is None:
        return False
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        now_ts = datetime.now(timezone.utc)
        with get_db_cursor() as cur:
            cur.execute(
                f"UPDATE child_servers SET last_heartbeat = {placeholder} WHERE id = {placeholder}",
                (now_ts, worker_id),
            )
        return True
    except Exception as exc:
        logger.warning("record_worker_heartbeat error for #%s: %s", worker_id, exc)
        return False


def get_cluster_worker_activity() -> list[dict]:
    """Retrieve all workers with their last heartbeat and active workload."""
    try:
        with get_db_cursor() as cur:
            active_clause = "active = TRUE" if USE_POSTGRES else "active = 1"
            cur.execute(
                f"""
                SELECT id, name, url, last_heartbeat,
                       (SELECT COUNT(*) FROM creator_watchlist c WHERE c.assigned_worker_id = s.id AND {active_clause})
                FROM child_servers s
                WHERE {active_clause}
                ORDER BY id ASC
                """
            )
            rows = cur.fetchall()
            res = []
            for r in rows:
                sid, name, url, hb_dt, c_count = r
                t_ago = _format_time_ago(hb_dt)
                is_online = False
                if hb_dt:
                    if isinstance(hb_dt, datetime):
                        delta = (datetime.now(timezone.utc) - (hb_dt.replace(tzinfo=timezone.utc) if hb_dt.tzinfo is None else hb_dt)).total_seconds()
                        is_online = delta < 300  # < 5 minutes
                res.append({
                    "id": sid,
                    "name": name,
                    "url": url,
                    "last_heartbeat": hb_dt,
                    "time_ago": t_ago,
                    "is_online": is_online,
                    "creator_count": int(c_count or 0),
                })
            return res
    except Exception as exc:
        logger.warning("get_cluster_worker_activity error: %s", exc)
        return []


def get_user_creator_targets(user_id: int, creator: str) -> dict:
    """Retrieve specific user's targets for a creator."""
    clean_user = creator.strip().lstrip("@").lower()
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"""
                SELECT COALESCE(min_likes, 5000), COALESCE(max_days, 5.0)
                FROM user_watchlist
                WHERE user_id = {placeholder} AND username = {placeholder}
                LIMIT 1
                """,
                (user_id, clean_user),
            )
            row = cur.fetchone()
            if row:
                return {"min_likes": int(row[0]), "max_days": float(row[1])}
        return {"min_likes": 5000, "max_days": 5.0}
    except Exception as exc:
        logger.warning("get_user_creator_targets error: %s", exc)
        return {"min_likes": 5000, "max_days": 5.0}


def set_user_watchlist_targets(user_id: int, creator: str, min_likes: int, max_days: float) -> bool:
    """Update target likes and days for a specific user and creator."""
    clean_user = creator.strip().lstrip("@").lower()
    if min_likes < 1:
        min_likes = 1
    if max_days < 0.1:
        max_days = 0.1
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"""
                UPDATE user_watchlist
                SET min_likes = {placeholder}, max_days = {placeholder}
                WHERE user_id = {placeholder} AND username = {placeholder}
                """,
                (min_likes, max_days, user_id, clean_user),
            )
            # Sync creator_watchlist if user target is lower
            if USE_POSTGRES:
                cur.execute(
                    """
                    UPDATE creator_watchlist
                    SET min_likes = LEAST(creator_watchlist.min_likes, %s),
                        max_days = GREATEST(creator_watchlist.max_days, %s)
                    WHERE username = %s
                    """,
                    (min_likes, max_days, clean_user),
                )
            else:
                cur.execute(
                    """
                    UPDATE creator_watchlist
                    SET min_likes = MIN(COALESCE(creator_watchlist.min_likes, 5000), ?),
                        max_days = MAX(COALESCE(creator_watchlist.max_days, 5.0), ?)
                    WHERE username = ?
                    """,
                    (min_likes, max_days, clean_user),
                )
            return True
    except Exception as exc:
        logger.warning("set_user_watchlist_targets error for %s: %s", clean_user, exc)
        return False


def add_user_watchlist_creator(user_id: int, username: str, is_admin_user: bool = False, min_likes: int = 5000, max_days: float = 5.0) -> tuple[bool, str]:
    """
    Add a creator to a user's watchlist with slot limit enforcement and custom targets.
    Returns:
      (True, "added")           - Successfully added
      (False, "limit_reached")  - User hit their slot limit (default 1)
      (False, "already_exists") - Already on user's list
      (False, "error")          - Database / validation error
    """
    clean_user = username.strip().lstrip("@").lower()
    if not clean_user or not user_id:
        return False, "invalid"

    if min_likes < 1:
        min_likes = 1
    if max_days < 0.1:
        max_days = 0.1

    # Enforce slot limit (admin has unlimited slots)
    if not is_admin_user:
        user_limit = get_user_scout_limit(user_id)
        current_count = get_user_watchlist_count(user_id)
        if current_count >= user_limit:
            return False, "limit_reached"

    # Check if already in user's list
    existing = get_user_watchlist(user_id)
    if clean_user in existing:
        return False, "already_exists"

    try:
        # 1. Insert into user_watchlist with targets
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"INSERT INTO user_watchlist (user_id, username, min_likes, max_days) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})",
                (user_id, clean_user, min_likes, max_days),
            )

        # 2. Ensure creator is active in global scout creator_watchlist
        add_watchlist_creator(clean_user, added_by=user_id, min_likes=min_likes, max_days=max_days)
        return True, "added"
    except Exception as exc:
        logger.warning("add_user_watchlist_creator error for user %s, creator %s: %s", user_id, clean_user, exc)
        return False, "error"


def remove_user_watchlist_creator(user_id: int, username: str) -> bool:
    """Remove a creator from user's watchlist. Deactivates from scout if 0 other users track it."""
    clean_user = username.strip().lstrip("@").lower()
    if not clean_user or not user_id:
        return False
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"DELETE FROM user_watchlist WHERE user_id = {placeholder} AND username = {placeholder}",
                (user_id, clean_user),
            )

            # Check if any other users still track this creator
            cur.execute(
                f"SELECT 1 FROM user_watchlist WHERE username = {placeholder} LIMIT 1",
                (clean_user,),
            )
            has_others = cur.fetchone() is not None
            if not has_others:
                remove_watchlist_creator(clean_user)

        return True
    except Exception as exc:
        logger.warning("remove_user_watchlist_creator error: %s", exc)
        return False


def get_subscribers_for_creator(creator: str) -> list[int]:
    """Retrieve all user IDs monitoring a specific creator."""
    clean_user = creator.strip().lstrip("@").lower()
    if not clean_user:
        return []
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT user_id FROM user_watchlist WHERE username = {placeholder}",
                (clean_user,),
            )
            rows = cur.fetchall()
            return [int(r[0]) for r in rows if r and r[0]]
    except Exception as exc:
        logger.warning("get_subscribers_for_creator error for %s: %s", clean_user, exc)
        return []


def get_subscribers_for_creator_filtered(creator: str, likes: int, age_days: float) -> list[int]:
    """Retrieve all user IDs monitoring a creator whose targets match the reel."""
    clean_user = creator.strip().lstrip("@").lower()
    if not clean_user:
        return []
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT user_id
                FROM user_watchlist
                WHERE username = {placeholder}
                  AND COALESCE(min_likes, 5000) <= {placeholder}
                  AND COALESCE(max_days, 5.0) >= {placeholder}
                """,
                (clean_user, likes, age_days),
            )
            rows = cur.fetchall()
            return [int(r[0]) for r in rows if r and r[0]]
    except Exception as exc:
        logger.warning("get_subscribers_for_creator_filtered error for %s: %s", clean_user, exc)
        return []


# ── Multi-Server Cluster & Workload Load-Balancing ─────────────────────────────
def add_child_server(name: str, render_service_id: str = "", url: str = "", uptimerobot_id: str = "") -> int:
    """Registers a child server. Automatically rebalances creators evenly across cluster."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        clean_url = url.strip().rstrip("/")
        clean_name = name.strip() or "Worker"
        with get_db_cursor() as cur:
            if USE_POSTGRES:
                cur.execute(
                    """
                    INSERT INTO child_servers (name, render_service_id, url, uptimerobot_id, active)
                    VALUES (%s, %s, %s, %s, TRUE)
                    RETURNING id
                    """,
                    (clean_name, render_service_id.strip(), clean_url, str(uptimerobot_id).strip()),
                )
                server_id = cur.fetchone()[0]
            else:
                cur.execute(
                    """
                    INSERT INTO child_servers (name, render_service_id, url, uptimerobot_id, active)
                    VALUES (?, ?, ?, ?, 1)
                    """,
                    (clean_name, render_service_id.strip(), clean_url, str(uptimerobot_id).strip()),
                )
                server_id = cur.lastrowid

        # Immediately divide existing accounts evenly across all servers (e.g. 10 -> 5-5)
        rebalance_creator_workload()
        return int(server_id)
    except Exception as exc:
        logger.warning("add_child_server error: %s", exc)
        return 0


def remove_child_server(server_id: int) -> bool:
    """Deactivates a child server and evenly rebalances creators across remaining servers."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        inactive_val = "FALSE" if USE_POSTGRES else "0"
        with get_db_cursor() as cur:
            cur.execute(
                f"UPDATE child_servers SET active = {inactive_val} WHERE id = {placeholder}",
                (server_id,),
            )
            cur.execute(
                f"UPDATE creator_watchlist SET assigned_worker_id = NULL WHERE assigned_worker_id = {placeholder}",
                (server_id,),
            )
        # Redistribute unassigned creators across active servers
        rebalance_creator_workload()
        return True
    except Exception as exc:
        logger.warning("remove_child_server error for %s: %s", server_id, exc)
        return False


def get_child_server(server_id: int) -> dict | None:
    """Retrieve details of a specific child server."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"SELECT id, name, render_service_id, url, uptimerobot_id, active FROM child_servers WHERE id = {placeholder}",
                (server_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "render_service_id": row[2],
                "url": row[3],
                "uptimerobot_id": row[4],
                "active": bool(row[5]),
            }
    except Exception as exc:
        logger.warning("get_child_server error for %s: %s", server_id, exc)
        return None


def get_active_child_servers() -> list[dict]:
    """Retrieve all active child servers."""
    try:
        active_val = "TRUE" if USE_POSTGRES else "1"
        with get_db_cursor() as cur:
            cur.execute(
                f"SELECT id, name, render_service_id, url, uptimerobot_id, created_at FROM child_servers WHERE active = {active_val} ORDER BY id ASC"
            )
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "name": r[1],
                    "render_service_id": r[2],
                    "url": r[3],
                    "uptimerobot_id": r[4],
                    "created_at": str(r[5]),
                }
                for r in rows
            ]
    except Exception as exc:
        logger.warning("get_active_child_servers error: %s", exc)
        return []


def update_child_server_uptime(server_id: int, uptimerobot_id: str) -> bool:
    """Save UptimeRobot monitor ID to server."""
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            cur.execute(
                f"UPDATE child_servers SET uptimerobot_id = {placeholder} WHERE id = {placeholder}",
                (str(uptimerobot_id).strip(), server_id),
            )
        return True
    except Exception as exc:
        logger.warning("update_child_server_uptime error for %s: %s", server_id, exc)
        return False


def rebalance_creator_workload() -> dict[int, list[str]]:
    """
    Evenly redistributes all active creators round-robin across all active child servers.
    Example: 10 creators across 2 servers -> 5-5
             10 creators across 3 servers -> 4-3-3
    Returns mapping of {server_id: [usernames]}
    """
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        active_val = "TRUE" if USE_POSTGRES else "1"
        with get_db_cursor() as cur:
            # 1. Fetch active servers
            cur.execute(f"SELECT id FROM child_servers WHERE active = {active_val} ORDER BY id ASC")
            servers = [int(r[0]) for r in cur.fetchall() if r and r[0]]

            # 2. Fetch active creators
            cur.execute(f"SELECT username FROM creator_watchlist WHERE active = {active_val} ORDER BY id ASC")
            creators = [r[0] for r in cur.fetchall() if r and r[0]]

            if not servers:
                # No child servers: unassign all (workers operate standalone)
                cur.execute(f"UPDATE creator_watchlist SET assigned_worker_id = NULL WHERE active = {active_val}")
                return {}

            distribution: dict[int, list[str]] = {s: [] for s in servers}
            k = len(servers)

            for i, creator in enumerate(creators):
                target_server = servers[i % k]
                distribution[target_server].append(creator)
                cur.execute(
                    f"UPDATE creator_watchlist SET assigned_worker_id = {placeholder} WHERE username = {placeholder}",
                    (target_server, creator),
                )

            logger.info("Rebalanced %d creators across %d child servers: %s",
                        len(creators), k, {sid: len(c_list) for sid, c_list in distribution.items()})
            return distribution
    except Exception as exc:
        logger.warning("rebalance_creator_workload error: %s", exc)
        return {}


def assign_creator_to_least_loaded_worker(username: str) -> int | None:
    """
    Assigns a creator to the active child server with the lowest count of assigned creators.
    Ensures wise round-robin / minimum-load allocation as new accounts are added.
    """
    clean_user = username.strip().lstrip("@").lower()
    if not clean_user:
        return None
    try:
        placeholder = "%s" if USE_POSTGRES else "?"
        active_val = "TRUE" if USE_POSTGRES else "1"
        with get_db_cursor() as cur:
            # Check if this creator is already assigned to a valid active server
            cur.execute(
                f"""
                SELECT cw.assigned_worker_id 
                FROM creator_watchlist cw
                JOIN child_servers cs ON cs.id = cw.assigned_worker_id
                WHERE cw.username = {placeholder} AND cs.active = {active_val}
                LIMIT 1
                """,
                (clean_user,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return int(row[0])

            # Get active servers
            cur.execute(f"SELECT id FROM child_servers WHERE active = {active_val} ORDER BY id ASC")
            servers = [int(r[0]) for r in cur.fetchall() if r and r[0]]
            if not servers:
                return None

            # Count assigned active creators for each active server
            server_counts = {sid: 0 for sid in servers}
            cur.execute(
                f"""
                SELECT assigned_worker_id, COUNT(*) 
                FROM creator_watchlist 
                WHERE active = {active_val} AND assigned_worker_id IS NOT NULL 
                GROUP BY assigned_worker_id
                """
            )
            for r in cur.fetchall():
                sid, cnt = r[0], r[1]
                if sid in server_counts:
                    server_counts[sid] = cnt

            # Pick active server with minimum assigned creators (tie-break on lowest id)
            least_loaded_server_id = min(servers, key=lambda s: (server_counts[s], s))

            cur.execute(
                f"UPDATE creator_watchlist SET assigned_worker_id = {placeholder} WHERE username = {placeholder}",
                (least_loaded_server_id, clean_user),
            )
            logger.info("Assigned creator @%s to least-loaded server #%d (%d active)",
                        clean_user, least_loaded_server_id, server_counts[least_loaded_server_id] + 1)
            return least_loaded_server_id
    except Exception as exc:
        logger.warning("assign_creator_to_least_loaded_worker error for %s: %s", clean_user, exc)
        return None


def get_active_watchlist_for_worker(worker_id: int | None = None) -> list[str]:
    """
    Returns active creator usernames assigned to a specific child worker.
    If worker_id is None, or if no child servers are registered in DB, returns all active creators.
    """
    try:
        active_val = "TRUE" if USE_POSTGRES else "1"
        placeholder = "%s" if USE_POSTGRES else "?"
        with get_db_cursor() as cur:
            # Check if cluster mode is active (any child servers registered)
            cur.execute(f"SELECT COUNT(*) FROM child_servers WHERE active = {active_val}")
            server_count = cur.fetchone()[0] or 0

            if server_count == 0 or worker_id is None:
                # Standalone mode: inspect all active creators
                cur.execute(f"SELECT username FROM creator_watchlist WHERE active = {active_val} ORDER BY id ASC")
                rows = cur.fetchall()
                return [r[0] for r in rows if r and r[0]]

            # Sharded cluster mode:
            cur.execute(
                f"""
                SELECT username FROM creator_watchlist 
                WHERE active = {active_val} AND assigned_worker_id = {placeholder} 
                ORDER BY id ASC
                """,
                (worker_id,),
            )
            rows = cur.fetchall()
            return [r[0] for r in rows if r and r[0]]
    except Exception as exc:
        logger.warning("get_active_watchlist_for_worker error for worker %s: %s", worker_id, exc)
        return []


def get_cluster_status() -> dict:
    """Comprehensive cluster status report with load breakdown."""
    try:
        active_val = "TRUE" if USE_POSTGRES else "1"
        servers = get_active_child_servers()
        with get_db_cursor() as cur:
            cur.execute(f"SELECT username, assigned_worker_id FROM creator_watchlist WHERE active = {active_val} ORDER BY id ASC")
            creator_rows = cur.fetchall()

        total_creators = len(creator_rows)
        server_map = {}
        for s in servers:
            server_map[s["id"]] = {
                "id": s["id"],
                "name": s["name"],
                "url": s["url"],
                "uptimerobot_id": s["uptimerobot_id"],
                "render_service_id": s["render_service_id"],
                "creators": [],
            }

        unassigned = []
        for username, wid in creator_rows:
            if wid and wid in server_map:
                server_map[wid]["creators"].append(username)
            else:
                unassigned.append(username)

        return {
            "total_servers": len(servers),
            "total_creators": total_creators,
            "servers": list(server_map.values()),
            "unassigned": unassigned,
        }
    except Exception as exc:
        logger.warning("get_cluster_status error: %s", exc)
        return {"total_servers": 0, "total_creators": 0, "servers": [], "unassigned": []}



