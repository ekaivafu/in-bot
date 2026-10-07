"""
bot.py
──────
InstaLoader Telegram Bot — Full featured.

Features:
  • Admin panel (stats, broadcast, maintenance, channel, cookie alert)
  • Required channel join check with inline buttons
  • Download queue — max 2 concurrent (safe for Render 512MB free tier)
  • Cookie expiry auto-detection → maintenance ON + 10-min admin reminders
  • All errors reported to admin only (users see only friendly messages)
  • Persistent stats via SQLite (database.py)
"""

import asyncio
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime
from functools import wraps
from pathlib import Path

from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InputMediaPhoto,
)
from telegram.constants import ParseMode, ChatAction
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from database import (
    init_db,
    upsert_user,
    log_download,
    get_stats,
    get_all_user_ids,
    get_setting,
    set_setting,
    add_channel,
    remove_channel,
    update_channel_link,
    get_all_channels,
    get_channel_count,
    clear_all_channels,
    get_cached_media,
    set_cached_media,
    update_cached_audio,
    register_channel_change_callback,
    add_watchlist_creator,
    remove_watchlist_creator,
    get_active_watchlist,
    get_pending_viral_reels,
    mark_viral_reel_dispatched,
    get_subscribers_for_creator,
    get_user_scout_limit,
    set_user_scout_limit,
    get_user_info,
    get_user_id_by_username,
    get_all_custom_limits,
    get_user_watchlist,
    get_user_watchlist_count,
    add_user_watchlist_creator,
    remove_user_watchlist_creator,
    add_child_server,
    remove_child_server,
    get_child_server,
    get_active_child_servers,
    rebalance_creator_workload,
    get_cluster_status,
)
import cloud_manager
from downloader import (
    cleanup_session,
    download_instagram,
    is_instagram_url,
    convert_json_cookies_to_netscape,
    extract_shortcode,
    extract_audio_from_video,
    check_cookies_health,
    DOWNLOAD_DIR,
)
import html


def safe_html(text: any) -> str:
    """Safely escape text for HTML parse mode, handling None gracefully."""
    if text is None:
        return ""
    return html.escape(str(text))

from emojis import (
    E_FLAME_BUTTERFLY,
    E_WHITE_BUTTERFLY,
    E_ARC_REACTOR,
    E_CONFETTI,
    E_SPARKLES,
    E_LIGHTNING,
    E_PINK_BOW,
    E_COLOR_DOTS,
    E_NEON_RINGS,
    E_RING_LOADER,
    E_GOLDEN_MAZE,
    E_RED_WOLF,
    E_BLUE_WOLF,
    E_DARK_SHADOW,
    E_BLACK_MASK,
    E_HEART_BORDER,
    E_BROKEN_HEART,
    E_ARROW,
    E_WARNING,
    E_HEART_PULSE,
    E_SKULL,
    E_BABY_NEON,
    E_DARK_CAT,
    E_HEART_RED,
    E_HEART_FIRE,
    E_CHECK_MARK,
    E_CROSS_MARK,
    E_FIRE_FLAME,
    E_ROCKET,
    E_DIAMOND,
    E_CHART_BAR,
    E_SHIELD,
    E_STAR_GLOW,
    E_CLOCK_TIME,
    E_PIN_LINK,
    E_TV_SCREEN,
)

# ── Bootstrap ──────────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
BOT_TOKEN          = os.getenv("BOT_TOKEN", "").strip()
_raw_admin         = os.getenv("ADMIN_ID", "").strip()
REQUIRED_CHANNEL   = os.getenv("REQUIRED_CHANNEL", "").strip()   # e.g. @mychannel

ADMIN_IDS: set[int] = set()
for _part in _raw_admin.replace(";", ",").split(","):
    _part = _part.strip()
    if _part and (_part.isdigit() or (_part.startswith("-") and _part[1:].isdigit())):
        ADMIN_IDS.add(int(_part))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set in .env")
if not ADMIN_IDS:
    raise RuntimeError("ADMIN_ID not set or invalid in .env")

ADMIN_ID = sorted(list(ADMIN_IDS))[0]   # Primary admin ID for fallback

# ── Queue Config (tuned for Render 512 MB free tier) ──────────────────────────
# Each yt-dlp + ffmpeg process ~100-150 MB → 2 concurrent = ~300 MB + bot overhead
MAX_CONCURRENT = 2
MAX_QUEUE      = 6   # max users allowed to wait; beyond this → "try later"

_semaphore: asyncio.Semaphore | None = None   # initialised in post_init or get_download_semaphore
_waiting_count: int = 0                        # users queued but not yet downloading
_active_count: int = 0                         # users actively downloading


def get_download_semaphore() -> asyncio.Semaphore:
    """Return download semaphore, lazily initializing if needed."""
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    return _semaphore


# ── Helpers ────────────────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def admin_only(func):
    """Decorator: silently reject non-admins."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.effective_message.reply_text(f"{E_WARNING} <b>Admin only.</b>", parse_mode=ParseMode.HTML)
            return
        return await func(update, context)
    return wrapper


def is_maintenance() -> bool:
    return get_setting("maintenance_mode", "0") == "1"


def get_channel() -> str:
    """DB value (admin-set) takes precedence over env var."""
    return get_setting("required_channel", REQUIRED_CHANNEL).strip()


def get_channel_display_name() -> str:
    title = get_setting("required_channel_name", "")
    if title:
        return title
    ch = get_channel()
    return ch if ch else ""


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def alert_admin(bot, text: str) -> None:
    """Send a message to all configured admins (never raises)."""
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(
                chat_id=aid,
                text=text,
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.error("alert_admin failed for %s: %s", aid, exc)


# ── Channel Membership Cache (makes /start instant) ───────────────────────────
_membership_cache: dict[int, tuple[float, bool]] = {}
MEMBERSHIP_CACHE_TTL = 300  # 5 minutes


def is_user_cached_member(user_id: int) -> bool:
    entry = _membership_cache.get(user_id)
    if entry:
        ts, is_member = entry
        if time.time() - ts < MEMBERSHIP_CACHE_TTL and is_member:
            return True
    return False


def cache_user_membership(user_id: int, is_member: bool) -> None:
    _membership_cache[user_id] = (time.time(), is_member)


def clear_membership_cache() -> None:
    _membership_cache.clear()
    logger.info("Cleared user channel membership cache")


register_channel_change_callback(clear_membership_cache)


async def check_channel_membership(bot, user_id: int, channel: str) -> bool:
    """Check if user has joined the required channel."""
    if not channel:
        return True
    try:
        chat_id = int(channel) if (channel.startswith("-") or channel.isdigit()) else channel
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        # Status can be: 'creator', 'administrator', 'member', 'restricted', 'left', 'kicked'
        return member.status in ("creator", "administrator", "member", "restricted")
    except TelegramError as exc:
        err_msg = str(exc).lower()
        if any(term in err_msg for term in ("user not found", "participant", "left", "kicked")):
            return False
        logger.warning("Channel check error (%s) for user %s: %s", channel, user_id, exc)
        return False
    except Exception as exc:
        logger.warning("Unexpected error during channel check: %s", exc)
        return False


async def get_unjoined_channels(bot, user_id: int) -> list[dict]:
    """Check membership for all required channels in parallel with in-memory caching."""
    if is_user_cached_member(user_id):
        return []

    try:
        channels = get_all_channels()
        if not channels:
            cache_user_membership(user_id, True)
            return []

        async def _check_one(ch: dict) -> tuple[dict, bool]:
            cid = ch.get("chat_id")
            if not cid:
                return ch, True
            try:
                ok = await asyncio.wait_for(check_channel_membership(bot, user_id, str(cid)), timeout=2.0)
                return ch, ok
            except Exception:
                return ch, False

        results = await asyncio.gather(*[_check_one(ch) for ch in channels], return_exceptions=True)
        unjoined = []
        for res in results:
            if isinstance(res, tuple):
                ch, ok = res
                if not ok:
                    unjoined.append(ch)

        if not unjoined:
            cache_user_membership(user_id, True)

        return unjoined
    except Exception as exc:
        logger.warning("get_unjoined_channels failed: %s", exc)
        return []


# ── Job: Cookie Reminder ───────────────────────────────────────────────────────
async def cookie_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs every 10 minutes while cookie_alert_active == '1'."""
    if get_setting("cookie_alert_active", "0") != "1":
        context.job.schedule_removal()
        return
    await alert_admin(
        context.bot,
        f"{E_WARNING} <b>Cookie Reminder</b>\n\n"
        "Instagram cookies are still expired or invalid.\n\n"
        f"{E_ARROW} <b>Fastest fix:</b>\n"
        "Send your fresh <code>cookies.txt</code> or <code>.json</code> directly to this chat!\n"
        "The bot will automatically update and resume downloads.\n\n"
        f"{E_CLOCK_TIME} <code>{now_str()}</code>",
    )


async def cookie_health_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Proactively checks Instagram cookie validity every 30 minutes."""
    if get_setting("cookie_alert_active", "0") == "1":
        return

    is_healthy, reason = check_cookies_health()
    if not is_healthy:
        logger.warning("Cookie health check failed: %s", reason)
        set_setting("maintenance_mode", "1")
        set_setting("cookie_alert_active", "1")
        start_cookie_reminder(context.job_queue)
        await alert_admin(
            context.bot,
            f"{E_WARNING} <b>Cookie Health Alert — Issue Detected!</b>\n\n"
            f"The 30-minute health monitor detected a cookie issue:\n"
            f"<b>Reason:</b> <code>{html.escape(reason)}</code>\n\n"
            f"{E_ARC_REACTOR} <b>Maintenance mode auto-enabled.</b>\n"
            f"{E_CLOCK_TIME} Admin reminders will be sent every 10 minutes.\n\n"
            f"{E_ARROW} <b>Fastest fix:</b>\n"
            f"Send your fresh <code>cookies.txt</code> or <code>.json</code> directly to this chat!",
        )
    else:
        logger.info("30-min cookie health check passed: %s", reason)


def start_cookie_reminder(job_queue) -> None:
    if not job_queue.get_jobs_by_name("cookie_reminder"):
        job_queue.run_repeating(
            cookie_reminder_job,
            interval=600,
            first=30,
            name="cookie_reminder",
        )
        logger.info("Cookie reminder started (every 10 min)")


def stop_cookie_reminder(job_queue) -> None:
    for job in job_queue.get_jobs_by_name("cookie_reminder"):
        job.schedule_removal()
    set_setting("cookie_alert_active", "0")
    logger.info("Cookie reminder stopped")


# ── Reply Keyboard Button Labels ──────────────────────────────────────────────
# User buttons
BTN_HELP        = "📖 Help"
BTN_WATCHLIST   = "🎯 My Watchlist"

# Admin buttons
BTN_STATS       = "📊 Stats"
BTN_MAINT       = "🔧 Maintenance"
BTN_BROADCAST   = "📢 Broadcast"
BTN_CHANNELS    = "📺 Channels"
BTN_COOKIE      = "🍪 Cookie Status"
BTN_SERVERS     = "⚡ Servers"
BTN_SCOUT       = "🎯 Scout"
BTN_CLOSE_MENU  = "❌ Close Menu"

ALL_BTNS = {
    BTN_HELP, BTN_WATCHLIST,
    BTN_STATS, BTN_MAINT, BTN_BROADCAST,
    BTN_CHANNELS, BTN_COOKIE, BTN_SERVERS, BTN_SCOUT, BTN_CLOSE_MENU,
}


# ── Reply Keyboard Builders ────────────────────────────────────────────────────
def rkb_user() -> ReplyKeyboardMarkup:
    """Persistent bottom keyboard for regular users."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_WATCHLIST), KeyboardButton(BTN_HELP)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Paste an Instagram link or tap Watchlist...",
    )


def kb_cancel() -> InlineKeyboardMarkup:
    """Reusable inline cancel button for prompts."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_cancel")]])


def rkb_admin() -> ReplyKeyboardMarkup:
    """Persistent bottom keyboard for the admin."""
    maint_label = "🔧 Maintenance: ON" if is_maintenance() else "🔧 Maintenance: OFF"
    cookie_label = "🍪 Cookie: 🔴 Alert" if get_setting("cookie_alert_active", "0") == "1" else "🍪 Cookie: ✅ OK"
    chan_count = get_channel_count()
    chan_label = f"📺 Channels ({chan_count})"
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_STATS),       KeyboardButton(BTN_BROADCAST)],
            [KeyboardButton(BTN_SERVERS),     KeyboardButton(BTN_SCOUT)],
            [KeyboardButton(maint_label),     KeyboardButton(chan_label)],
            [KeyboardButton(cookie_label),    KeyboardButton(BTN_CLOSE_MENU)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Admin mode active...",
    )


# ── Watchlist, Cluster, and Scout Inline UI Builders ──────────────────────────
def build_watchlist_text(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    """Generate dynamic text and interactive inline keyboard for personal watchlist."""
    admin_flag = is_admin(user_id)
    user_limit = get_user_scout_limit(user_id)
    my_creators = get_user_watchlist(user_id)
    limit_str = "Unlimited 👑" if admin_flag else f"{user_limit}"

    text = (
        f"🎯 <b>Your Creator Watchlist</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 <b>Active Slots:</b> <b>{len(my_creators)} / {limit_str}</b>\n\n"
    )
    if my_creators:
        text += "<b>Monitored Creators:</b>\n"
        text += "\n".join(f"  • @{html.escape(c)}" for c in my_creators) + "\n\n"
        text += "🚀 <i>Viral reels (5k+ likes, 0–5 days) from these accounts will be auto-delivered here!</i>\n"
    else:
        text += "<i>You are not monitoring any creators yet.</i>\n\n"
        text += "Tap <b>➕ Add Creator</b> below to start monitoring!\n"

    buttons = []
    row1 = [InlineKeyboardButton("➕ Add Creator", callback_data="watch_add_btn")]
    if my_creators:
        row1.append(InlineKeyboardButton("🗑️ Remove Creator", callback_data="watch_del_menu"))
    buttons.append(row1)
    buttons.append([InlineKeyboardButton("🔄 Refresh", callback_data="watch_refresh")])

    return text, InlineKeyboardMarkup(buttons)


def kb_watchlist_delete(user_id: int) -> InlineKeyboardMarkup:
    """Generate inline buttons for each creator on user's watchlist to remove with 1 tap."""
    my_creators = get_user_watchlist(user_id)
    buttons = []
    for c in my_creators:
        buttons.append([InlineKeyboardButton(f"❌ Remove @{c}", callback_data=f"watch_rm:{c}")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Watchlist", callback_data="watch_refresh")])
    return InlineKeyboardMarkup(buttons)


def build_cluster_text() -> tuple[str, InlineKeyboardMarkup]:
    """Generate dynamic cluster status text and interactive buttons."""
    status = get_cluster_status()
    servers = status["servers"]
    total_creators = status["total_creators"]
    unassigned = status["unassigned"]

    lines = [
        "⚡ <b>Child Server Cluster & Load Balancing</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🖥️ <b>Active Servers:</b> {len(servers)}",
        f"👥 <b>Total Monitored Creators:</b> {total_creators}\n",
    ]

    if not servers:
        lines.append("<i>No child servers registered. Workers operate in standalone mode.</i>\n")
        lines.append("💡 <i>Tap <b>➕ Add Server URL</b> or <b>🚀 1-Click Deploy</b> below to register a server!</i>\n")
    else:
        for s in servers:
            c_list = s["creators"]
            count = len(c_list)
            pct = f"({count/total_creators*100:.0f}%)" if total_creators > 0 else ""
            uptime_info = f"✅ Monitored (ID: <code>{s['uptimerobot_id']}</code>)" if s["uptimerobot_id"] else "⚠️ No UptimeRobot ID"
            c_str = ", ".join(f"@{c}" for c in c_list) if c_list else "<i>None assigned</i>"
            lines.append(
                f"<b>Server #{s['id']}: {html.escape(s['name'])}</b>\n"
                f"  🔗 URL: <code>{html.escape(s['url'] or 'N/A')}</code>\n"
                f"  🤖 Uptime: {uptime_info}\n"
                f"  📊 Workload: <b>{count} creators</b> {pct}\n"
                f"  👥 Accounts: {c_str}\n"
            )

        if unassigned:
            lines.append(f"⚠️ <b>Unassigned Creators ({len(unassigned)}):</b> {', '.join('@'+c for c in unassigned)}")
            lines.append("<i>Tap <b>⚖️ Rebalance Workload</b> to distribute unassigned creators evenly.</i>\n")

    buttons = [
        [
            InlineKeyboardButton("➕ Add Server URL", callback_data="adm_srv_add_btn"),
            InlineKeyboardButton("🚀 1-Click Deploy", callback_data="adm_srv_deploy_btn"),
        ],
        [
            InlineKeyboardButton("⚖️ Rebalance Workload", callback_data="adm_srv_rebalance"),
        ],
    ]
    if servers:
        buttons[1].append(InlineKeyboardButton("🗑️ Remove Server", callback_data="adm_srv_del_menu"))
    buttons.append([
        InlineKeyboardButton("🔄 Refresh", callback_data="adm_srv_refresh"),
        InlineKeyboardButton("⬅️ Admin Panel", callback_data="adm_panel"),
    ])

    return "\n".join(lines), InlineKeyboardMarkup(buttons)


def kb_cluster_delete() -> InlineKeyboardMarkup:
    """Generate inline buttons for each active child server to delete with 1 tap."""
    servers = get_active_child_servers()
    buttons = []
    for s in servers:
        buttons.append([InlineKeyboardButton(f"❌ #{s['id']}: {s['name'][:18]}", callback_data=f"adm_srv_del:{s['id']}")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Servers", callback_data="adm_srv_refresh")])
    return InlineKeyboardMarkup(buttons)


def build_scout_text() -> tuple[str, InlineKeyboardMarkup]:
    """Generate dynamic scout overview text and interactive buttons."""
    creators = get_active_watchlist()
    pending = get_pending_viral_reels(limit=5)
    servers = get_active_child_servers()

    text = (
        "🎯 <b>Viral Reel Scout System</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 <b>Active Watchlist:</b> {len(creators)} creators\n"
        f"🖥️ <b>Connected Servers:</b> {len(servers)} servers\n"
    )
    if creators:
        text += "   " + ", ".join(f"@{html.escape(c)}" for c in creators[:8])
        if len(creators) > 8:
            text += f" (+{len(creators)-8} more)"
        text += "\n"
    else:
        text += "   <i>None yet.</i>\n"

    text += f"\n📥 <b>Pending Queued Viral Reels:</b> {len(pending)}\n\n"
    text += "<i>Use buttons below to manage target creators and workers:</i>"

    buttons = []
    if pending:
        buttons.append([
            InlineKeyboardButton(f"🚀 Send Pending Reels ({len(pending)})", callback_data="adm_scout_dispatch")
        ])
    row_add = [InlineKeyboardButton("➕ Add Creator", callback_data="adm_scout_add_btn")]
    if creators:
        row_add.append(InlineKeyboardButton("🗑️ Remove Creator", callback_data="adm_scout_del_menu"))
    buttons.append(row_add)
    buttons.append([
        InlineKeyboardButton("⚙️ Set User Slot Limit", callback_data="adm_setlimit_prompt"),
        InlineKeyboardButton("📋 View User Limits", callback_data="adm_view_limits"),
    ])
    buttons.append([
        InlineKeyboardButton("⚡ Manage Servers", callback_data="adm_srv_menu"),
        InlineKeyboardButton("🔄 Refresh", callback_data="adm_scout_refresh"),
    ])
    buttons.append([InlineKeyboardButton("⬅️ Admin Panel", callback_data="adm_panel")])

    return text, InlineKeyboardMarkup(buttons)


def kb_scout_delete() -> InlineKeyboardMarkup:
    """Generate inline buttons for creators in scout watchlist to remove with 1 tap."""
    creators = get_active_watchlist()
    buttons = []
    for c in creators[:20]:
        buttons.append([InlineKeyboardButton(f"❌ Remove @{c}", callback_data=f"adm_scout_del:{c}")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Scout", callback_data="adm_scout_refresh")])
    return InlineKeyboardMarkup(buttons)


# ── Keyboards (inline) ─────────────────────────────────────────────────────────
def kb_video_actions(shortcode: str) -> InlineKeyboardMarkup:
    """Inline button below delivered video for 1-tap audio extraction."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎵 Extract Audio", callback_data=f"audio:{shortcode}")]
    ])


def get_media_file_id(sent_msg) -> str:
    """Extract file_id whether sent as video, document, or animation."""
    if not sent_msg:
        return ""
    if getattr(sent_msg, "video", None):
        return sent_msg.video.file_id
    if getattr(sent_msg, "document", None):
        return sent_msg.document.file_id
    if getattr(sent_msg, "animation", None):
        return sent_msg.animation.file_id
    return ""


async def _send_video_resilient(
    bot,
    chat_id: int,
    video_path: Path,
    caption: str,
    reply_markup: InlineKeyboardMarkup | None = None,
):
    """
    Sends video with robust multi-tier fallback:
    Tier 1: send_video with HTML parse_mode
    Tier 2: send_video without parse_mode (if HTML parsing fails)
    Tier 3: send_document fallback (Telegram NEVER rejects MP4 files up to 50MB as documents)
    """
    # Tier 1: send_video (HTML)
    try:
        with open(video_path, "rb") as vf:
            sent = await bot.send_video(
                chat_id=chat_id,
                video=vf,
                caption=caption,
                parse_mode=ParseMode.HTML,
                supports_streaming=True,
                reply_markup=reply_markup,
                write_timeout=180,
                read_timeout=120,
                connect_timeout=30,
            )
            return sent
    except TelegramError as e1:
        logger.warning("Tier 1 send_video failed: %s. Trying Tier 2 (plain text)...", e1)

    # Tier 2: send_video (plain text without HTML tags)
    clean_caption = re.sub(r"<[^>]+>", "", caption)
    try:
        with open(video_path, "rb") as vf:
            sent = await bot.send_video(
                chat_id=chat_id,
                video=vf,
                caption=clean_caption,
                supports_streaming=True,
                reply_markup=reply_markup,
                write_timeout=180,
                read_timeout=120,
                connect_timeout=30,
            )
            return sent
    except TelegramError as e2:
        logger.warning("Tier 2 send_video failed: %s. Trying Tier 3 (send_document)...", e2)

    # Tier 3: send_document fallback
    with open(video_path, "rb") as vf:
        sent = await bot.send_document(
            chat_id=chat_id,
            document=vf,
            filename=video_path.name,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            write_timeout=240,
            read_timeout=120,
            connect_timeout=30,
        )
        return sent


def kb_admin_panel() -> InlineKeyboardMarkup:
    maint       = "🟢 ON" if is_maintenance() else "⚫ OFF"
    cookie_flag = get_setting("cookie_alert_active", "0") == "1"
    chan_count  = get_channel_count()
    chan_label  = f"📺 Channels: {chan_count} Active" if chan_count > 0 else "📺 Channels: None ⚠️"
    srv_count   = len(get_active_child_servers())
    srv_label   = f"⚡ Servers ({srv_count})" if srv_count > 0 else "⚡ Servers: None"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Stats",           callback_data="adm_stats"),
            InlineKeyboardButton("📢 Broadcast",       callback_data="adm_broadcast"),
        ],
        [
            InlineKeyboardButton(srv_label,            callback_data="adm_srv_menu"),
            InlineKeyboardButton("🎯 Viral Scout",     callback_data="adm_scout_view"),
        ],
        [
            InlineKeyboardButton(f"🔧 Maintenance: {maint}", callback_data="adm_maint_toggle"),
        ],
        [
            InlineKeyboardButton(chan_label, callback_data="adm_chan_menu"),
        ],
        [
            InlineKeyboardButton(
                "🍪 Clear Cookie Alert 🔴" if cookie_flag else "🍪 Cookie: OK ✅",
                callback_data="adm_cookie_clear",
            ),
        ],
    ])


def build_channel_list_text() -> str:
    channels = get_all_channels()
    if not channels:
        return (
            f"{E_NEON_RINGS} <b>Force-Sub Channel Management</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{E_WARNING} <b>No required channels configured yet.</b>\n\n"
            "Users can download without joining any channel.\n"
            f"{E_ARROW} Tap <b>➕ Add Channel</b> below to add one!"
        )

    lines = [
        f"{E_NEON_RINGS} <b>Force-Sub Channel Management</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Active Required Channels: <b>{len(channels)}</b>\n",
    ]
    for i, ch in enumerate(channels, 1):
        cid = ch["chat_id"]
        title = html.escape(ch["title"])
        link = ch["invite_link"].strip()
        link_str = f'<a href="{html.escape(link)}">Invite Link</a>' if link else f"{E_WARNING} <i>No link set (use /setlink)</i>"
        lines.append(f"{E_HEART_BORDER} <b>{title}</b>")
        lines.append(f"   • Chat ID: <code>{cid}</code>")
        lines.append(f"   • Link: {link_str}\n")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"{E_ARROW} Choose an action below:")
    return "\n".join(lines)


def kb_channel_management() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add Channel",       callback_data="adm_chan_add"),
            InlineKeyboardButton("🗑️ Remove Channel",   callback_data="adm_chan_del_menu"),
        ],
        [
            InlineKeyboardButton("🔗 Set / Update Link", callback_data="adm_chan_link_menu"),
        ],
        [
            InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="adm_panel"),
        ],
    ])


def kb_channel_delete_menu() -> InlineKeyboardMarkup:
    channels = get_all_channels()
    buttons = []
    for ch in channels:
        cid = ch["chat_id"]
        title = ch["title"]
        label = f"❌ {title[:20]}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"adm_del_ch:{cid}")])

    if channels:
        buttons.append([InlineKeyboardButton("🗑️ Clear ALL Channels", callback_data="adm_del_all")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu")])
    return InlineKeyboardMarkup(buttons)


def kb_channel_link_menu() -> InlineKeyboardMarkup:
    channels = get_all_channels()
    buttons = []
    for ch in channels:
        cid = ch["chat_id"]
        title = ch["title"]
        has_link = "✅" if ch["invite_link"].strip() else "⚠️"
        label = f"🔗 {has_link} {title[:20]}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"adm_link_ch:{cid}")])

    buttons.append([InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu")])
    return InlineKeyboardMarkup(buttons)


def kb_force_sub(unjoined_channels: list[dict]) -> InlineKeyboardMarkup:
    """Build inline keyboard for all unjoined channels + Verify button."""
    buttons = []
    for i, ch in enumerate(unjoined_channels, 1):
        title = ch.get("title", f"Channel {i}")
        link = ch.get("invite_link", "").strip()

        # If no invite_link stored, try to build one from chat_id
        if not link:
            cid = str(ch.get("chat_id", "")).strip()
            if cid.startswith("@"):
                # Public channel username
                link = f"https://t.me/{cid.lstrip('@')}"
            # Numeric chat_id (private channel) — no link available, skip button
            # Admin must set a link with /setlink or via Channels > Set Link

        if link:
            btn_text = f"📢 Join {title}" if len(title) <= 24 else f"📢 Join Channel {i}"
            buttons.append([InlineKeyboardButton(btn_text, url=link)])
        else:
            # Show a disabled-style notice so the user knows a channel exists
            buttons.append([InlineKeyboardButton(f"⚠️ {title[:22]} (link missing)", callback_data="no_link_notice")])

    buttons.append([InlineKeyboardButton("✅ I've Joined All — Continue", callback_data="check_joined")])
    return InlineKeyboardMarkup(buttons)


def kb_back_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")]])


# ── Stats text helper ──────────────────────────────────────────────────────────
def build_stats_text() -> str:
    s          = get_stats()
    maint      = f"{E_CHECK_MARK} <b>ON</b>" if is_maintenance() else f"{E_CROSS_MARK} <b>OFF</b>"
    cookie     = f"{E_WARNING} Alert active" if get_setting("cookie_alert_active", "0") == "1" else f"{E_CHECK_MARK} OK"
    channels   = get_all_channels()
    ch_count   = len(channels)
    ch_summary = f"{ch_count} Active" if ch_count > 0 else "None"
    q_active   = _active_count
    return (
        f"{E_ARC_REACTOR} <b>Bot Statistics & Health</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{E_COLOR_DOTS} <b>Users</b>\n"
        f"  • Total registered : <code>{s['total_users']:,}</code>\n"
        f"  • Active today     : <code>{s['active_today']:,}</code>\n\n"
        f"{E_NEON_RINGS} <b>Downloads</b>\n"
        f"  • Today            : <code>{s['today_ok']:,}</code> {E_CONFETTI}   <code>{s['today_fail']:,}</code> {E_BROKEN_HEART}\n"
        f"  • All time         : <code>{s['total_ok']:,}</code> {E_CONFETTI}   <code>{s['total_fail']:,}</code> {E_BROKEN_HEART}\n\n"
        f"{E_HEART_PULSE} <b>System</b>\n"
        f"  • Active downloads : <code>{q_active}/{MAX_CONCURRENT}</code>\n"
        f"  • Queue waiting    : <code>{_waiting_count}</code>\n"
        f"  • Maintenance      : {maint}\n"
        f"  • Cookie status    : {cookie}\n"
        f"  • Required Channels: <b>{ch_summary}</b>\n\n"
        f"{E_CLOCK_TIME} <code>{now_str()}</code>"
    )


# ── Command Handlers ───────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message or update.message
    if not message:
        return

    user = update.effective_user
    if not user:
        return

    # Non-blocking async DB write (0ms delay for user response!)
    asyncio.create_task(asyncio.to_thread(upsert_user, user.id, user.username, user.first_name))

    # Instant typing feedback (< 0.1s!)
    try:
        await context.bot.send_chat_action(message.chat_id, ChatAction.TYPING)
    except Exception:
        pass

    name = safe_html(user.first_name or user.username or "Friend")

    try:
        # 1. Admin greeting & controls
        if is_admin(user.id):
            channels = get_all_channels()
            ch_count = len(channels)
            missing_links = [
                safe_html(c.get("title") or c.get("chat_id"))
                for c in channels
                if not (c.get("invite_link") or "").strip()
            ]
            link_warn = ""
            if missing_links:
                link_warn = f"\n\n{E_WARNING} <i>Notice:</i> {len(missing_links)} channel(s) need an invite link: {', '.join(missing_links)}. Use /setlink or tap Channels below."

            await message.reply_text(
                f"{E_GOLDEN_MAZE} <b>Admin Control Panel</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Welcome back, <b>{name}</b>!\n"
                f"• Maintenance: <b>{'ON' if is_maintenance() else 'OFF'}</b>\n"
                f"• Required Channels: <b>{ch_count} Active</b>{link_warn}\n\n"
                f"{E_ARROW} Select an option below or send /admin for inline controls.",
                parse_mode=ParseMode.HTML,
                reply_markup=rkb_admin(),
            )
            return

        # 2. Regular user: check membership across all required channels
        unjoined = []
        try:
            unjoined = await asyncio.wait_for(get_unjoined_channels(context.bot, user.id), timeout=4.0)
        except Exception:
            unjoined = []

        if unjoined:
            ch_list_str = "\n".join([f"  {E_HEART_BORDER} <b>{safe_html(c.get('title') or c.get('chat_id'))}</b>" for c in unjoined])
            await message.reply_text(
                f"{E_FLAME_BUTTERFLY} <b>Hey {name}! Welcome to InstaBot</b> {E_SPARKLES}\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "To download Instagram <b>Reels, Posts & IGTV</b>, "
                "you must first join our official channel(s):\n\n"
                f"{ch_list_str}\n\n"
                f"{E_ARROW} Tap <b>Join</b> for each channel below, then tap <b>I've Joined All</b> to unlock the bot:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_force_sub(unjoined),
            )
            return

        # 3. Regular user (all joined or no channel): send clean welcome
        await message.reply_text(
            f"{E_FLAME_BUTTERFLY} <b>Hey {name}! Welcome to InstaBot</b> {E_SPARKLES}\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "I download Instagram <b>Reels, Posts & IGTV</b> for you.\n\n"
            f"{E_SPARKLES} <b>Features:</b>\n"
            f"• Best available quality {E_LIGHTNING}\n"
            f"• Repost safe {E_BLACK_MASK} <i>(fresh metadata & unique hash)</i>\n"
            f"• 🎵 1-Tap Audio Extractor <i>(MP3 sound)</i>\n"
            f"• Monospace caption for 1-tap copy {E_DIAMOND}\n\n"
            f"{E_ARROW} <b>Paste any Instagram link to get started!</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=rkb_user(),
        )
    except Exception as exc:
        logger.exception("cmd_start encountered error: %s", exc)
        # Guaranteed fallback so user is NEVER left without a response
        try:
            await message.reply_text(
                f"{E_FLAME_BUTTERFLY} <b>Hey {name}! Welcome to InstaBot</b> {E_SPARKLES}\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "I download Instagram <b>Reels, Posts & IGTV</b> for you.\n\n"
                f"{E_ARROW} <b>Paste any Instagram link to get started!</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=rkb_admin() if is_admin(user.id) else rkb_user(),
            )
        except Exception:
            pass


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    channels = get_all_channels()
    channel_line = f"• Must join <b>{len(channels)} required channel(s)</b>\n" if channels else ""
    await update.message.reply_text(
        f"{E_ARC_REACTOR} <b>InstaBot — Help Guide</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{E_ARROW} <b>How to use:</b>\n"
        "1. Copy any public Instagram link\n"
        "2. Paste it in this chat\n"
        f"3. Get your video or photos in seconds {E_LIGHTNING}\n"
        "4. Tap <b>🎵 Extract Audio</b> below any video to get the MP3!\n\n"
        f"{E_SPARKLES} <b>Supported links:</b>\n"
        "<code>instagram.com/reel/...</code>\n"
        "<code>instagram.com/p/...</code>\n"
        "<code>instagram.com/tv/...</code>\n\n"
        f"{E_HEART_BORDER} <b>Requirements:</b>\n"
        f"{channel_line}"
        "• Public accounts only\n\n"
        f"{E_CONFETTI} <b>Features & Safety:</b>\n"
        "• Highest quality video / images\n"
        f"• Advanced anti-detection {E_BLACK_MASK} (fresh metadata & unique hash)\n"
        "• 🎵 1-Tap Audio Extractor (MP3 below video)\n"
        "• Caption in <code>monospace</code> for easy copy\n\n"
        f"{E_WARNING} Problems? Contact the admin.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Open admin panel — shows both reply keyboard + inline panel."""
    await update.message.reply_text(
        f"{E_GOLDEN_MAZE} <b>Admin Panel</b>\n"
        "<i>Select an option from the menu below:</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=rkb_admin(),
    )
    await update.message.reply_text(
        "Inline controls:",
        reply_markup=kb_admin_panel(),
    )


@admin_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(build_stats_text(), parse_mode=ParseMode.HTML)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
    context.user_data.pop("pending_link", None)
    context.user_data.pop("pending_ch_link", None)
    keyboard = rkb_admin() if is_admin(update.effective_user.id) else rkb_user()
    await update.message.reply_text(
        f"{E_WARNING} <b>Action cancelled.</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


# ── Animated Progress Helper ───────────────────────────────────────────────────
async def animate_progress(status_msg, stop_event: asyncio.Event) -> None:
    """
    Lightweight animated progress ticker with premium custom emojis.
    Zero extra RAM, strictly respects Telegram rate limits (1.2s - 2.0s intervals).
    """
    frames = [
        (f"{E_LIGHTNING} <b>Connecting to Instagram...</b>\n<code>[▰▱▱▱▱▱▱▱▱▱] 15%</code>\n<i>Initializing secure stream...</i>", 1.2),
        (f"{E_RING_LOADER} <b>Fetching Media Data...</b>\n<code>[▰▰▰▱▱▱▱▱▱▱] 35%</code>\n<i>Resolving highest quality source...</i>", 1.5),
        (f"{E_NEON_RINGS} <b>Downloading Media...</b>\n<code>[▰▰▰▰▰▰▱▱▱▱] 65%</code>\n<i>Bypassing restrictions...</i>", 1.8),
        (f"{E_DARK_SHADOW} <b>Processing & Cleaning Metadata...</b>\n<code>[▰▰▰▰▰▰▰▰▱▱] 85%</code>\n<i>Stripping tracking info for safety...</i>", 2.0),
    ]
    for text, delay in frames:
        try:
            if stop_event.is_set():
                break
            await asyncio.sleep(delay)
            if stop_event.is_set():
                break
            await status_msg.edit_text(text, parse_mode=ParseMode.HTML)
        except (TelegramError, Exception):
            pass


# ── Callback Handler ───────────────────────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q    = update.callback_query
    data = q.data
    uid  = q.from_user.id
    await q.answer()

    # ── Admin callbacks (guard every one) ─────────────────────────────────
    if data in ("adm_cancel", "cancel"):
        context.user_data.pop("state", None)
        context.user_data.pop("pending_link", None)
        context.user_data.pop("pending_ch_link", None)
        await q.answer("Action cancelled.")
        await q.edit_message_text(
            f"{E_WARNING} <b>Action cancelled.</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_admin_panel() if is_admin(uid) else None,
        )
        return

    if data == "no_link_notice":
        await q.answer(
            "⚠️ This channel's invite link hasn't been set yet.\n"
            "Please ask the admin to set it, or join manually.",
            show_alert=True,
        )
        return

    if data.startswith("adm_") and not is_admin(uid):
        await q.answer("⛔ Admin only.", show_alert=True)
        return

    if data == "adm_panel":
        await q.edit_message_text(
            f"{E_GOLDEN_MAZE} <b>Admin Panel</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_admin_panel(),
        )

    elif data == "adm_stats":
        await q.edit_message_text(
            build_stats_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back_admin(),
        )

    elif data == "adm_maint_toggle":
        new_val = "0" if is_maintenance() else "1"
        set_setting("maintenance_mode", new_val)
        if new_val == "0":
            stop_cookie_reminder(context.job_queue)
        label = f"{E_CHECK_MARK} ON" if new_val == "1" else f"{E_CROSS_MARK} OFF"
        await q.edit_message_text(
            f"{E_ARC_REACTOR} <b>Admin Panel</b>\n\n<i>Maintenance toggled → {label}</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_admin_panel(),
        )

    elif data == "adm_broadcast":
        context.user_data["state"] = "awaiting_broadcast"
        await q.edit_message_text(
            f"{E_RED_WOLF} <b>Broadcast</b>\n\n"
            "Send the message you want to broadcast to all users.\n"
            "Supports text, photos, videos — anything.\n\n"
            "Send /cancel to abort or tap below.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel(),
        )

    elif data == "adm_chan_menu":
        await q.edit_message_text(
            build_channel_list_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_management(),
            disable_web_page_preview=True,
        )

    elif data == "adm_chan_add":
        context.user_data["state"] = "awaiting_channel_add"
        context.user_data.pop("pending_ch_link", None)
        await q.edit_message_text(
            f"{E_NEON_RINGS} <b>Add Required Channel</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send any of the following:\n"
            "1️⃣ <b>Chat ID</b> (e.g. <code>-1001234567890</code>)\n"
            "2️⃣ <b>Username</b> (e.g. <code>@mychannel</code>)\n"
            "3️⃣ <b>Link</b> (e.g. <code>https://t.me/channel</code> or <code>-100... https://t.me/+...</code>)\n"
            "4️⃣ Or simply <b>forward any post from your channel</b> here!\n\n"
            f"{E_WARNING} <i>Make sure the bot is added as an Administrator in your channel first!</i>\n\n"
            "Send /cancel to abort or tap below.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu"), InlineKeyboardButton("❌ Cancel", callback_data="adm_cancel")]
            ]),
        )

    elif data == "adm_chan_del_menu":
        channels = get_all_channels()
        if not channels:
            await q.answer("No channels to remove!", show_alert=True)
            return
        await q.edit_message_text(
            f"{E_WARNING} <b>Remove Required Channel</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Tap a channel below to remove it from the force-sub requirement:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_delete_menu(),
        )

    elif data.startswith("adm_del_ch:"):
        target_cid = data.split(":", 1)[1]
        ok = remove_channel(target_cid)
        if ok:
            await q.answer("✅ Channel removed!", show_alert=False)
        else:
            await q.answer("Channel not found in database.", show_alert=True)
        channels = get_all_channels()
        if channels:
            await q.edit_message_text(
                f"{E_WARNING} <b>Remove Required Channel</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Tap a channel below to remove it from the force-sub requirement:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_channel_delete_menu(),
            )
        else:
            await q.edit_message_text(
                build_channel_list_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=kb_channel_management(),
            )

    elif data == "adm_del_all":
        clear_all_channels()
        await q.answer("🗑️ All channels removed!", show_alert=True)
        await q.edit_message_text(
            build_channel_list_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_management(),
        )

    elif data == "adm_chan_link_menu":
        channels = get_all_channels()
        if not channels:
            await q.answer("No channels configured yet! Add one first.", show_alert=True)
            return
        await q.edit_message_text(
            f"{E_NEON_RINGS} <b>Set / Update Channel Invite Link</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Select a channel to set or update its invite link:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_link_menu(),
        )

    elif data.startswith("adm_link_ch:"):
        target_cid = data.split(":", 1)[1]
        context.user_data["state"] = f"awaiting_ch_link:{target_cid}"
        channels = get_all_channels()
        target_ch = next((c for c in channels if str(c["chat_id"]) == str(target_cid)), None)
        ch_title = target_ch["title"] if target_ch else target_cid
        cur_link = target_ch["invite_link"] if target_ch and target_ch["invite_link"] else "None"
        await q.edit_message_text(
            f"{E_NEON_RINGS} <b>Update Invite Link</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{E_HEART_BORDER} Channel: <b>{html.escape(ch_title)}</b>\n"
            f"{E_PIN_LINK} Chat ID: <code>{target_cid}</code>\n"
            f"{E_PIN_LINK} Current Link: <code>{html.escape(cur_link)}</code>\n\n"
            "Send the new Telegram invite link (e.g. <code>https://t.me/+...</code> or <code>https://t.me/channel</code>).\n"
            "Send <code>-</code> to clear the link.\n"
            "Send /cancel to abort or tap below.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu"), InlineKeyboardButton("❌ Cancel", callback_data="adm_cancel")]
            ]),
        )

    elif data == "adm_cookie_clear":
        stop_cookie_reminder(context.job_queue)
        await q.edit_message_text(
            f"{E_SPARKLES} <b>Cookie alert cleared.</b>\n\n"
            "Make sure you've updated <code>cookies.txt</code> on Render, then turn off Maintenance.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back_admin(),
        )

    # ── User Watchlist Interactive Callbacks ──────────────────────────────
    elif data == "watch_refresh":
        text, kb = build_watchlist_text(uid)
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "watch_add_btn":
        context.user_data["state"] = "awaiting_user_watch_add"
        await q.message.reply_html(
            f"{E_NEON_RINGS} <b>Add Creator to Watchlist</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send the Instagram username to monitor:\n"
            "Example: <code>@nike</code> or <code>cristiano</code>\n\n"
            "Send /cancel to abort or tap below.",
            reply_markup=kb_cancel(),
        )

    elif data == "watch_del_menu":
        my_creators = get_user_watchlist(uid)
        if not my_creators:
            await q.answer("Your watchlist is already empty!", show_alert=True)
            return
        await q.edit_message_text(
            "🗑️ <b>Remove Creator from Watchlist</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Tap a creator below to remove them from your monitored list:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_watchlist_delete(uid),
        )

    elif data.startswith("watch_rm:"):
        creator = data.split(":", 1)[1].strip()
        remove_user_watchlist_creator(uid, creator)
        await q.answer(f"Removed @{creator} from watchlist.")
        text, kb = build_watchlist_text(uid)
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    # ── Admin Cluster & Child Server Callbacks ─────────────────────────────
    elif data in ("adm_srv_menu", "adm_srv_refresh"):
        text, kb = build_cluster_text()
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

    elif data == "adm_srv_add_btn":
        context.user_data["state"] = "awaiting_server_url"
        await q.message.reply_html(
            "🖥️ <b>Add Existing Child Server URL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send your Render child server URL:\n"
            "Example: <code>https://instabot-worker-2.onrender.com Worker2</code>\n\n"
            "Send /cancel to abort or tap below.",
            reply_markup=kb_cancel(),
        )

    elif data == "adm_srv_deploy_btn":
        context.user_data["state"] = "awaiting_render_api_key"
        repo_disp = os.getenv("GITHUB_REPO_URL", "Not set in .env")
        await q.message.reply_html(
            f"🚀 <b>1-Click Auto Deploy on Render</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send your Render API Key (from Render Account Settings → API Keys):\n"
            "Example: <code>rnd_xxxxxxxxxxxx</code>\n\n"
            f"🌐 <i>Using repo: <code>{html.escape(repo_disp)}</code></i>\n\n"
            "Send /cancel to abort or tap below.",
            reply_markup=kb_cancel(),
            disable_web_page_preview=True,
        )

    elif data == "adm_srv_rebalance":
        rebalance_creator_workload()
        await q.answer("⚖️ Creators rebalanced evenly across all active servers!", show_alert=True)
        text, kb = build_cluster_text()
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

    elif data == "adm_srv_del_menu":
        servers = get_active_child_servers()
        if not servers:
            await q.answer("No active child servers to remove!", show_alert=True)
            return
        await q.edit_message_text(
            "🗑️ <b>Remove Child Server</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Tap a server below to remove it from cluster and redistribute accounts:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cluster_delete(),
        )

    elif data.startswith("adm_srv_del:"):
        try:
            target_sid = int(data.split(":", 1)[1])
        except ValueError:
            await q.answer("Invalid server ID")
            return
        s_info = get_child_server(target_sid)
        if s_info and s_info.get("uptimerobot_id"):
            cloud_manager.delete_uptimerobot_monitor(s_info["uptimerobot_id"])
        remove_child_server(target_sid)
        await q.answer(f"Server #{target_sid} removed and accounts redistributed!", show_alert=True)
        text, kb = build_cluster_text()
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

    # ── Admin Scout Callbacks ──────────────────────────────────────────────
    elif data in ("adm_scout_view", "adm_scout_refresh"):
        text, kb = build_scout_text()
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "adm_scout_add_btn":
        context.user_data["state"] = "awaiting_admin_scout_add"
        await q.message.reply_html(
            "➕ <b>Add Target Creator to Scout</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send the Instagram creator username:\n"
            "Example: <code>@cristiano</code> or <code>nike</code>\n\n"
            "Send /cancel to abort or tap below.",
            reply_markup=kb_cancel(),
        )

    elif data == "adm_scout_del_menu":
        creators = get_active_watchlist()
        if not creators:
            await q.answer("Scout watchlist is empty!", show_alert=True)
            return
        await q.edit_message_text(
            "🗑️ <b>Remove Creator from Scout</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Tap a creator below to stop scouting:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_scout_delete(),
        )

    elif data.startswith("adm_scout_del:"):
        creator = data.split(":", 1)[1].strip()
        remove_watchlist_creator(creator)
        await q.answer(f"Removed @{creator} from scout.")
        text, kb = build_scout_text()
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "adm_scout_dispatch":
        count = await dispatch_pending_scout_queue(context.bot)
        if count > 0:
            await q.answer(f"🚀 Dispatched {count} queued reel(s)!")
        else:
            await q.answer("ℹ️ No pending reels in queue.")
        text, kb = build_scout_text()
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "adm_setlimit_prompt":
        context.user_data["state"] = "awaiting_user_limit"
        await q.message.reply_html(
            "⚙️ <b>Set User Watchlist Slot Limit</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send the user's <b>Telegram User ID</b> or <b>@username</b> and slot count:\n\n"
            "<b>Examples:</b>\n"
            "• <code>123456789 5</code>\n"
            "• <code>@username 5</code>\n\n"
            "<i>(Default for free users is 1 creator. Setting to 5 allows monitoring 5 accounts.)</i>\n\n"
            "Send /cancel to abort or tap below.",
            reply_markup=kb_cancel(),
        )

    elif data == "adm_view_limits":
        customs = get_all_custom_limits()
        if not customs:
            text = (
                "📋 <b>Custom User Watchlist Limits</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "<i>No custom user limits configured yet. All users currently have the default 1 slot.</i>\n\n"
                "Tap <b>⚙️ Set User Slot Limit</b> below to upgrade a user!"
            )
        else:
            lines = [
                "📋 <b>Users With Custom Watchlist Limits</b>",
                "━━━━━━━━━━━━━━━━━━━━\n",
            ]
            for c in customs:
                u_info = f"@{c['username']}" if c['username'] else (c['first_name'] or "User")
                used = len(get_user_watchlist(c['user_id']))
                lines.append(f"• <b>{html.escape(u_info)}</b> (ID: <code>{c['user_id']}</code>): <b>{c['limit']} slots</b> (using {used})")
            lines.append(f"\n<i>Total upgraded users: {len(customs)}</i>")
            text = "\n".join(lines)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Set User Slot Limit", callback_data="adm_setlimit_prompt")],
            [InlineKeyboardButton("⬅️ Back to Scout", callback_data="adm_scout_refresh")],
        ])
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    # ── Audio Extraction Callback ─────────────────────────────────────────
    elif data.startswith("audio:"):
        shortcode = data.split(":", 1)[1].strip()
        cached = get_cached_media(shortcode) if shortcode else None
        bot_user = (context.bot.username or "InstaLoaderBot").lstrip("@")
        audio_caption = (
            f"{E_SPARKLES} <b>Audio Extracted</b> {E_LIGHTNING}\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"{E_ROCKET} <b>Downloaded via @{bot_user}</b>"
        )

        # 1. Instant Cache Hit: send cached audio_file_id in < 0.2s!
        if cached and cached.get("audio_file_id"):
            await q.answer("🎵 Sending cached audio...")
            await context.bot.send_chat_action(q.message.chat_id, ChatAction.UPLOAD_VOICE)
            await context.bot.send_audio(
                chat_id=q.message.chat_id,
                audio=cached["audio_file_id"],
                title=f"Audio - {shortcode}",
                performer=f"@{bot_user}",
                caption=audio_caption,
                parse_mode=ParseMode.HTML,
                reply_to_message_id=q.message.message_id,
            )
            return

        # 2. Cache Miss: Extract from message video
        if not q.message.video:
            await q.answer("❌ Video media unavailable for extraction.", show_alert=True)
            return

        await q.answer("⏳ Extracting audio MP3...")
        status_msg = await q.message.reply_text(
            f"{E_RING_LOADER} <b>Extracting audio stream...</b>",
            parse_mode=ParseMode.HTML,
        )

        session_id = uuid.uuid4().hex[:8]
        work_dir = DOWNLOAD_DIR / f"audio_{session_id}"
        work_dir.mkdir(parents=True, exist_ok=True)
        video_temp_path = work_dir / "source_video.mp4"

        try:
            v_file = await context.bot.get_file(q.message.video.file_id)
            await v_file.download_to_drive(video_temp_path)

            audio_path = await asyncio.to_thread(extract_audio_from_video, video_temp_path)
            if not audio_path or not audio_path.exists():
                await status_msg.edit_text(
                    f"{E_BROKEN_HEART} <b>Failed to extract audio from video.</b>",
                    parse_mode=ParseMode.HTML,
                )
                return

            await context.bot.send_chat_action(q.message.chat_id, ChatAction.UPLOAD_VOICE)
            sent_audio = None
            try:
                with open(audio_path, "rb") as af:
                    sent_audio = await context.bot.send_audio(
                        chat_id=q.message.chat_id,
                        audio=af,
                        title=f"Audio - {shortcode}" if shortcode else "Instagram Audio",
                        performer=f"@{bot_user}",
                        caption=audio_caption,
                        parse_mode=ParseMode.HTML,
                        reply_to_message_id=q.message.message_id,
                        write_timeout=180,
                        read_timeout=120,
                    )
            except TelegramError as te:
                logger.warning("send_audio with HTML failed (%s), retrying plain text...", te)
                clean_audio_caption = re.sub(r"<[^>]+>", "", audio_caption)
                with open(audio_path, "rb") as af:
                    sent_audio = await context.bot.send_audio(
                        chat_id=q.message.chat_id,
                        audio=af,
                        title=f"Audio - {shortcode}" if shortcode else "Instagram Audio",
                        performer=f"@{bot_user}",
                        caption=clean_audio_caption,
                        reply_to_message_id=q.message.message_id,
                        write_timeout=180,
                        read_timeout=120,
                    )

            if shortcode and sent_audio and sent_audio.audio:
                update_cached_audio(shortcode, sent_audio.audio.file_id)

            try:
                await status_msg.delete()
            except Exception:
                pass
        except Exception as exc:
            logger.exception("Error extracting audio in callback")
            try:
                await status_msg.edit_text(
                    f"{E_BROKEN_HEART} <b>Could not extract audio.</b> Please try again.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        finally:
            cleanup_session(video_temp_path)
        return

    # ── User callback: multi-channel join check ───────────────────────────
    elif data == "check_joined":
        unjoined = await get_unjoined_channels(context.bot, uid)
        if not unjoined:
            cache_user_membership(uid, True)
            await q.answer("Verification successful! Welcome!", show_alert=False)
            try:
                await q.edit_message_text(
                    f"{E_CONFETTI} <b>Access Granted!</b> {E_LIGHTNING}\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    "All required channel memberships verified!\n"
                    f"You now have full access to <b>InstaBot</b> {E_SPARKLES}\n\n"
                    f"{E_ARROW} <b>Paste any Instagram link below to get started!</b>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            await context.bot.send_message(
                chat_id=uid,
                text=f"{E_ARROW} <b>Choose an option or paste any Instagram link below:</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=rkb_user(),
            )
        else:
            remaining = len(unjoined)
            ch_names = ", ".join([c.get("title", "Channel") for c in unjoined])
            await q.answer(
                f"❌ You still haven't joined {remaining} channel(s):\n{ch_names}\n\nPlease join all of them first!",
                show_alert=True,
            )
            try:
                ch_list_str = "\n".join([f"  {E_HEART_BORDER} <b>{html.escape(c.get('title', 'Channel'))}</b>" for c in unjoined])
                await q.edit_message_text(
                    f"{E_WARNING} <b>Subscription Incomplete</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"You still need to join the following <b>{remaining}</b> channel(s):\n\n"
                    f"{ch_list_str}\n\n"
                    f"{E_ARROW} Tap <b>Join</b> below, then tap <b>I've Joined All</b>:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_force_sub(unjoined),
                )
            except Exception:
                pass


# ── Main Message Handler ───────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _waiting_count

    message = update.message
    user    = update.effective_user
    text    = (message.text or "").strip()

    if user:
        asyncio.create_task(asyncio.to_thread(upsert_user, user.id, user.username, user.first_name))

    # ── Quick Cancel interceptor (command or button text) ────────────────
    if text.lower() in ("/cancel", "cancel", "❌ cancel", "abort"):
        context.user_data.pop("state", None)
        context.user_data.pop("pending_link", None)
        context.user_data.pop("pending_ch_link", None)
        keyboard = rkb_admin() if is_admin(user.id) else rkb_user()
        await message.reply_text(
            f"{E_WARNING} <b>Action cancelled.</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        return

    # ── Quick Start interceptor ──────────────────────────────────────────
    if text.lower() == "/start" or text.lower().startswith("/start") or text.lower() == "start":
        await cmd_start(update, context)
        return

    # ── State Machine (User and Admin) ────────────────────────────────────
    state = context.user_data.get("state")

    if state == "awaiting_user_watch_add":
        context.user_data.pop("state", None)
        creator = text.strip().lstrip("@").lower()
        admin_flag = is_admin(user.id)
        user_limit = get_user_scout_limit(user.id)
        success, reason = add_user_watchlist_creator(user.id, creator, is_admin_user=admin_flag)
        if success:
            text_resp, kb = build_watchlist_text(user.id)
            await message.reply_html(
                f"{E_CONFETTI} <b>Added @{html.escape(creator)} to your Watchlist!</b>\n\n" + text_resp,
                reply_markup=kb,
            )
        elif reason == "limit_reached":
            await message.reply_html(
                f"🔒 <b>Watchlist Slot Limit Reached!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Your current plan allows monitoring <b>{user_limit}</b> creator account(s).\n"
                f"💎 Contact Admin to upgrade your slots!\n\n"
                f"<i>Tap below to remove a creator first:</i>",
                reply_markup=kb_watchlist_delete(user.id),
            )
        elif reason == "already_exists":
            text_resp, kb = build_watchlist_text(user.id)
            await message.reply_html(f"⚠️ <b>@{html.escape(creator)} is already on your watchlist!</b>\n\n" + text_resp, reply_markup=kb)
        else:
            await message.reply_html(f"⚠️ <b>Could not add @{html.escape(creator)}. Please try again.</b>")
        return

    if is_admin(user.id):
        if state == "awaiting_broadcast":
            await _do_broadcast(message, context)
            return
        if state in ("awaiting_channel_add", "awaiting_channel"):
            await _do_add_channel(message, context)
            return
        if state and state.startswith("awaiting_ch_link:"):
            cid = state.split(":", 1)[1]
            await _do_update_channel_link(message, context, cid)
            return
        if state == "awaiting_channel_link":
            channels = get_all_channels()
            cid = channels[0]["chat_id"] if channels else ""
            if cid:
                await _do_update_channel_link(message, context, cid)
            else:
                context.user_data.pop("state", None)
                await message.reply_text(f"{E_WARNING} <b>No channel to link.</b> Add a channel first.", parse_mode=ParseMode.HTML, reply_markup=rkb_admin())
            return
        if state == "awaiting_user_limit":
            context.user_data.pop("state", None)
            parts = text.split()
            if len(parts) >= 2:
                raw_target = parts[0].strip()
                try:
                    lim = int(parts[1])
                    if lim < 1:
                        lim = 1

                    if raw_target.lstrip("-").isdigit():
                        target_uid = int(raw_target)
                    else:
                        target_uid = get_user_id_by_username(raw_target)
                        if not target_uid:
                            await message.reply_html(
                                f"⚠️ Could not find user with username <b>{html.escape(raw_target)}</b> in database.\n"
                                f"Please make sure they have used the bot before, or provide their numeric Telegram User ID.",
                                reply_markup=rkb_admin(),
                            )
                            return

                    set_user_scout_limit(target_uid, lim)
                    u_info = get_user_info(target_uid)
                    u_disp = f"@{u_info['username']}" if u_info and u_info.get("username") else f"User #{target_uid}"
                    await message.reply_html(
                        f"✅ <b>Slot Limit Updated!</b>\n\n"
                        f"👤 <b>User:</b> {html.escape(u_disp)} (<code>{target_uid}</code>)\n"
                        f"📊 <b>New Slot Limit:</b> <b>{lim} creators</b>\n\n"
                        f"This user can now monitor up to {lim} accounts via <b>🎯 My Watchlist</b>!",
                        reply_markup=rkb_admin(),
                    )
                    return
                except ValueError:
                    pass

            await message.reply_html(
                "⚠️ Invalid format. Please provide: <code>&lt;user_id or @username&gt; &lt;limit&gt;</code>\n"
                "Example: <code>123456789 5</code> or <code>@username 5</code>",
                reply_markup=rkb_admin(),
            )
            return
        if state == "awaiting_admin_scout_add":
            context.user_data.pop("state", None)
            creator = text.strip().lstrip("@").lower()
            add_watchlist_creator(creator, added_by=user.id)
            text_resp, kb = build_scout_text()
            await message.reply_html(f"✅ <b>Added @{html.escape(creator)} to scout watchlist!</b>\n\n" + text_resp, reply_markup=kb)
            return
        if state == "awaiting_server_url":
            context.user_data.pop("state", None)
            parts = text.split()
            url = parts[0].strip()
            name = " ".join(parts[1:]).strip() if len(parts) > 1 else f"Worker-{int(time.time()) % 1000}"
            uptimerobot_id = ""
            uptime_note = ""
            if os.getenv("UPTIMEROBOT_API_KEY", "").strip():
                ok_uptime, res_uptime = cloud_manager.create_uptimerobot_monitor(server_url=url, friendly_name=name)
                if ok_uptime:
                    uptimerobot_id = res_uptime
                    uptime_note = f"✅ UptimeRobot 24/7 Monitor created (ID: <code>{uptimerobot_id}</code>)"
                else:
                    uptime_note = f"⚠️ UptimeRobot monitor skipped: {res_uptime}"
            else:
                uptime_note = "ℹ️ UptimeRobot monitor not created (UPTIMEROBOT_API_KEY not in .env)"

            server_id = add_child_server(name=name, url=url, uptimerobot_id=uptimerobot_id)
            text_resp, kb = build_cluster_text()
            await message.reply_html(
                f"🎉 <b>Child Server #{server_id} Added & Load Divided!</b>\n"
                f"🖥️ <b>Name:</b> {html.escape(name)}\n"
                f"🔗 <b>URL:</b> <code>{html.escape(url)}</code>\n"
                f"{uptime_note}\n\n" + text_resp,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            return
        if state == "awaiting_render_api_key":
            context.user_data.pop("state", None)
            render_api_key = text.strip()
            repo_url = os.getenv("GITHUB_REPO_URL", "").strip()
            if not repo_url:
                await message.reply_html("⚠️ <b>GITHUB_REPO_URL not set in .env!</b> Please set it first.")
                return
            prog = await message.reply_html("⏳ <b>Deploying Child Worker to Render...</b>")
            active_servers = get_active_child_servers()
            next_worker_id = len(active_servers) + 1
            ok, res = await asyncio.to_thread(
                cloud_manager.deploy_render_child_service,
                render_api_key=render_api_key,
                repo_url=repo_url,
                db_url=os.getenv("DATABASE_URL", "").strip(),
                bot_token=BOT_TOKEN,
                admin_id=ADMIN_ID,
                worker_id=next_worker_id,
                service_name=f"instabot-scout-{next_worker_id}",
            )
            if not ok:
                await prog.edit_text(f"❌ <b>Render Deploy Failed:</b>\n\n<code>{html.escape(str(res))}</code>", parse_mode=ParseMode.HTML)
                return
            srv_id = res.get("service_id", "")
            srv_url = res.get("url", "")
            srv_name = res.get("name", f"Worker-{next_worker_id}")
            uptimerobot_id = ""
            uptime_note = ""
            if os.getenv("UPTIMEROBOT_API_KEY", "").strip():
                ok_u, res_u = await asyncio.to_thread(cloud_manager.create_uptimerobot_monitor, server_url=srv_url, friendly_name=srv_name)
                if ok_u:
                    uptimerobot_id = res_u
                    uptime_note = f"✅ UptimeRobot 24/7 Monitor created (ID: <code>{uptimerobot_id}</code>)"
                else:
                    uptime_note = f"⚠️ UptimeRobot skipped: {res_u}"
            server_id = add_child_server(name=srv_name, render_service_id=srv_id, url=srv_url, uptimerobot_id=uptimerobot_id)
            text_resp, kb = build_cluster_text()
            await prog.edit_text(
                f"🎉 <b>Render Child Worker #{server_id} Deployed!</b>\n"
                f"🖥️ <b>Name:</b> {html.escape(srv_name)}\n"
                f"🆔 <b>Render Service ID:</b> <code>{srv_id}</code>\n"
                f"🔗 <b>Worker URL:</b> <code>{html.escape(srv_url)}</code>\n"
                f"{uptime_note}\n\n" + text_resp,
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            return

    # ── Admin cookie document upload ───────────────────────────────────────
    if message and message.document and is_admin(user.id):
        doc = message.document
        fname = (doc.file_name or "").lower()
        if fname.endswith(".txt") or fname.endswith(".json"):
            status_up = await message.reply_text(f"{E_RING_LOADER} <b>Receiving cookie file...</b>", parse_mode=ParseMode.HTML)
            try:
                tg_file = await context.bot.get_file(doc.file_id)
                target_path = Path("cookies.txt")
                if fname.endswith(".json"):
                    temp_json = Path("temp_cookies.json")
                    await tg_file.download_to_drive(custom_path=temp_json)
                    ok = convert_json_cookies_to_netscape(temp_json, target_path)
                    temp_json.unlink(missing_ok=True)
                    if not ok:
                        await status_up.edit_text(f"{E_WARNING} <b>Failed to parse JSON cookies format.</b> Please check the file.", parse_mode=ParseMode.HTML)
                        return
                else:
                    await tg_file.download_to_drive(custom_path=target_path)

                # Persist cookie content to database for cloud survivability across Render restarts
                try:
                    cookie_content = target_path.read_text(encoding="utf-8")
                    set_setting("active_cookies", cookie_content)
                except Exception as ex:
                    logger.warning("Failed to backup cookies to database: %s", ex)

                # Reset maintenance & alerts
                stop_cookie_reminder(context.job_queue)
                set_setting("maintenance_mode", "0")
                set_setting("cookie_alert_active", "0")

                try:
                    await status_up.delete()
                except Exception:
                    pass

                await message.reply_text(
                    f"{E_CONFETTI} <b>Cookies Updated Successfully!</b>\n\n"
                    f"{E_CHECK_MARK} Saved fresh <code>cookies.txt</code>\n"
                    f"{E_DIAMOND} Backed up to Neon PostgreSQL\n"
                    f"{E_CHECK_MARK} Maintenance mode turned <b>OFF</b>\n"
                    f"{E_SPARKLES} Cookie reminders stopped\n\n"
                    f"{E_LIGHTNING} Your bot is ready to download reels!",
                    parse_mode=ParseMode.HTML,
                    reply_markup=rkb_admin(),
                )
                return
            except Exception as e:
                logger.exception("Failed to process uploaded cookie file")
                await status_up.edit_text(f"{E_WARNING} <b>Error saving cookies:</b> <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)
                return

    # ── Reply Keyboard Button Presses ─────────────────────────────────────
    if text in ALL_BTNS or text.startswith("🔧 Maintenance:") or text.startswith("🍪 Cookie:") or text.startswith("📺 Channels"):
        await _handle_button(text, user, message, context)
        return

    # ── Maintenance gate ───────────────────────────────────────────────────
    if is_maintenance() and not is_admin(user.id):
        await message.reply_text(
            f"{E_ARC_REACTOR} <b>Under Maintenance</b>\n\n"
            "The bot is temporarily down for updates.\n"
            "Please check back in a few minutes!",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Multi-channel membership gate ──────────────────────────────────────
    if not is_admin(user.id):
        unjoined = await get_unjoined_channels(context.bot, user.id)
        if unjoined:
            ch_list_str = "\n".join([f"  {E_HEART_BORDER} <b>{html.escape(c.get('title', 'Channel'))}</b>" for c in unjoined])
            await message.reply_text(
                f"{E_WARNING} <b>Subscription Required</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "You must join our official channel(s) before downloading:\n\n"
                f"{ch_list_str}\n\n"
                f"{E_ARROW} Tap <b>Join</b> for each channel below, then tap <b>I've Joined All</b>:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_force_sub(unjoined),
            )
            return

    # ── URL check ─────────────────────────────────────────────────────────
    if not is_instagram_url(text):
        await message.reply_text(
            f"{E_DARK_CAT} That doesn't look like an Instagram link.\n\n"
            "Send something like:\n"
            "<code>https://www.instagram.com/reel/ABC123/</code>\n"
            "<code>https://www.instagram.com/p/ABC123/</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Instant Cache Check (< 0.5s response) ─────────────────────────────
    shortcode = extract_shortcode(text)
    if shortcode:
        cached = get_cached_media(shortcode)
        if cached and cached.get("video_file_id"):
            bot_user = (context.bot.username or "InstaLoaderBot").lstrip("@")
            cap_lines = [
                f"{E_CONFETTI} <b>Done!</b> Metadata stripped {E_BLACK_MASK}",
                "━━━━━━━━━━━━━━━━━━━━",
            ]
            c_text = (cached.get("caption") or "").strip()
            if c_text:
                clean_cap = c_text[:700] + ("…" if len(c_text) > 700 else "")
                cap_lines.append(f"📝 <b>Caption:</b>\n<code>{html.escape(clean_cap)}</code>")
                cap_lines.append("━━━━━━━━━━━━━━━━━━━━")
            cap_lines.append(f"{E_ROCKET} <b>Downloaded via @{bot_user}</b>")
            video_cap = "\n".join(cap_lines)

            try:
                await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_VIDEO)
                await context.bot.send_video(
                    chat_id=message.chat_id,
                    video=cached["video_file_id"],
                    caption=video_cap,
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True,
                    reply_markup=kb_video_actions(shortcode),
                )
                log_download(user.id, text, True)
                logger.info("Instant cache hit | user=%s | shortcode=%s", user.id, shortcode)
                return
            except Exception as exc:
                logger.warning("Cache send failed for %s, falling back to fresh download: %s", shortcode, exc)

    # ── Queue capacity check ───────────────────────────────────────────────
    global _waiting_count, _active_count
    if _waiting_count >= MAX_QUEUE:
        await message.reply_text(
            f"{E_WARNING} <b>Queue Full</b>\n\n"
            f"The bot is currently handling high traffic ({_waiting_count} users in queue).\n"
            "Please try again in a minute!",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Queue and download ─────────────────────────────────────────────────
    _waiting_count += 1
    q_pos = _waiting_count
    waiting_decremented = False
    heart_msg = None
    status_msg = None

    sem = get_download_semaphore()
    is_queued = sem.locked() or (_active_count >= MAX_CONCURRENT)

    try:
        # Send premium pulsing heart emoji while downloading
        try:
            heart_msg = await message.reply_text(
                f"{E_HEART_RED}",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

        if is_queued:
            status_msg = await message.reply_text(
                f"{E_RING_LOADER} <b>Queued</b> — you're #{q_pos} in line.\n"
                "Hang tight, your download will start shortly...",
                parse_mode=ParseMode.HTML,
            )
        else:
            status_msg = await message.reply_text(
                f"{E_LIGHTNING} <b>Connecting to Instagram...</b>\n"
                "<code>[▰▱▱▱▱▱▱▱▱▱] 15%</code>\n"
                "<i>Initializing secure stream...</i>",
                parse_mode=ParseMode.HTML,
            )

        async with sem:
            if not waiting_decremented:
                _waiting_count = max(0, _waiting_count - 1)
                waiting_decremented = True

            _active_count += 1
            # If the user was queued, update status immediately once their turn begins!
            if is_queued and status_msg:
                try:
                    await status_msg.edit_text(
                        f"{E_LIGHTNING} <b>Download Started!</b>\n"
                        "<code>[▰▱▱▱▱▱▱▱▱▱] 15%</code>\n"
                        "<i>Connecting to Instagram...</i>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

            # Start smooth background animation ticker
            stop_event = asyncio.Event()
            anim_task = asyncio.create_task(animate_progress(status_msg, stop_event))

            logger.info("DL start | user=%s | url=%.80s", user.id, text)
            try:
                await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_VIDEO)
                # Run download in worker thread to keep bot completely non-blocking
                result = await asyncio.to_thread(download_instagram, text)
            finally:
                stop_event.set()
                await anim_task

            # ── Handle failure ─────────────────────────────────────────────
            if not result["success"]:
                if heart_msg:
                    try:
                        await heart_msg.delete()
                    except Exception:
                        pass
                log_download(user.id, text, False)
                err_type = result.get("error_type", "generic")

                if err_type == "cookie":
                    set_setting("maintenance_mode", "1")
                    set_setting("cookie_alert_active", "1")
                    start_cookie_reminder(context.job_queue)

                    await alert_admin(
                        context.bot,
                        f"{E_WARNING} <b>Cookie Error — Maintenance ON</b>\n\n"
                        f"👤 User: {html.escape(user.first_name)} (<code>{user.id}</code>)\n"
                        f"{E_PIN_LINK} <code>{html.escape(text[:100])}</code>\n"
                        f"{E_CLOCK_TIME} {now_str()}\n\n"
                        f"<pre>{html.escape(result.get('raw_error', '')[:400])}</pre>\n\n"
                        f"{E_WARNING} <b>Maintenance auto-enabled. Please send fresh cookies.txt!</b>",
                    )
                    await status_msg.edit_text(
                        f"{E_ARC_REACTOR} <b>Bot is entering maintenance mode.</b>\n\nPlease try again in a little while.",
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    await alert_admin(
                        context.bot,
                        f"{E_WARNING} <b>Download Failed</b>\n\n"
                        f"👤 {html.escape(user.first_name)} (<code>{user.id}</code>)\n"
                        f"{E_PIN_LINK} <code>{html.escape(text[:100])}</code>\n"
                        f"{E_CLOCK_TIME} {now_str()}\n"
                        f"Type: <code>{err_type}</code>\n\n"
                        f"<pre>{html.escape(result.get('raw_error', result.get('error', ''))[:400])}</pre>",
                    )
                    if err_type == "private":
                        await status_msg.edit_text(
                            f"{E_BLACK_MASK} <b>Private Account</b>\n\n"
                            "This post is from a private account.\n"
                            "I can only download from public accounts.",
                            parse_mode=ParseMode.HTML,
                        )
                    elif err_type == "not_found":
                        await status_msg.edit_text(
                            f"{E_SKULL} <b>Post Not Found</b>\n\n"
                            "This Instagram reel or post was removed, deleted, or the link is broken.",
                            parse_mode=ParseMode.HTML,
                        )
                    else:
                        await status_msg.edit_text(
                            f"{E_BROKEN_HEART} <b>Download Failed</b>\n\n"
                            "Possible reasons:\n"
                            "• The link is invalid or expired\n"
                            "• The post was deleted\n"
                            "• Instagram temporarily restricted access\n\n"
                            "Please try again shortly.",
                            parse_mode=ParseMode.HTML,
                        )
                return

            # ── Upload Phase (Animated transition) ────────────────────────
            try:
                await status_msg.edit_text(
                    f"{E_COLOR_DOTS} <b>Uploading to Telegram...</b>\n"
                    "<code>[▰▰▰▰▰▰▰▰▰▰] 100%</code>\n"
                    "<i>Almost there!</i>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

            video_path   = result.get("video_path")
            image_paths  = result.get("image_paths", [])
            caption_text = result.get("caption", "")

            try:
                # ── Case A: Video / Reel ──────────────────────────────────
                if result.get("is_video", True) and video_path:
                    bot_user = (context.bot.username or "InstaLoaderBot").lstrip("@")
                    cap_lines = [
                        f"{E_CONFETTI} <b>Done!</b> Metadata stripped {E_BLACK_MASK}",
                        "━━━━━━━━━━━━━━━━━━━━",
                    ]
                    if caption_text and caption_text.strip():
                        clean_cap = caption_text.strip()
                        if len(clean_cap) > 700:
                            clean_cap = clean_cap[:700] + "…"
                        cap_lines.append(f"📝 <b>Caption:</b>\n<code>{html.escape(clean_cap)}</code>")
                        cap_lines.append("━━━━━━━━━━━━━━━━━━━━")
                    cap_lines.append(f"{E_ROCKET} <b>Downloaded via @{bot_user}</b>")
                    video_caption = "\n".join(cap_lines)

                    sc = extract_shortcode(text) or ""
                    v_kb = kb_video_actions(sc) if sc else None

                    await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_VIDEO)
                    sent_vid = await _send_video_resilient(
                        bot=context.bot,
                        chat_id=message.chat_id,
                        video_path=video_path,
                        caption=video_caption,
                        reply_markup=v_kb,
                    )
                    vid_file_id = get_media_file_id(sent_vid)
                    if sc and vid_file_id:
                        set_cached_media(
                            shortcode=sc,
                            video_file_id=vid_file_id,
                            caption=caption_text,
                        )

                # ── Case B: Single Photo or Carousel ──────────────────────
                elif image_paths:
                    bot_user = (context.bot.username or "InstaLoaderBot").lstrip("@")
                    media_lines = [
                        f"{E_CONFETTI} <b>Done!</b> Metadata stripped {E_BLACK_MASK}",
                        "━━━━━━━━━━━━━━━━━━━━",
                    ]
                    if caption_text and caption_text.strip():
                        clean_cap = caption_text.strip()
                        if len(clean_cap) > 700:
                            clean_cap = clean_cap[:700] + "…"
                        media_lines.append(f"📝 <b>Caption:</b>\n<code>{html.escape(clean_cap)}</code>")
                        media_lines.append("━━━━━━━━━━━━━━━━━━━━")
                    media_lines.append(f"{E_ROCKET} <b>Downloaded via @{bot_user}</b>")
                    media_caption = "\n".join(media_lines)

                    if len(image_paths) == 1:
                        with open(image_paths[0], "rb") as pf:
                            await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_PHOTO)
                            await context.bot.send_photo(
                                chat_id=message.chat_id,
                                photo=pf,
                                caption=media_caption,
                                parse_mode=ParseMode.HTML,
                                write_timeout=180,
                                read_timeout=120,
                                connect_timeout=30,
                            )
                    else:
                        await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_PHOTO)
                        media_group = []
                        opened_files = []
                        try:
                            for idx, img_p in enumerate(image_paths[:10]):  # Telegram max 10 per album
                                f = open(img_p, "rb")
                                opened_files.append(f)
                                cap = media_caption if idx == 0 else None
                                pm = ParseMode.HTML if idx == 0 else None
                                media_group.append(InputMediaPhoto(media=f, caption=cap, parse_mode=pm))
                            await context.bot.send_media_group(chat_id=message.chat_id, media=media_group)
                        finally:
                            for f in opened_files:
                                try:
                                    f.close()
                                except Exception:
                                    pass

                log_download(user.id, text, True)
                logger.info("DL done  | user=%s", user.id)

                # Delete temporary download messages (heart emoji & progress status)
                for m in (heart_msg, status_msg):
                    if m:
                        try:
                            await m.delete()
                        except Exception:
                            pass

            except Exception as exc:
                if heart_msg:
                    try:
                        await heart_msg.delete()
                    except Exception:
                        pass
                log_download(user.id, text, False)
                logger.exception("Send media failed")
                await alert_admin(
                    context.bot,
                    f"{E_WARNING} <b>Send Failed</b>\n\n"
                    f"👤 {html.escape(user.first_name)} (<code>{user.id}</code>)\n"
                    f"{E_CLOCK_TIME} {now_str()}\n"
                    f"<pre>{html.escape(str(exc)[:400])}</pre>",
                )
                file_size_mb = 0
                if video_path and Path(video_path).exists():
                    file_size_mb = Path(video_path).stat().st_size / (1024 * 1024)

                if file_size_mb > 50:
                    err_user_text = (
                        f"{E_BROKEN_HEART} <b>File exceeds Telegram limits ({file_size_mb:.1f} MB).</b>\n\n"
                        "Telegram bot API restricts file uploads to 50MB max."
                    )
                else:
                    err_user_text = (
                        f"{E_BROKEN_HEART} <b>Couldn't send the media.</b>\n\n"
                        "Telegram encountered a temporary error delivering the file.\n"
                        "Please try sending the link again."
                    )
                await status_msg.edit_text(
                    err_user_text,
                    parse_mode=ParseMode.HTML,
                )

            finally:
                main_path = video_path or (image_paths[0] if image_paths else None)
                cleanup_session(main_path)
                _active_count = max(0, _active_count - 1)

    except Exception as exc:
        if 'heart_msg' in locals() and heart_msg:
            try:
                await heart_msg.delete()
            except Exception:
                pass
        logger.exception("Unexpected error in handle_message")
        await alert_admin(
            context.bot,
            f"{E_WARNING} <b>Unexpected Error</b>\n\n"
            f"👤 {html.escape(user.first_name)} (<code>{user.id}</code>)\n"
            f"{E_CLOCK_TIME} {now_str()}\n"
            f"<pre>{html.escape(str(exc)[:400])}</pre>",
        )
        if 'status_msg' in locals() and status_msg:
            try:
                await status_msg.edit_text(f"{E_BROKEN_HEART} <b>Something went wrong. Please try again.</b>", parse_mode=ParseMode.HTML)
            except Exception:
                pass
    finally:
        if not waiting_decremented:
            _waiting_count = max(0, _waiting_count - 1)



# ── Button press router ────────────────────────────────────────────────────────
async def _handle_button(text: str, user, message, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Route reply-keyboard button presses."""

    # ── User buttons ───────────────────────────────────────────────────────
    if text == BTN_HELP:
        channels = get_all_channels()
        channel_line = f"• Must join <b>{len(channels)} required channel(s)</b>\n" if channels else ""
        await message.reply_text(
            f"{E_SPARKLES} <b>Help — InstaBot</b>\n\n"
            f"<b>How to use:</b>\n"
            f"1. Copy any public Instagram link\n"
            f"2. Paste it in this chat\n"
            f"3. Get your video or photos in seconds {E_LIGHTNING}\n"
            f"4. Tap <b>🎵 Extract Audio</b> below any video for MP3 sound!\n\n"
            f"<b>Supported links:</b>\n"
            f"<code>instagram.com/reel/...</code>\n"
            f"<code>instagram.com/p/...</code>\n"
            f"<code>instagram.com/tv/...</code>\n\n"
            f"<b>Requirements:</b>\n"
            f"{channel_line}"
            f"• Public accounts only\n\n"
            f"<b>Features & Safety:</b>\n"
            f"• Highest quality video & photos\n"
            f"• Advanced anti-detection {E_BLACK_MASK} (fresh metadata & unique hash)\n"
            f"• 🎵 1-Tap Audio Extractor (MP3 sound)\n"
            f"• Caption in <code>monospace</code> for easy copy",
            parse_mode=ParseMode.HTML,
        )
        return

    elif text == BTN_WATCHLIST:
        text_resp, kb = build_watchlist_text(user.id)
        await message.reply_html(text_resp, reply_markup=kb)
        return

    # ── Admin-only buttons ─────────────────────────────────────────────────
    if not is_admin(user.id):
        await message.reply_text(f"{E_WARNING} <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return

    if text == BTN_STATS:
        await message.reply_text(build_stats_text(), parse_mode=ParseMode.HTML)

    elif text == BTN_SERVERS:
        text_resp, kb = build_cluster_text()
        await message.reply_html(text_resp, reply_markup=kb, disable_web_page_preview=True)

    elif text == BTN_SCOUT:
        text_resp, kb = build_scout_text()
        await message.reply_html(text_resp, reply_markup=kb)

    elif text == BTN_BROADCAST:
        context.user_data["state"] = "awaiting_broadcast"
        await message.reply_text(
            f"{E_RED_WOLF} <b>Broadcast</b>\n\n"
            f"Send the message to broadcast to all users.\n"
            f"Supports text, photos, videos — anything.\n\n"
            f"Send /cancel to abort or tap below.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel(),
        )

    elif text.startswith("🔧 Maintenance:"):
        new_val = "0" if is_maintenance() else "1"
        set_setting("maintenance_mode", new_val)
        if new_val == "0":
            stop_cookie_reminder(context.job_queue)
        label = f"{E_CHECK_MARK} ON" if new_val == "1" else f"{E_CROSS_MARK} OFF"
        await message.reply_text(
            f"{E_ARC_REACTOR} Maintenance mode → <b>{label}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=rkb_admin(),
        )

    elif text.startswith("📺 Channels") or text == BTN_CHANNELS:
        await message.reply_text(
            build_channel_list_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_management(),
            disable_web_page_preview=True,
        )

    elif text.startswith("🍪 Cookie:"):
        if get_setting("cookie_alert_active", "0") == "1":
            stop_cookie_reminder(context.job_queue)
            await message.reply_text(
                f"{E_CONFETTI} <b>Cookie alert cleared.</b>\n\n"
                f"Remember to update cookies on Render or send fresh file, then turn off Maintenance.",
                parse_mode=ParseMode.HTML,
                reply_markup=rkb_admin(),
            )
        else:
            await message.reply_text(
                f"{E_CONFETTI} Cookie status: <b>OK</b>\n\nNo active alerts.",
                parse_mode=ParseMode.HTML,
            )

    elif text == BTN_CLOSE_MENU:
        await message.reply_text(
            f"{E_WHITE_BUTTERFLY} Admin menu closed. You're back to normal user mode.",
            parse_mode=ParseMode.HTML,
            reply_markup=rkb_user(),
        )


# ── Admin helpers ──────────────────────────────────────────────────────────────
async def _do_broadcast(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
    user_ids = get_all_user_ids()
    progress = await message.reply_text(
        f"{E_RING_LOADER} <b>Broadcasting to {len(user_ids):,} users...</b>",
        parse_mode=ParseMode.HTML,
    )

    sent   = 0
    failed = 0
    for uid in user_ids:
        try:
            await context.bot.copy_message(
                chat_id=uid,
                from_chat_id=message.chat_id,
                message_id=message.message_id,
            )
            sent += 1
        except TelegramError:
            failed += 1
        await asyncio.sleep(0.05)   # ~20 msgs/s, stay under Telegram limits

    await progress.edit_text(
        f"{E_SPARKLES} <b>Broadcast Complete</b>\n\n"
        f"{E_CHECK_MARK} Sent   : <code>{sent:,}</code>\n"
        f"{E_WARNING} Failed : <code>{failed:,}</code> <i>(blocked/deleted)</i>\n"
        f"{E_CLOCK_TIME} {now_str()}",
        parse_mode=ParseMode.HTML,
    )


async def _do_add_channel(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (message.text or "").strip()
    target = None
    custom_link = None

    # Check if a pending link was already stored from previous step
    pending_link = context.user_data.pop("pending_ch_link", None)

    # 1. Check if forwarded from a channel
    forward_chat = None
    if message.forward_origin and getattr(message.forward_origin, "chat", None):
        forward_chat = message.forward_origin.chat
    elif getattr(message, "forward_from_chat", None):
        forward_chat = message.forward_from_chat

    if forward_chat:
        target = forward_chat.id
        if pending_link:
            custom_link = pending_link

    # 2. Check text format
    elif text:
        tokens = text.split()
        for tok in tokens:
            if "t.me/" in tok or tok.startswith("http"):
                custom_link = tok
            elif tok.startswith("@"):
                target = tok
            elif tok.startswith("-") or tok.isdigit():
                try:
                    target = int(tok)
                except ValueError:
                    target = tok

        # If user only provided a URL without chat ID or username
        if not target and custom_link:
            cleaned = custom_link.split("t.me/")[-1].strip("/")
            if not cleaned.startswith("+") and not cleaned.startswith("joinchat/"):
                target = f"@{cleaned}"
            else:
                context.user_data["pending_ch_link"] = custom_link
                await message.reply_text(
                    f"{E_NEON_RINGS} <b>Private Invite Link Received!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Telegram Bot API needs the channel's <b>Chat ID</b> to verify member subscriptions.\n\n"
                    f"{E_ARROW} <b>Two easy ways to finish:</b>\n"
                    f"1️⃣ <b>Forward any post</b> from that channel into this chat.\n"
                    f"2️⃣ Or send its <b>Chat ID</b> (e.g. <code>-1001234567890</code>).\n\n"
                    f"<i>I've saved your invite link and will attach it automatically!</i>\n"
                    f"Send /cancel to abort or tap below.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_cancel(),
                )
                return

    if not target:
        await message.reply_text(
            f"{E_WARNING} <b>Invalid format.</b>\n\n"
            f"Please provide:\n"
            f"• <b>Chat ID</b> (e.g. <code>-1001234567890</code>)\n"
            f"• <b>Username</b> (e.g. <code>@mychannel</code>)\n"
            f"• <b>Link</b> (e.g. <code>https://t.me/mychannel</code> or <code>-100... https://t.me/+...</code>)\n"
            f"• Or <b>forward any post from your channel</b> here!\n\n"
            f"Send /cancel to abort or tap below.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel(),
        )
        return

    status_msg = await message.reply_text(
        f"{E_RING_LOADER} <b>Verifying channel and checking bot permissions...</b>",
        parse_mode=ParseMode.HTML,
    )

    # 3. Verify channel exists and bot can access it
    try:
        chat = await context.bot.get_chat(target)
    except Exception as exc:
        logger.warning("get_chat failed for %s: %s", target, exc)
        await status_msg.edit_text(
            f"{E_WARNING} <b>Could not find or access that channel!</b>\n\n"
            f"Target: <code>{html.escape(str(target))}</code>\n"
            f"Error: <code>{html.escape(str(exc))}</code>\n\n"
            f"{E_WARNING} <b>Please make sure:</b>\n"
            f"1. You have already added this bot to your channel!\n"
            f"2. The Chat ID or username is typed correctly.\n"
            f"3. Or forward any message directly from the channel into this chat.\n\n"
            f"Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel(),
        )
        return

    # 4. Verify bot is an Administrator in the channel
    try:
        me = await context.bot.get_me()
        bot_member = await context.bot.get_chat_member(chat_id=chat.id, user_id=me.id)
        if bot_member.status not in ("administrator", "creator"):
            await status_msg.edit_text(
                f"{E_WARNING} <b>Bot is NOT an Administrator in {html.escape(chat.title or '')}!</b>\n\n"
                f"{E_TV_SCREEN} Channel: <b>{html.escape(chat.title or '')}</b>\n"
                f"{E_PIN_LINK} Chat ID: <code>{chat.id}</code>\n"
                f"{E_ARC_REACTOR} Bot Role: <code>{bot_member.status}</code>\n\n"
                f"{E_WARNING} Telegram <b>requires</b> bots to be an Administrator to verify if users have joined.\n\n"
                f"{E_ARROW} <b>Steps to fix:</b>\n"
                f"1. Open channel <b>{html.escape(chat.title or '')}</b> in Telegram\n"
                f"2. Tap Channel Title ➜ Edit ➜ Administrators\n"
                f"3. Add @{me.username} as an Admin\n"
                f"4. Send the Chat ID <code>{chat.id}</code> or forward a message again here!",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel(),
            )
            return
    except Exception as exc:
        logger.warning("get_chat_member failed for bot in %s: %s", chat.id, exc)
        await status_msg.edit_text(
            f"{E_WARNING} <b>Failed to verify bot admin permissions in {html.escape(chat.title or '')}:</b>\n\n"
            f"<code>{html.escape(str(exc))}</code>\n\n"
            f"Make sure the bot is an Administrator in the channel.",
            parse_mode=ParseMode.HTML,
        )
        return

    # 5. Auto-fetch invite link — try every method before asking admin
    invite_link = custom_link or (pending_link if pending_link else "")
    if not invite_link:
        if chat.username:
            # Public channel — username link always works
            invite_link = f"https://t.me/{chat.username}"
        else:
            # Private channel — try 3 ways to get the link automatically
            # Method A: link already on the chat object (returned by get_chat)
            existing = getattr(chat, "invite_link", None)
            if existing:
                invite_link = existing

            # Method B: export the primary invite link (requires "Invite Users" perm)
            if not invite_link:
                try:
                    invite_link = await context.bot.export_chat_invite_link(chat.id)
                    logger.info("Got primary invite link via export for %s", chat.id)
                except Exception as exc:
                    logger.warning("export_chat_invite_link failed for %s: %s", chat.id, exc)

            # Method C: create a new invite link
            if not invite_link:
                try:
                    created = await context.bot.create_chat_invite_link(chat.id, name="InstaBot")
                    invite_link = created.invite_link
                    logger.info("Created new invite link for %s", chat.id)
                except Exception as exc:
                    logger.warning("create_chat_invite_link failed for %s: %s", chat.id, exc)
                    invite_link = ""

    # 6. Save channel to DB
    title = chat.title or (f"@{chat.username}" if chat.username else str(chat.id))
    add_channel(chat.id, title, invite_link)
    context.user_data.pop("state", None)

    try:
        await status_msg.delete()
    except Exception:
        pass

    if invite_link:
        await message.reply_text(
            f"{E_CONFETTI} <b>Channel Added Successfully!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{E_TV_SCREEN} <b>Channel:</b> {html.escape(title)}\n"
            f"{E_PIN_LINK} <b>Chat ID:</b> <code>{chat.id}</code>\n"
            f"{E_ARC_REACTOR} <b>Bot Role:</b> Administrator {E_CHECK_MARK}\n"
            f"{E_PIN_LINK} <b>Invite Link:</b> <a href=\"{invite_link}\">Click Here</a>\n\n"
            f"{E_BLACK_MASK} Users will now be required to join this channel before downloading.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_management(),
        )
    else:
        context.user_data["state"] = f"awaiting_ch_link:{chat.id}"
        await message.reply_text(
            f"{E_CONFETTI} <b>Channel Added:</b> <b>{html.escape(title)}</b>\n"
            f"{E_PIN_LINK} <b>Chat ID:</b> <code>{chat.id}</code>\n"
            f"{E_ARC_REACTOR} <b>Bot Role:</b> Administrator {E_CHECK_MARK}\n\n"
            f"{E_WARNING} <b>Invite Link Needed for Private Channel</b>\n"
            f"The bot could not auto-generate an invite link.\n\n"
            f"{E_ARROW} <b>Please send the channel's invite link now</b> (e.g. <code>https://t.me/+...</code>):\n"
            f"<i>(Or send /cancel to set it later)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel(),
        )


async def _do_update_channel_link(message, context: ContextTypes.DEFAULT_TYPE, chat_id: str) -> None:
    text = (message.text or "").strip()
    context.user_data.pop("state", None)

    if text == "-":
        update_channel_link(chat_id, "")
        await message.reply_text(f"{E_CONFETTI} Channel invite link cleared.", parse_mode=ParseMode.HTML, reply_markup=kb_channel_management())
        return

    if not (text.startswith("https://t.me/") or text.startswith("http://t.me/") or text.startswith("@")):
        await message.reply_text(
            f"{E_WARNING} <b>Invalid link format.</b>\n\nPlease send a valid link like <code>https://t.me/+...</code> or <code>https://t.me/channel</code>.\nUse /setlink to try again.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_management(),
        )
        return

    update_channel_link(chat_id, text)
    channels = get_all_channels()
    ch = next((c for c in channels if str(c["chat_id"]) == str(chat_id)), None)
    title = ch["title"] if ch else chat_id

    await message.reply_text(
        f"{E_CONFETTI} <b>Invite Link Updated!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{E_TV_SCREEN} Channel: <b>{html.escape(title)}</b>\n"
        f"{E_PIN_LINK} Chat ID: <code>{chat_id}</code>\n"
        f"{E_PIN_LINK} Link: {html.escape(text)}\n\n"
        f"The <b>Join</b> button is now updated for users!",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_channel_management(),
    )


# ── Channel Management Commands ────────────────────────────────────────────────
@admin_only
async def cmd_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manage force-sub channels."""
    await update.message.reply_text(
        build_channel_list_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_channel_management(),
        disable_web_page_preview=True,
    )


@admin_only
async def cmd_addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Quick command to add a channel."""
    args = context.args
    if args:
        update.message.text = " ".join(args)
        await _do_add_channel(update.message, context)
        return

    context.user_data["state"] = "awaiting_channel_add"
    await update.message.reply_text(
        f"{E_NEON_RINGS} <b>Add Required Channel</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Send any of the following:\n"
        f"1️⃣ <b>Chat ID</b> (e.g. <code>-1001234567890</code>)\n"
        f"2️⃣ <b>Username</b> (e.g. <code>@mychannel</code>)\n"
        f"3️⃣ <b>Link</b> (e.g. <code>https://t.me/channel</code> or <code>-100... https://t.me/+...</code>)\n"
        f"4️⃣ Or forward any post from your channel here!\n\n"
        f"Send /cancel to abort or tap below.",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_cancel(),
    )


@admin_only
async def cmd_delchannel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Quick command to delete a channel."""
    args = context.args
    if args:
        target = args[0].strip()
        ok = remove_channel(target)
        if ok:
            await update.message.reply_text(
                f"{E_CONFETTI} Channel <code>{html.escape(target)}</code> removed.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_channel_management(),
            )
        else:
            await update.message.reply_text(
                f"{E_WARNING} Channel <code>{html.escape(target)}</code> not found in database.",
                parse_mode=ParseMode.HTML,
            )
        return

    channels = get_all_channels()
    if not channels:
        await update.message.reply_text(
            f"{E_WARNING} No required channels configured yet.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(
        f"{E_WARNING} <b>Select a channel to remove:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_channel_delete_menu(),
    )


@admin_only
async def cmd_setlink(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set or update invite link for a channel."""
    args = context.args
    channels = get_all_channels()
    if not channels:
        await update.message.reply_text(
            f"{E_WARNING} No channels configured yet. Add one first with /addchannel.",
            parse_mode=ParseMode.HTML,
        )
        return

    if len(args) >= 2:
        cid = args[0].strip()
        link = args[1].strip()
        update_channel_link(cid, "" if link == "-" else link)
        await update.message.reply_text(
            f"{E_CONFETTI} Link updated for <code>{html.escape(cid)}</code>: {html.escape(link)}",
            parse_mode=ParseMode.HTML,
        )
        return

    if len(channels) == 1:
        cid = channels[0]["chat_id"]
        context.user_data["state"] = f"awaiting_ch_link:{cid}"
        await update.message.reply_text(
            f"{E_NEON_RINGS} <b>Set Invite Link for {html.escape(channels[0]['title'])}</b>\n\n"
            f"Send the invite link (e.g. <code>https://t.me/+...</code> or <code>https://t.me/channel</code>):\n"
            f"Send <code>-</code> to clear.\n"
            f"Send /cancel to abort or tap below.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel(),
        )
        return

    await update.message.reply_text(
        f"{E_NEON_RINGS} <b>Select a channel to update its invite link:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_channel_link_menu(),
    )


# ── Scout Queue Dispatcher ────────────────────────────────────────────────────
async def dispatch_pending_scout_queue(bot) -> int:
    """
    Checks scout_queue for pending viral reels and delivers them to subscribers and admins.
    Returns count of dispatched items.
    """
    pending = get_pending_viral_reels(limit=10)
    if not pending:
        return 0

    dispatched_count = 0
    for item in pending:
        q_id = item["id"]
        shortcode = item["shortcode"]
        creator = item["creator"]
        file_id = item["video_file_id"]
        likes = item.get("likes") or 0

        # Find all subscribers from user_watchlist
        subscribers = get_subscribers_for_creator(creator)

        # Recipients: all subscribers + all admins (ensures admin always gets discovery)
        recipients = set(subscribers)
        recipients.update(ADMIN_IDS)

        caption = (
            f"🎯 <b>Viral Reel Detected!</b>\n\n"
            f"👤 <b>Creator:</b> @{html.escape(creator)}\n"
            f"❤️ <b>Likes:</b> {likes:,}\n"
            f"🔗 <a href='https://www.instagram.com/reel/{shortcode}/'>Original Instagram Reel</a>\n\n"
            f"✨ <i>Auto-delivered from Watchlist!</i>"
        )

        for uid in recipients:
            try:
                await bot.send_video(
                    chat_id=uid,
                    video=file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_video_actions(shortcode),
                    supports_streaming=True,
                )
            except Exception as e_send:
                logger.warning("Failed to send queued reel %s to user %s: %s", shortcode, uid, e_send)

        # Mark as dispatched in database so it is never stuck in queue
        mark_viral_reel_dispatched(q_id)
        dispatched_count += 1
        logger.info("Dispatched queued viral reel %s (@%s) to %d recipients", shortcode, creator, len(recipients))

    return dispatched_count


async def scout_dispatcher_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic job running every 30 seconds to dispatch viral reels found by workers."""
    try:
        await dispatch_pending_scout_queue(context.bot)
    except Exception as exc:
        logger.error("scout_dispatcher_job error: %s", exc)


async def cmd_dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: Force-dispatch any pending reels in scout queue immediately."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    count = await dispatch_pending_scout_queue(context.bot)
    if count > 0:
        await update.message.reply_html(f"🚀 <b>Dispatched {count} pending viral reel(s) to subscribers!</b>")
    else:
        await update.message.reply_html("ℹ️ <b>Scout queue is empty.</b> No pending reels to dispatch.")


# ── Scout Watchlist Command ────────────────────────────────────────────────────
async def cmd_scout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to manage or view autonomous viral reel scout."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    args = context.args
    if args:
        subcmd = args[0].lower()
        if subcmd == "add" and len(args) > 1:
            creator = args[1].strip().lstrip("@")
            success = add_watchlist_creator(creator, added_by=user.id)
            if success:
                await update.message.reply_html(f"✅ <b>Added @{html.escape(creator)} to viral scout watchlist!</b>")
            else:
                await update.message.reply_html(f"⚠️ <b>Could not add @{html.escape(creator)} (might already be active).</b>")
            return
        elif subcmd in ("del", "remove") and len(args) > 1:
            creator = args[1].strip().lstrip("@")
            remove_watchlist_creator(creator)
            await update.message.reply_html(f"🗑️ <b>Removed @{html.escape(creator)} from watchlist.</b>")
            return
        elif subcmd == "list":
            creators = get_active_watchlist()
            text = f"🎯 <b>Active Creator Watchlist ({len(creators)}):</b>\n\n"
            text += "\n".join(f"• @{html.escape(c)}" for c in creators) if creators else "<i>No creators added yet.</i>"
            await update.message.reply_html(text)
            return
        elif subcmd == "limit" and len(args) > 2:
            try:
                target_uid = int(args[1])
                lim = int(args[2])
                set_user_scout_limit(target_uid, lim)
                await update.message.reply_html(f"✅ <b>Limit for user <code>{target_uid}</code> set to {lim} slots!</b>")
                return
            except ValueError:
                pass

    # Default /scout info overview with interactive buttons
    text, kb = build_scout_text()
    await update.message.reply_html(text, reply_markup=kb)


# ── User Creator Watchlist Command ─────────────────────────────────────────────
async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User command to manage personal creator watchlist (default 1 account)."""
    user = update.effective_user
    if not user:
        return

    admin_flag = is_admin(user.id)
    user_limit = get_user_scout_limit(user.id)
    my_creators = get_user_watchlist(user.id)
    limit_str = "Unlimited 👑" if admin_flag else f"{user_limit}"

    args = context.args
    if args:
        subcmd = args[0].lower()
        if subcmd == "add" and len(args) > 1:
            creator = args[1].strip().lstrip("@").lower()
            success, reason = add_user_watchlist_creator(user.id, creator, is_admin_user=admin_flag)
            if success:
                new_count = len(get_user_watchlist(user.id))
                await update.message.reply_html(
                    f"{E_CONFETTI} <b>Added @{html.escape(creator)} to your Watchlist!</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>Slots Used:</b> {new_count} / {limit_str}\n\n"
                    f"🚀 <i>Whenever @{html.escape(creator)} posts a new viral reel (0–5 days, 5k+ likes), "
                    f"it will be automatically downloaded, protected, and sent to you here!</i>"
                )
            elif reason == "limit_reached":
                await update.message.reply_html(
                    f"🔒 <b>Watchlist Slot Limit Reached!</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Your current plan allows monitoring <b>{user_limit}</b> creator account(s).\n"
                    f"You have used all <b>{len(my_creators)}/{user_limit}</b> available slots.\n\n"
                    f"💎 <b>Want to monitor more creators and get instant viral reels automatically?</b>\n"
                    f"👉 Contact Admin to upgrade your slots and unlock multi-creator tracking!\n\n"
                    f"<i>Tip: You can remove your current creator using <code>/watch remove {my_creators[0] if my_creators else 'username'}</code> to monitor a different one.</i>"
                )
            elif reason == "already_exists":
                await update.message.reply_html(
                    f"⚠️ <b>@{html.escape(creator)} is already on your watchlist!</b>"
                )
            else:
                await update.message.reply_html(
                    f"⚠️ <b>Could not add @{html.escape(creator)}. Please try again.</b>"
                )
            return

        elif subcmd in ("del", "remove") and len(args) > 1:
            creator = args[1].strip().lstrip("@").lower()
            remove_user_watchlist_creator(user.id, creator)
            await update.message.reply_html(
                f"🗑️ <b>Removed @{html.escape(creator)} from your watchlist.</b>"
            )
            return

    # Default /watch display with interactive buttons
    text, kb = build_watchlist_text(user.id)
    await update.message.reply_html(text, reply_markup=kb)


# ── Admin Slot Limit Commands ──────────────────────────────────────────────────
async def cmd_setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: Set creator slot limit for any user (/setlimit <user_id or @username> <limit>)."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    args = context.args
    if not args or len(args) < 2:
        await update.message.reply_html(
            "<b>Usage:</b> <code>/setlimit &lt;user_id or @username&gt; &lt;limit&gt;</code>\n\n"
            "<b>Examples:</b>\n"
            "• <code>/setlimit 123456789 5</code>\n"
            "• <code>/setlimit @username 5</code>"
        )
        return

    raw_target = args[0].strip()
    try:
        limit_val = int(args[1])
        if limit_val < 1:
            limit_val = 1
    except ValueError:
        await update.message.reply_html("⚠️ Limit must be a positive number.")
        return

    if raw_target.lstrip("-").isdigit():
        target_uid = int(raw_target)
    else:
        target_uid = get_user_id_by_username(raw_target)
        if not target_uid:
            await update.message.reply_html(
                f"⚠️ Could not find user with username <b>{html.escape(raw_target)}</b> in database.\n"
                f"Please ensure they have used the bot before, or provide their numeric Telegram User ID."
            )
            return

    success = set_user_scout_limit(target_uid, limit_val)
    if success:
        u_info = get_user_info(target_uid)
        u_disp = f"@{u_info['username']}" if u_info and u_info.get("username") else f"User #{target_uid}"
        await update.message.reply_html(
            f"✅ <b>Slot Limit Updated!</b>\n\n"
            f"👤 User: <b>{html.escape(u_disp)}</b> (<code>{target_uid}</code>)\n"
            f"📊 New Creator Limit: <b>{limit_val}</b> slots\n\n"
            f"This user can now monitor up to {limit_val} accounts via <b>🎯 My Watchlist</b>!"
        )
    else:
        await update.message.reply_html("⚠️ Failed to update limit in database.")


async def cmd_getlimit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: Check user creator slot limit (/getlimit [user_id or @username])."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    args = context.args
    if not args:
        customs = get_all_custom_limits()
        if not customs:
            await update.message.reply_html(
                "📋 <b>User Watchlist Limits</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "<i>No custom user limits set yet. All users currently have the default 1 slot.</i>\n\n"
                "<b>Usage:</b>\n"
                "• <code>/setlimit &lt;user_id or @username&gt; &lt;limit&gt;</code>\n"
                "• <code>/getlimit &lt;user_id or @username&gt;</code>"
            )
            return

        lines = ["📋 <b>Users With Custom Watchlist Limits</b>", "━━━━━━━━━━━━━━━━━━━━\n"]
        for c in customs:
            u_info = f"@{c['username']}" if c['username'] else (c['first_name'] or "User")
            used = len(get_user_watchlist(c['user_id']))
            lines.append(f"• <b>{html.escape(u_info)}</b> (ID: <code>{c['user_id']}</code>): <b>{c['limit']} slots</b> (using {used})")

        lines.append(f"\n<i>Total upgraded users: {len(customs)}</i>")
        await update.message.reply_html("\n".join(lines))
        return

    raw_target = args[0].strip()
    if raw_target.lstrip("-").isdigit():
        target_uid = int(raw_target)
    else:
        target_uid = get_user_id_by_username(raw_target)
        if not target_uid:
            await update.message.reply_html(
                f"⚠️ Could not find user with username <b>{html.escape(raw_target)}</b> in database.\n"
                f"Please ensure they have used the bot before, or provide their numeric Telegram User ID."
            )
            return

    limit_val = get_user_scout_limit(target_uid)
    monitored = get_user_watchlist(target_uid)
    u_info = get_user_info(target_uid)
    u_disp = f"@{u_info['username']}" if u_info and u_info.get("username") else (u_info.get("first_name", "") if u_info else "")
    disp_suffix = f" ({u_disp})" if u_disp else ""

    await update.message.reply_html(
        f"👤 <b>User Watchlist Info</b>\n"
        f"User ID: <code>{target_uid}</code>{disp_suffix}\n"
        f"Limit: <b>{limit_val}</b> slots\n"
        f"Monitored ({len(monitored)}/{limit_val}): {', '.join('@' + c for c in monitored) if monitored else 'None'}"
    )


# ── Admin Multi-Server Cluster & Workload Distribution ─────────────────────────
async def cmd_cluster(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: View child worker cluster status, URLs, UptimeRobot, and creator workload distribution."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    text, kb = build_cluster_text()
    await update.message.reply_html(text, reply_markup=kb, disable_web_page_preview=True)


async def cmd_addserver(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: Manually register a child server URL and rebalance creators evenly."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    args = context.args
    if not args:
        await update.message.reply_html(
            "<b>Usage:</b> <code>/addserver &lt;server_url&gt; [server_name]</code>\n\n"
            "Example:\n"
            "<code>/addserver https://instabot-worker-2.onrender.com Worker2</code>"
        )
        return

    url = args[0].strip()
    name = " ".join(args[1:]).strip() if len(args) > 1 else f"Worker-{int(time.time()) % 1000}"

    # Auto-create UptimeRobot keep-alive monitor if API key configured
    uptimerobot_id = ""
    uptime_note = ""
    if os.getenv("UPTIMEROBOT_API_KEY", "").strip():
        ok_uptime, res_uptime = cloud_manager.create_uptimerobot_monitor(
            server_url=url,
            friendly_name=name,
        )
        if ok_uptime:
            uptimerobot_id = res_uptime
            uptime_note = f"✅ UptimeRobot 24/7 Keep-Alive Monitor created (ID: <code>{uptimerobot_id}</code>)"
        else:
            uptime_note = f"⚠️ UptimeRobot monitor skipped: {res_uptime}"
    else:
        uptime_note = "ℹ️ UptimeRobot monitor not created (UPTIMEROBOT_API_KEY not set in .env)"

    # Register in DB and automatically rebalance workload!
    server_id = add_child_server(
        name=name,
        url=url,
        uptimerobot_id=uptimerobot_id,
    )

    if server_id:
        status = get_cluster_status()
        servers = status["servers"]
        breakdown = "\n".join(
            f"  • <b>{html.escape(s['name'])}</b> (ID: {s['id']}): <b>{len(s['creators'])} creators</b>"
            for s in servers
        )
        await update.message.reply_html(
            f"🎉 <b>Child Server #{server_id} Added & Rebalanced!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🖥️ <b>Name:</b> {html.escape(name)}\n"
            f"🔗 <b>URL:</b> <code>{html.escape(url)}</code>\n"
            f"{uptime_note}\n\n"
            f"⚖️ <b>Even Workload Division:</b>\n"
            f"{breakdown}\n\n"
            f"🚀 <i>All {status['total_creators']} creators are now evenly distributed across your {len(servers)} servers!</i>",
            disable_web_page_preview=True,
        )
    else:
        await update.message.reply_html("⚠️ Failed to register server in database.")


async def cmd_delserver(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: Remove child server, delete UptimeRobot monitor, and rebalance remaining servers."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    args = context.args
    if not args:
        await update.message.reply_html("<b>Usage:</b> <code>/delserver &lt;server_id&gt;</code>")
        return

    try:
        server_id = int(args[0])
    except ValueError:
        await update.message.reply_html("⚠️ Server ID must be a number.")
        return

    server_info = get_child_server(server_id)
    if not server_info:
        await update.message.reply_html(f"⚠️ Server #{server_id} not found.")
        return

    # Delete UptimeRobot monitor if existed
    if server_info.get("uptimerobot_id"):
        cloud_manager.delete_uptimerobot_monitor(server_info["uptimerobot_id"])

    success = remove_child_server(server_id)
    if success:
        status = get_cluster_status()
        servers = status["servers"]
        breakdown = "\n".join(
            f"  • <b>{html.escape(s['name'])}</b> (ID: {s['id']}): <b>{len(s['creators'])} creators</b>"
            for s in servers
        ) if servers else "<i>No remaining child servers. Running standalone.</i>"

        await update.message.reply_html(
            f"🗑️ <b>Server #{server_id} ({html.escape(server_info['name'])}) Removed!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚖️ <b>Workload Rebalanced Across Remaining Servers:</b>\n"
            f"{breakdown}",
            disable_web_page_preview=True,
        )
    else:
        await update.message.reply_html(f"⚠️ Failed to remove server #{server_id}.")


async def cmd_rebalance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: Force recalculate and evenly rebalance creators across all active child servers."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    dist = rebalance_creator_workload()
    status = get_cluster_status()
    servers = status["servers"]

    if not servers:
        await update.message.reply_html(
            "ℹ️ No active child servers found. Creators unassigned (standalone mode)."
        )
        return

    breakdown = "\n".join(
        f"  • <b>{html.escape(s['name'])}</b> (ID: {s['id']}): <b>{len(s['creators'])} creators</b>\n"
        f"    Accounts: {', '.join('@'+c for c in s['creators']) if s['creators'] else 'None'}"
        for s in servers
    )
    await update.message.reply_html(
        f"⚖️ <b>Cluster Workload Rebalanced!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total Active Creators: <b>{status['total_creators']}</b>\n"
        f"Active Servers: <b>{len(servers)}</b>\n\n"
        f"{breakdown}",
        disable_web_page_preview=True,
    )


async def cmd_deploy_child(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command: 1-Click auto deploy child worker on Render and register in cluster."""
    user = update.effective_user
    if not user or not is_admin(user.id):
        return

    args = context.args
    if not args:
        await update.message.reply_html(
            "🚀 <b>Automated Render Child Deployer</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Deploys a new child worker web service on Render automatically using Render API!\n\n"
            "<b>Usage:</b>\n"
            "<code>/deploy_child &lt;render_api_key&gt;</code>\n\n"
            "📌 <b>How to get your Render API Key:</b>\n"
            "1. Go to <a href=\"https://dashboard.render.com/u/settings#api-keys\">Render Account Settings → API Keys</a>\n"
            "2. Click <b>Create API Key</b>\n"
            "3. Copy the key and run: <code>/deploy_child rnd_xxxxxx</code>\n\n"
            f"🌐 <i>GitHub Repo URL from .env: <code>{html.escape(os.getenv('GITHUB_REPO_URL', 'Not set'))}</code></i>",
            disable_web_page_preview=True,
        )
        return

    render_api_key = args[0].strip()
    repo_url = os.getenv("GITHUB_REPO_URL", "").strip()
    if len(args) > 1:
        repo_url = args[1].strip()

    if not repo_url:
        await update.message.reply_html(
            "⚠️ <b>Missing GITHUB_REPO_URL!</b>\n\n"
            "Please add your repository URL in <code>.env</code>:\n"
            "<code>GITHUB_REPO_URL=https://github.com/yourusername/instabot</code>\n"
            "Or pass it directly:\n"
            "<code>/deploy_child &lt;render_api_key&gt; &lt;github_repo_url&gt;</code>"
        )
        return

    progress_msg = await update.message.reply_html(
        "⏳ <b>Provisioning Child Worker on Render...</b>\n"
        "Connecting to Render API, setting up Python web service & environment variables..."
    )

    # Calculate next worker ID
    active_servers = get_active_child_servers()
    next_worker_id = len(active_servers) + 1

    db_url = os.getenv("DATABASE_URL", "").strip()
    bot_tok = BOT_TOKEN
    admin_id_str = ADMIN_ID

    ok, res = await asyncio.to_thread(
        cloud_manager.deploy_render_child_service,
        render_api_key=render_api_key,
        repo_url=repo_url,
        db_url=db_url,
        bot_token=bot_tok,
        admin_id=admin_id_str,
        worker_id=next_worker_id,
        service_name=f"instabot-scout-{next_worker_id}",
    )

    if not ok:
        await progress_msg.edit_text(
            f"❌ <b>Render Deployment Failed:</b>\n\n<code>{html.escape(str(res))}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    service_id = res.get("service_id", "")
    srv_url = res.get("url", "")
    srv_name = res.get("name", f"Worker-{next_worker_id}")

    # UptimeRobot monitor
    uptimerobot_id = ""
    uptime_note = ""
    if os.getenv("UPTIMEROBOT_API_KEY", "").strip():
        ok_uptime, res_uptime = await asyncio.to_thread(
            cloud_manager.create_uptimerobot_monitor,
            server_url=srv_url,
            friendly_name=srv_name,
        )
        if ok_uptime:
            uptimerobot_id = res_uptime
            uptime_note = f"✅ UptimeRobot 24/7 Monitor created (ID: <code>{uptimerobot_id}</code>)"
        else:
            uptime_note = f"⚠️ UptimeRobot monitor skipped: {res_uptime}"
    else:
        uptime_note = "ℹ️ UptimeRobot monitor not created (UPTIMEROBOT_API_KEY not configured in .env)"

    # Save in DB and rebalance workload
    server_id = add_child_server(
        name=srv_name,
        render_service_id=service_id,
        url=srv_url,
        uptimerobot_id=uptimerobot_id,
    )

    status = get_cluster_status()
    servers = status["servers"]
    breakdown = "\n".join(
        f"  • <b>{html.escape(s['name'])}</b> (ID: {s['id']}): <b>{len(s['creators'])} creators</b>"
        for s in servers
    )

    await progress_msg.edit_text(
        f"🎉 <b>Render Child Worker #{server_id} Deployed Successfully!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🖥️ <b>Service Name:</b> {html.escape(srv_name)}\n"
        f"🆔 <b>Render Service ID:</b> <code>{service_id}</code>\n"
        f"🔗 <b>Worker URL:</b> <code>{html.escape(srv_url)}</code>\n"
        f"{uptime_note}\n\n"
        f"⚖️ <b>Even Workload Division Across Cluster:</b>\n"
        f"{breakdown}\n\n"
        f"✨ <i>Worker #{server_id} will be live in 1-2 minutes and automatically scout its assigned creators!</i>",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ── Error handler ──────────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", context.error, exc_info=True)
    try:
        await alert_admin(
            context.bot,
            f"{E_WARNING} <b>Unhandled Error</b>\n\n"
            f"{E_CLOCK_TIME} {now_str()}\n"
            f"<pre>{html.escape(str(context.error)[:400])}</pre>",
        )
    except Exception:
        pass


# ── Lifecycle ──────────────────────────────────────────────────────────────────
async def post_init(application: Application) -> None:
    global _semaphore
    _semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    logger.info("Semaphore ready (max_concurrent=%d)", MAX_CONCURRENT)

    # Resume cookie reminder if it was active before restart
    if get_setting("cookie_alert_active", "0") == "1":
        start_cookie_reminder(application.job_queue)
        logger.info("Resumed cookie reminder from previous session")

    # Schedule 30-minute proactive cookie health check
    if not application.job_queue.get_jobs_by_name("cookie_health_check"):
        application.job_queue.run_repeating(
            cookie_health_check_job,
            interval=1800,  # 30 min
            first=120,      # first check in 2 min
            name="cookie_health_check",
        )
        logger.info("Registered 30-min cookie health check job")

    # Schedule Scout Queue Auto-Dispatcher (runs every 30s)
    if not application.job_queue.get_jobs_by_name("scout_dispatcher"):
        application.job_queue.run_repeating(
            scout_dispatcher_job,
            interval=30,  # 30 seconds
            first=5,       # first check in 5 sec
            name="scout_dispatcher",
        )
        logger.info("Registered 30-sec scout queue dispatcher job")


# ── Health Check Server (Render Web Service Port Binding) ──────────────────────
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - InstaLoader Bot is running")

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()

    def log_message(self, format, *args):
        # Silence access logs so console output stays clean
        pass


def start_health_server() -> None:
    """Runs a minimal HTTP server so Render Web Service port scan succeeds immediately."""
    port_env = os.environ.get("PORT", "8080")
    try:
        port = int(port_env)
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        logger.info("Health check server bound to 0.0.0.0:%d (Render port scan ready)", port)
        server.serve_forever()
    except Exception as exc:
        logger.warning("Could not bind health check server on port %s: %s", port_env, exc)


# ── Entry Point ────────────────────────────────────────────────────────────────
def main() -> None:
    # Ensure an asyncio event loop exists in the main thread (fixes Python 3.12+ / 3.14 on Render)
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    init_db()
    logger.info("Starting InstaLoader Bot…")

    # Start healthcheck HTTP server in background thread for Render Web Service port check
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .connect_timeout(30)
        .read_timeout(60)
        .write_timeout(180)
        .pool_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CommandHandler("admin",      cmd_admin))
    app.add_handler(CommandHandler("stats",      cmd_stats))
    app.add_handler(CommandHandler("cancel",     cmd_cancel))
    app.add_handler(CommandHandler("channels",   cmd_channels))
    app.add_handler(CommandHandler("addchannel", cmd_addchannel))
    app.add_handler(CommandHandler("delchannel", cmd_delchannel))
    app.add_handler(CommandHandler("setlink",    cmd_setlink))
    app.add_handler(CommandHandler("scout",      cmd_scout))
    app.add_handler(CommandHandler("dispatch",   cmd_dispatch))
    app.add_handler(CommandHandler(["watch", "watchlist"], cmd_watch))
    app.add_handler(CommandHandler("setlimit",   cmd_setlimit))
    app.add_handler(CommandHandler(["getlimit", "limits"], cmd_getlimit))
    app.add_handler(CommandHandler(["cluster", "servers"], cmd_cluster))
    app.add_handler(CommandHandler("deploy_child", cmd_deploy_child))
    app.add_handler(CommandHandler("addserver",    cmd_addserver))
    app.add_handler(CommandHandler("delserver",    cmd_delserver))
    app.add_handler(CommandHandler("rebalance",    cmd_rebalance))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

