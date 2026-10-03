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
import logging
import os
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
)
from downloader import cleanup_session, download_instagram, is_instagram_url, convert_json_cookies_to_netscape
import html
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

_semaphore: asyncio.Semaphore | None = None   # initialised in post_init
_waiting_count: int = 0                        # users queued but not yet downloading


# ── Helpers ────────────────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def admin_only(func):
    """Decorator: silently reject non-admins."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.effective_message.reply_text("⛔ Admin only.")
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
    """Check membership for all required channels. Returns list of unjoined channel dicts."""
    channels = get_all_channels()
    if not channels:
        return []
    unjoined = []
    for ch in channels:
        is_member = await check_channel_membership(bot, user_id, ch["chat_id"])
        if not is_member:
            unjoined.append(ch)
    return unjoined


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
        f"🕐 <code>{now_str()}</code>",
    )


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
# ── Reply Keyboard Button Labels ──────────────────────────────────────────────
# User buttons
BTN_HELP        = "📖 Help"
BTN_ABOUT       = "ℹ️ About Bot"

# Admin buttons
BTN_STATS       = "📊 Stats"
BTN_MAINT       = "🔧 Maintenance"
BTN_BROADCAST   = "📢 Broadcast"
BTN_CHANNELS    = "📺 Channels"
BTN_COOKIE      = "🍪 Cookie Status"
BTN_CLOSE_MENU  = "❌ Close Menu"

ALL_BTNS = {
    BTN_HELP, BTN_ABOUT,
    BTN_STATS, BTN_MAINT, BTN_BROADCAST,
    BTN_CHANNELS, BTN_COOKIE, BTN_CLOSE_MENU,
}


# ── Reply Keyboard Builders ────────────────────────────────────────────────────
def rkb_user() -> ReplyKeyboardMarkup:
    """Persistent bottom keyboard for regular users."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_HELP), KeyboardButton(BTN_ABOUT)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Paste an Instagram link...",
    )


def rkb_admin() -> ReplyKeyboardMarkup:
    """Persistent bottom keyboard for the admin."""
    maint_label = "🔧 Maintenance: ON" if is_maintenance() else "🔧 Maintenance: OFF"
    cookie_label = "🍪 Cookie: 🔴 Alert" if get_setting("cookie_alert_active", "0") == "1" else "🍪 Cookie: ✅ OK"
    chan_count = get_channel_count()
    chan_label = f"📺 Channels ({chan_count})"
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_STATS),       KeyboardButton(BTN_BROADCAST)],
            [KeyboardButton(maint_label),     KeyboardButton(chan_label)],
            [KeyboardButton(cookie_label)],
            [KeyboardButton(BTN_CLOSE_MENU)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Admin mode active...",
    )


# ── Keyboards (inline) ─────────────────────────────────────────────────────────
def kb_admin_panel() -> InlineKeyboardMarkup:
    maint       = "🟢 ON" if is_maintenance() else "⚫ OFF"
    cookie_flag = get_setting("cookie_alert_active", "0") == "1"
    chan_count  = get_channel_count()
    chan_label  = f"📺 Channels: {chan_count} Active" if chan_count > 0 else "📺 Channels: None ⚠️"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Stats",      callback_data="adm_stats"),
            InlineKeyboardButton("📢 Broadcast",  callback_data="adm_broadcast"),
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
    maint      = "🟢 ON" if is_maintenance() else "⚫ OFF"
    cookie     = f"{E_WARNING} Alert active" if get_setting("cookie_alert_active", "0") == "1" else f"{E_SPARKLES} OK"
    channels   = get_all_channels()
    ch_count   = len(channels)
    ch_summary = f"{ch_count} Active" if ch_count > 0 else "None"
    q_active   = (MAX_CONCURRENT - _semaphore._value) if _semaphore else 0
    return (
        f"{E_ARC_REACTOR} <b>Bot Statistics & Health</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 <b>Users</b>\n"
        f"  • Total registered : <code>{s['total_users']:,}</code>\n"
        f"  • Active today     : <code>{s['active_today']:,}</code>\n\n"
        f"📥 <b>Downloads</b>\n"
        f"  • Today            : <code>{s['today_ok']:,}</code> {E_CONFETTI}   <code>{s['today_fail']:,}</code> {E_BROKEN_HEART}\n"
        f"  • All time         : <code>{s['total_ok']:,}</code> {E_CONFETTI}   <code>{s['total_fail']:,}</code> {E_BROKEN_HEART}\n\n"
        f"{E_HEART_PULSE} <b>System</b>\n"
        f"  • Active downloads : <code>{q_active}/{MAX_CONCURRENT}</code>\n"
        f"  • Queue waiting    : <code>{_waiting_count}</code>\n"
        f"  • Maintenance      : {maint}\n"
        f"  • Cookie status    : {cookie}\n"
        f"  • Required Channels: <b>{ch_summary}</b>\n\n"
        f"{E_COLOR_DOTS} <code>{now_str()}</code>"
    )


# ── Command Handlers ───────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)

    # 1. Admin greeting & controls
    if is_admin(user.id):
        channels = get_all_channels()
        ch_count = len(channels)
        missing_links = [html.escape(c["title"]) for c in channels if not c["invite_link"].strip()]
        link_warn = ""
        if missing_links:
            link_warn = f"\n\n{E_WARNING} <i>Notice:</i> {len(missing_links)} channel(s) need an invite link: {', '.join(missing_links)}. Use /setlink or tap Channels below."

        await update.message.reply_text(
            f"{E_GOLDEN_MAZE} <b>Admin Control Panel</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Welcome back, <b>{html.escape(user.first_name)}</b>!\n"
            f"• Maintenance: <b>{'ON' if is_maintenance() else 'OFF'}</b>\n"
            f"• Required Channels: <b>{ch_count} Active</b>{link_warn}\n\n"
            f"{E_ARROW} Select an option below or send /admin for inline controls.",
            parse_mode=ParseMode.HTML,
            reply_markup=rkb_admin(),
        )
        return

    # 2. Regular user: check membership across all required channels
    unjoined = await get_unjoined_channels(context.bot, user.id)
    if unjoined:
        ch_list_str = "\n".join([f"  {E_HEART_BORDER} <b>{html.escape(c.get('title', 'Channel'))}</b>" for c in unjoined])
        await update.message.reply_text(
            f"{E_FLAME_BUTTERFLY} <b>Hey {html.escape(user.first_name)}! Welcome to InstaBot</b> 🤖\n"
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
    await update.message.reply_text(
        f"{E_FLAME_BUTTERFLY} <b>Hey {html.escape(user.first_name)}! Welcome to InstaBot</b> 🤖\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "I download Instagram <b>Reels, Posts & IGTV</b> for you.\n\n"
        f"{E_SPARKLES} <b>Features:</b>\n"
        f"• Best available quality {E_LIGHTNING}\n"
        f"• Clean metadata {E_BLACK_MASK} <i>(safe to repost)</i>\n"
        "• Monospace caption for 1-tap copy 📋\n\n"
        f"{E_ARROW} <b>Paste any Instagram link to get started!</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=rkb_user(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    channels = get_all_channels()
    channel_line = f"• Must join <b>{len(channels)} required channel(s)</b>\n" if channels else ""
    await update.message.reply_text(
        f"{E_ARC_REACTOR} <b>InstaBot — Help Guide</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{E_ARROW} <b>How to use:</b>\n"
        "1. Copy any public Instagram link\n"
        "2. Paste it here\n"
        f"3. Get your video or photos in seconds {E_LIGHTNING}\n\n"
        f"{E_SPARKLES} <b>Supported links:</b>\n"
        "<code>instagram.com/reel/...</code>\n"
        "<code>instagram.com/p/...</code>\n"
        "<code>instagram.com/tv/...</code>\n\n"
        f"{E_HEART_BORDER} <b>Requirements:</b>\n"
        f"{channel_line}"
        "• Public accounts only\n\n"
        f"{E_CONFETTI} <b>What you get:</b>\n"
        "• Highest quality video / images\n"
        f"• Metadata stripped {E_BLACK_MASK} (repost safe)\n"
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
    keyboard = rkb_admin() if is_admin(update.effective_user.id) else rkb_user()
    await update.message.reply_text(f"{E_WARNING} Action cancelled.", reply_markup=keyboard)


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
        label = "🟢 ON" if new_val == "1" else "⚫ OFF"
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
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
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
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu")]]),
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
            f"🆔 Chat ID: <code>{target_cid}</code>\n"
            f"🔗 Current Link: <code>{html.escape(cur_link)}</code>\n\n"
            "Send the new Telegram invite link (e.g. <code>https://t.me/+...</code> or <code>https://t.me/channel</code>).\n"
            "Send <code>-</code> to clear the link.\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu")]]),
        )

    elif data == "adm_cookie_clear":
        stop_cookie_reminder(context.job_queue)
        await q.edit_message_text(
            f"{E_SPARKLES} <b>Cookie alert cleared.</b>\n\n"
            "Make sure you've updated <code>cookies.txt</code> on Render, then turn off Maintenance.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back_admin(),
        )

    # ── User callback: multi-channel join check ───────────────────────────
    elif data == "check_joined":
        unjoined = await get_unjoined_channels(context.bot, uid)
        if not unjoined:
            await q.answer("Verification successful! Welcome!", show_alert=False)
            try:
                await q.edit_message_text(
                    f"{E_CONFETTI} <b>Access Granted!</b> {E_LIGHTNING}\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    "All required channel memberships verified!\n"
                    "You now have full access to <b>InstaBot</b> 🤖\n\n"
                    f"{E_ARROW} <b>Paste any Instagram link below to get started!</b>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            await context.bot.send_message(
                chat_id=uid,
                text="Choose an option or paste any Instagram link below 👇",
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

    upsert_user(user.id, user.username, user.first_name)

    # ── Admin state machine ────────────────────────────────────────────────
    state = context.user_data.get("state")

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
                await message.reply_text("⚠️ No channel to link. Add a channel first.", reply_markup=rkb_admin())
            return

    # ── Admin cookie document upload ───────────────────────────────────────
    if message and message.document and is_admin(user.id):
        doc = message.document
        fname = (doc.file_name or "").lower()
        if fname.endswith(".txt") or fname.endswith(".json"):
            status_up = await message.reply_text("📥 <b>Receiving cookie file...</b>", parse_mode=ParseMode.HTML)
            try:
                tg_file = await context.bot.get_file(doc.file_id)
                target_path = Path("cookies.txt")
                if fname.endswith(".json"):
                    temp_json = Path("temp_cookies.json")
                    await tg_file.download_to_drive(custom_path=temp_json)
                    ok = convert_json_cookies_to_netscape(temp_json, target_path)
                    temp_json.unlink(missing_ok=True)
                    if not ok:
                        await status_up.edit_text("❌ Failed to parse JSON cookies format. Please check the file.")
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
                    f"✅ Saved fresh <code>cookies.txt</code>\n"
                    f"☁️ Backed up to Neon PostgreSQL\n"
                    f"🟢 Maintenance mode turned <b>OFF</b>\n"
                    f"🔕 Cookie reminders stopped\n\n"
                    f"{E_LIGHTNING} Your bot is ready to download reels!",
                    parse_mode=ParseMode.HTML,
                    reply_markup=rkb_admin(),
                )
                return
            except Exception as e:
                logger.exception("Failed to process uploaded cookie file")
                await status_up.edit_text(f"❌ Error saving cookies: <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)
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

    # ── Queue capacity check ───────────────────────────────────────────────
    if _waiting_count >= MAX_QUEUE:
        await message.reply_text(
            f"{E_WARNING} <b>Queue Full</b>\n\n"
            "The bot is very busy right now.\n"
            "Please try again in a minute!",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Queue and download ─────────────────────────────────────────────────
    _waiting_count += 1
    q_pos = _waiting_count

    if _semaphore and _semaphore._value == 0:
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

    try:
        async with _semaphore:
            _waiting_count = max(0, _waiting_count - 1)

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
                        f"🔗 <code>{html.escape(text[:100])}</code>\n"
                        f"🕐 {now_str()}\n\n"
                        f"<pre>{html.escape(result.get('raw_error', '')[:400])}</pre>\n\n"
                        "⚠️ Maintenance auto-enabled. Please send fresh cookies.txt!",
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
                        f"🔗 <code>{html.escape(text[:100])}</code>\n"
                        f"🕐 {now_str()}\n"
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
                    with open(video_path, "rb") as vf:
                        await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_VIDEO)
                        await context.bot.send_video(
                            chat_id=message.chat_id,
                            video=vf,
                            caption=f"{E_CONFETTI} <b>Done!</b> Metadata stripped {E_BLACK_MASK}",
                            parse_mode=ParseMode.HTML,
                            supports_streaming=True,
                            write_timeout=180,
                            read_timeout=120,
                            connect_timeout=30,
                        )

                # ── Case B: Single Photo or Carousel ──────────────────────
                elif image_paths:
                    if len(image_paths) == 1:
                        with open(image_paths[0], "rb") as pf:
                            await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_PHOTO)
                            await context.bot.send_photo(
                                chat_id=message.chat_id,
                                photo=pf,
                                caption=f"{E_CONFETTI} <b>Done!</b> Metadata stripped {E_BLACK_MASK}",
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
                                cap = f"{E_CONFETTI} <b>Done!</b> Metadata stripped {E_BLACK_MASK}" if idx == 0 else None
                                pm = ParseMode.HTML if idx == 0 else None
                                media_group.append(InputMediaPhoto(media=f, caption=cap, parse_mode=pm))
                            await context.bot.send_media_group(chat_id=message.chat_id, media=media_group)
                        finally:
                            for f in opened_files:
                                try:
                                    f.close()
                                except Exception:
                                    pass

                # ── Monospace Caption (1-Tap Copy) ────────────────────────
                if caption_text:
                    chunks = [caption_text[i : i + 3900] for i in range(0, len(caption_text), 3900)]
                    header = f"{E_SPARKLES} <b>Caption</b> <i>(tap to copy):</i>\n\n"
                    for chunk in chunks:
                        await message.reply_text(
                            f"{header}<code>{html.escape(chunk)}</code>",
                            parse_mode=ParseMode.HTML,
                        )
                        header = ""

                log_download(user.id, text, True)
                logger.info("DL done  | user=%s", user.id)

                try:
                    await status_msg.delete()
                except Exception:
                    pass

            except Exception as exc:
                log_download(user.id, text, False)
                logger.exception("Send media failed")
                await alert_admin(
                    context.bot,
                    f"{E_WARNING} <b>Send Failed</b>\n\n"
                    f"👤 {html.escape(user.first_name)} (<code>{user.id}</code>)\n"
                    f"🕐 {now_str()}\n"
                    f"<pre>{html.escape(str(exc)[:400])}</pre>",
                )
                await status_msg.edit_text(
                    f"{E_BROKEN_HEART} <b>Couldn't send the media.</b>\n\n"
                    "The file may be too large for Telegram.\n"
                    "Please try again.",
                    parse_mode=ParseMode.HTML,
                )

            finally:
                main_path = video_path or (image_paths[0] if image_paths else None)
                cleanup_session(main_path)

    except Exception as exc:
        logger.exception("Unexpected error in handle_message")
        await alert_admin(
            context.bot,
            f"{E_WARNING} <b>Unexpected Error</b>\n\n"
            f"👤 {html.escape(user.first_name)} (<code>{user.id}</code>)\n"
            f"🕐 {now_str()}\n"
            f"<pre>{html.escape(str(exc)[:400])}</pre>",
        )
        try:
            await status_msg.edit_text("😕 Something went wrong. Please try again.")
        except Exception:
            pass



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
            f"3. Get your video or photos in seconds {E_LIGHTNING}\n\n"
            f"<b>Supported links:</b>\n"
            f"<code>instagram.com/reel/...</code>\n"
            f"<code>instagram.com/p/...</code>\n"
            f"<code>instagram.com/tv/...</code>\n\n"
            f"<b>Requirements:</b>\n"
            f"{channel_line}"
            f"• Public accounts only\n\n"
            f"<b>What you get:</b>\n"
            f"• Highest quality video & photos\n"
            f"• All metadata stripped {E_BLACK_MASK}\n"
            f"• Caption in <code>monospace</code> for easy copy",
            parse_mode=ParseMode.HTML,
        )
        return

    if text == BTN_ABOUT:
        channels = get_all_channels()
        await message.reply_text(
            f"{E_ARC_REACTOR} <b>About InstaBot</b>\n\n"
            f"A fast, clean Instagram downloader bot.\n\n"
            f"{E_LIGHTNING} <b>Tech stack:</b>\n"
            f"• <code>yt-dlp</code> — download engine\n"
            f"• <code>ffmpeg</code> — metadata stripper\n"
            f"• <code>python-telegram-bot</code> — bot framework\n"
            f"• <code>PostgreSQL / Neon</code> — multi-channel & stats database\n\n"
            f"{E_NEON_RINGS} Active Required Channels: <b>{len(channels)}</b>\n\n"
            f"{E_BLACK_MASK} <b>Privacy:</b>\n"
            f"All metadata is stripped before sending.\n"
            f"Temp files are deleted immediately after.\n\n"
            f"📬 Issues? Contact the admin.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Admin-only buttons ─────────────────────────────────────────────────
    if not is_admin(user.id):
        await message.reply_text(f"{E_WARNING} Admin only.")
        return

    if text == BTN_STATS:
        await message.reply_text(build_stats_text(), parse_mode=ParseMode.HTML)

    elif text == BTN_BROADCAST:
        context.user_data["state"] = "awaiting_broadcast"
        await message.reply_text(
            f"{E_RED_WOLF} <b>Broadcast</b>\n\n"
            f"Send the message to broadcast to all users.\n"
            f"Supports text, photos, videos — anything.\n\n"
            f"Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif text.startswith("🔧 Maintenance:"):
        new_val = "0" if is_maintenance() else "1"
        set_setting("maintenance_mode", new_val)
        if new_val == "0":
            stop_cookie_reminder(context.job_queue)
        label = "🟢 ON" if new_val == "1" else "⚫ OFF"
        await message.reply_text(
            f"{E_RING_LOADER} Maintenance mode → <b>{label}</b>",
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
            reply_markup=rkb_user(),
        )


# ── Admin helpers ──────────────────────────────────────────────────────────────
async def _do_broadcast(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
    user_ids = get_all_user_ids()
    progress = await message.reply_text(f"{E_RING_LOADER} Broadcasting to {len(user_ids):,} users...")

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
        f"✅ Sent   : <code>{sent:,}</code>\n"
        f"{E_WARNING} Failed : <code>{failed:,}</code> <i>(blocked/deleted)</i>\n"
        f"🕐 {now_str()}",
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
                    f"👉 <b>Two easy ways to finish:</b>\n"
                    f"1️⃣ <b>Forward any post</b> from that channel into this chat.\n"
                    f"2️⃣ Or send its <b>Chat ID</b> (e.g. <code>-1001234567890</code>).\n\n"
                    f"<i>I've saved your invite link and will attach it automatically!</i>\n"
                    f"Send /cancel to abort.",
                    parse_mode=ParseMode.HTML,
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
            f"Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
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
            f"⚠️ <b>Please make sure:</b>\n"
            f"1. You have already added this bot to your channel!\n"
            f"2. The Chat ID or username is typed correctly.\n"
            f"3. Or forward any message directly from the channel into this chat.\n\n"
            f"Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    # 4. Verify bot is an Administrator in the channel
    try:
        me = await context.bot.get_me()
        bot_member = await context.bot.get_chat_member(chat_id=chat.id, user_id=me.id)
        if bot_member.status not in ("administrator", "creator"):
            await status_msg.edit_text(
                f"{E_WARNING} <b>Bot is NOT an Administrator in {html.escape(chat.title or '')}!</b> ⛔\n\n"
                f"📺 Channel: <b>{html.escape(chat.title or '')}</b>\n"
                f"🆔 Chat ID: <code>{chat.id}</code>\n"
                f"🤖 Bot Role: <code>{bot_member.status}</code>\n\n"
                f"⚠️ Telegram <b>requires</b> bots to be an Administrator to verify if users have joined.\n\n"
                f"👉 <b>Steps to fix:</b>\n"
                f"1. Open channel <b>{html.escape(chat.title or '')}</b> in Telegram\n"
                f"2. Tap Channel Title ➔ Edit ➔ Administrators\n"
                f"3. Add @{me.username} as an Admin\n"
                f"4. Send the Chat ID <code>{chat.id}</code> or forward a message again here!",
                parse_mode=ParseMode.HTML,
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

    # 5. Determine invite link
    invite_link = custom_link or (pending_link if pending_link else "")
    if not invite_link:
        if chat.username:
            invite_link = f"https://t.me/{chat.username}"
        elif getattr(chat, "invite_link", None):
            invite_link = chat.invite_link
        else:
            try:
                created = await context.bot.create_chat_invite_link(chat.id, name="InstaBot")
                invite_link = created.invite_link
            except Exception:
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
            f"{E_CONFETTI} <b>Channel Added Successfully!</b> 🎉\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 <b>Channel:</b> {html.escape(title)}\n"
            f"🆔 <b>Chat ID:</b> <code>{chat.id}</code>\n"
            f"🤖 <b>Bot Role:</b> Administrator ✅\n"
            f"🔗 <b>Invite Link:</b> <a href=\"{invite_link}\">Click Here</a>\n\n"
            f"{E_BLACK_MASK} Users will now be required to join this channel before downloading.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_management(),
        )
    else:
        context.user_data["state"] = f"awaiting_ch_link:{chat.id}"
        await message.reply_text(
            f"{E_CONFETTI} <b>Channel Added:</b> <b>{html.escape(title)}</b>\n"
            f"🆔 <b>Chat ID:</b> <code>{chat.id}</code>\n"
            f"🤖 <b>Bot Role:</b> Administrator ✅\n\n"
            f"{E_WARNING} <b>Invite Link Needed for Private Channel</b>\n"
            f"The bot could not auto-generate an invite link.\n\n"
            f"👉 <b>Please send the channel's invite link now</b> (e.g. <code>https://t.me/+...</code>):\n"
            f"<i>(Or send /cancel to set it later)</i>",
            parse_mode=ParseMode.HTML,
        )


async def _do_update_channel_link(message, context: ContextTypes.DEFAULT_TYPE, chat_id: str) -> None:
    text = (message.text or "").strip()
    context.user_data.pop("state", None)

    if text == "-":
        update_channel_link(chat_id, "")
        await message.reply_text(f"{E_CONFETTI} Channel invite link cleared.", reply_markup=kb_channel_management())
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
        f"{E_CONFETTI} <b>Invite Link Updated!</b> 🎉\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📺 Channel: <b>{html.escape(title)}</b>\n"
        f"🆔 Chat ID: <code>{chat_id}</code>\n"
        f"🔗 Link: {html.escape(text)}\n\n"
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
        f"Send /cancel to abort.",
        parse_mode=ParseMode.HTML,
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
        await update.message.reply_text(f"{E_WARNING} No required channels configured yet.")
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
        await update.message.reply_text(f"{E_WARNING} No channels configured yet. Add one first with /addchannel.")
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
            f"Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(
        f"{E_NEON_RINGS} <b>Select a channel to update its invite link:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_channel_link_menu(),
    )


# ── Error handler ──────────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", context.error, exc_info=True)
    try:
        await alert_admin(
            context.bot,
            f"{E_WARNING} <b>Unhandled Error</b>\n\n"
            f"🕐 {now_str()}\n"
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


# ── Entry Point ────────────────────────────────────────────────────────────────
def main() -> None:
    # Ensure an asyncio event loop exists in the main thread (fixes Python 3.12+ / 3.14 on Render)
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    init_db()
    logger.info("Starting InstaLoader Bot…")

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
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

