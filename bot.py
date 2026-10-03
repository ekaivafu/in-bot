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
                parse_mode=ParseMode.MARKDOWN,
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
        "⏰ *Cookie Reminder*\n\n"
        "Instagram cookies are still expired/invalid.\n\n"
        "Steps to fix:\n"
        "1️⃣ Export fresh `cookies.txt` from your browser\n"
        "2️⃣ Update Secret File on Render\n"
        "3️⃣ Redeploy or restart the bot\n"
        "4️⃣ Open /admin → turn off Maintenance\n\n"
        f"🕐 {now_str()}",
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
            "📺 *Force-Sub Channel Management*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ *No required channels configured yet.*\n\n"
            "Users can download without joining any channel.\n"
            "Tap **➕ Add Channel** below to add one!"
        )

    lines = [
        "📺 *Force-Sub Channel Management*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Active Required Channels: *{len(channels)}*\n",
    ]
    for i, ch in enumerate(channels, 1):
        cid = ch["chat_id"]
        title = ch["title"]
        link = ch["invite_link"].strip()
        link_str = f"[Invite Link]({link})" if link else "⚠️ _No link set (use /setlink)_"
        lines.append(f"{i}️⃣ *{title}*")
        lines.append(f"   • Chat ID: `{cid}`")
        lines.append(f"   • Link: {link_str}\n")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("👇 Choose an action below:")
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
        if not link:
            cid = ch.get("chat_id", "")
            if cid.startswith("@"):
                link = f"https://t.me/{cid.lstrip('@')}"
            else:
                link = "https://t.me"
        btn_text = f"📢 Join {title}" if len(title) <= 24 else f"📢 Join Channel {i}"
        buttons.append([InlineKeyboardButton(btn_text, url=link)])

    buttons.append([InlineKeyboardButton("✅ I've Joined All — Continue", callback_data="check_joined")])
    return InlineKeyboardMarkup(buttons)


def kb_back_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")]])


# ── Stats text helper ──────────────────────────────────────────────────────────
def build_stats_text() -> str:
    s          = get_stats()
    maint      = "🟢 ON" if is_maintenance() else "⚫ OFF"
    cookie     = "🔴 Alert active" if get_setting("cookie_alert_active", "0") == "1" else "✅ OK"
    channels   = get_all_channels()
    ch_count   = len(channels)
    ch_summary = f"{ch_count} Active" if ch_count > 0 else "None"
    q_active   = (MAX_CONCURRENT - _semaphore._value) if _semaphore else 0
    return (
        "📊 *Bot Statistics*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "👥 *Users*\n"
        f"  • Total registered : `{s['total_users']:,}`\n"
        f"  • Active today     : `{s['active_today']:,}`\n\n"
        "📥 *Downloads*\n"
        f"  • Today            : `{s['today_ok']:,}` ✅   `{s['today_fail']:,}` ❌\n"
        f"  • All time         : `{s['total_ok']:,}` ✅   `{s['total_fail']:,}` ❌\n\n"
        "⚙️ *System*\n"
        f"  • Active downloads : `{q_active}/{MAX_CONCURRENT}`\n"
        f"  • Queue waiting    : `{_waiting_count}`\n"
        f"  • Maintenance      : {maint}\n"
        f"  • Cookie status    : {cookie}\n"
        f"  • Required Channels: *{ch_summary}*\n\n"
        f"🕐 `{now_str()}`"
    )


# ── Command Handlers ───────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)

    # 1. Admin greeting & controls
    if is_admin(user.id):
        channels = get_all_channels()
        ch_count = len(channels)
        missing_links = [c["title"] for c in channels if not c["invite_link"].strip()]
        link_warn = ""
        if missing_links:
            link_warn = f"\n\n⚠️ *Notice:* {len(missing_links)} channel(s) need an invite link: {', '.join(missing_links)}. Use `/setlink` or tap Channels below."

        await update.message.reply_text(
            f"👑 *Admin Control Panel*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Welcome back, *{user.first_name}*!\n"
            f"• Maintenance: *{'ON' if is_maintenance() else 'OFF'}*\n"
            f"• Required Channels: *{ch_count} Active*{link_warn}\n\n"
            "Select an option below or send /admin for inline controls.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=rkb_admin(),
        )
        return

    # 2. Regular user: check membership across all required channels
    unjoined = await get_unjoined_channels(context.bot, user.id)
    if unjoined:
        ch_list_str = "\n".join([f"  • *{c.get('title', 'Channel')}*" for c in unjoined])
        await update.message.reply_text(
            f"👋 *Hey {user.first_name}!* Welcome to *InstaBot* 🤖\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "To download Instagram *Reels, Posts & IGTV*, "
            "you must first join our official channel(s):\n\n"
            f"{ch_list_str}\n\n"
            "👉 Tap **Join** for each channel below, then tap **I've Joined All** to unlock the bot:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_force_sub(unjoined),
        )
        return

    # 3. Regular user (all joined or no channel): send clean welcome
    await update.message.reply_text(
        f"👋 *Hey {user.first_name}!* Welcome to *InstaBot* 🤖\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "I download Instagram *Reels, Posts & IGTV* for you.\n\n"
        "✨ *Features:*\n"
        "• Best available quality ⚡\n"
        "• Clean metadata 🔒 _(safe to repost)_\n"
        "• Monospace caption for 1-tap copy 📋\n\n"
        "🚀 *Paste any Instagram link to get started!*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=rkb_user(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    channels = get_all_channels()
    channel_line = f"• Must join *{len(channels)} required channel(s)*\n" if channels else ""
    await update.message.reply_text(
        "📖 *InstaBot — Help*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "*How to use:*\n"
        "1. Copy any public Instagram link\n"
        "2. Paste it here\n"
        "3. Get your video or photos in seconds ⚡\n\n"
        "*Supported links:*\n"
        "`instagram.com/reel/...`\n"
        "`instagram.com/p/...`\n"
        "`instagram.com/tv/...`\n\n"
        "*Requirements:*\n"
        f"{channel_line}"
        "• Public accounts only\n\n"
        "*What you get:*\n"
        "• Highest quality video / images\n"
        "• Metadata stripped (repost safe)\n"
        "• Caption in `monospace` for easy copy\n\n"
        "❓ Problems? Contact the admin.",
        parse_mode=ParseMode.MARKDOWN,
    )


@admin_only
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Open admin panel — shows both reply keyboard + inline panel."""
    await update.message.reply_text(
        "🔧 *Admin Panel*\n"
        "_Select an option from the menu below:_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=rkb_admin(),
    )
    await update.message.reply_text(
        "Inline controls:",
        reply_markup=kb_admin_panel(),
    )


@admin_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(build_stats_text(), parse_mode=ParseMode.MARKDOWN)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
    context.user_data.pop("pending_link", None)
    keyboard = rkb_admin() if is_admin(update.effective_user.id) else rkb_user()
    await update.message.reply_text("❌ Action cancelled.", reply_markup=keyboard)


# ── Animated Progress Helper ───────────────────────────────────────────────────
async def animate_progress(status_msg, stop_event: asyncio.Event) -> None:
    """
    Lightweight animated progress ticker.
    Zero extra RAM, strictly respects Telegram rate limits (1.2s - 2.0s intervals).
    """
    frames = [
        ("⚡ *Connecting to Instagram...*\n`[▰▱▱▱▱▱▱▱▱▱]` 15%\n_Initializing secure stream..._", 1.2),
        ("📥 *Fetching Media Data...*\n`[▰▰▰▱▱▱▱▱▱▱]` 35%\n_Resolving highest quality source..._", 1.5),
        ("🚀 *Downloading Media...*\n`[▰▰▰▰▰▰▱▱▱▱]` 65%\n_Bypassing restrictions..._", 1.8),
        ("✨ *Processing & Cleaning Metadata...*\n`[▰▰▰▰▰▰▰▰▱▱]` 85%\n_Stripping tracking info for safety..._", 2.0),
    ]
    for text, delay in frames:
        try:
            if stop_event.is_set():
                break
            await asyncio.sleep(delay)
            if stop_event.is_set():
                break
            await status_msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        except (TelegramError, Exception):
            pass


# ── Callback Handler ───────────────────────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q    = update.callback_query
    data = q.data
    uid  = q.from_user.id
    await q.answer()

    # ── Admin callbacks (guard every one) ─────────────────────────────────
    if data.startswith("adm_") and not is_admin(uid):
        await q.answer("⛔ Admin only.", show_alert=True)
        return

    if data == "adm_panel":
        await q.edit_message_text(
            "🔧 *Admin Panel*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_admin_panel(),
        )

    elif data == "adm_stats":
        await q.edit_message_text(
            build_stats_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back_admin(),
        )

    elif data == "adm_maint_toggle":
        new_val = "0" if is_maintenance() else "1"
        set_setting("maintenance_mode", new_val)
        if new_val == "0":
            stop_cookie_reminder(context.job_queue)
        label = "🟢 ON" if new_val == "1" else "⚫ OFF"
        await q.edit_message_text(
            f"🔧 *Admin Panel*\n\n_Maintenance toggled → {label}_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_admin_panel(),
        )

    elif data == "adm_broadcast":
        context.user_data["state"] = "awaiting_broadcast"
        await q.edit_message_text(
            "📢 *Broadcast*\n\n"
            "Send the message you want to broadcast to all users.\n"
            "Supports text, photos, videos — anything.\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )

    elif data == "adm_chan_menu":
        await q.edit_message_text(
            build_channel_list_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_channel_management(),
            disable_web_page_preview=True,
        )

    elif data == "adm_chan_add":
        context.user_data["state"] = "awaiting_channel_add"
        context.user_data.pop("pending_ch_link", None)
        await q.edit_message_text(
            "➕ *Add Required Channel*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send any of the following:\n"
            "1️⃣ **Chat ID** (e.g. `-1001234567890`)\n"
            "2️⃣ **Username** (e.g. `@mychannel`)\n"
            "3️⃣ **Link** (e.g. `https://t.me/channel` or `-100... https://t.me/+...`)\n"
            "4️⃣ Or simply **forward any post from your channel** here!\n\n"
            "⚠️ *Make sure the bot is added as an Administrator in your channel first!*\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu")]]),
        )

    elif data == "adm_chan_del_menu":
        channels = get_all_channels()
        if not channels:
            await q.answer("No channels to remove!", show_alert=True)
            return
        await q.edit_message_text(
            "🗑️ *Remove Required Channel*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Tap a channel below to remove it from the force-sub requirement:",
            parse_mode=ParseMode.MARKDOWN,
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
                "🗑️ *Remove Required Channel*\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Tap a channel below to remove it from the force-sub requirement:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_channel_delete_menu(),
            )
        else:
            await q.edit_message_text(
                build_channel_list_text(),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_channel_management(),
            )

    elif data == "adm_del_all":
        clear_all_channels()
        await q.answer("🗑️ All channels removed!", show_alert=True)
        await q.edit_message_text(
            build_channel_list_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_channel_management(),
        )

    elif data == "adm_chan_link_menu":
        channels = get_all_channels()
        if not channels:
            await q.answer("No channels configured yet! Add one first.", show_alert=True)
            return
        await q.edit_message_text(
            "🔗 *Set / Update Channel Invite Link*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Select a channel to set or update its invite link:",
            parse_mode=ParseMode.MARKDOWN,
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
            f"🔗 *Update Invite Link*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 Channel: *{ch_title}*\n"
            f"🆔 Chat ID: `{target_cid}`\n"
            f"🔗 Current Link: `{cur_link}`\n\n"
            "Send the new Telegram invite link (e.g. `https://t.me/+...` or `https://t.me/channel`).\n"
            "Send `-` to clear the link.\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Channels", callback_data="adm_chan_menu")]]),
        )

    elif data == "adm_cookie_clear":
        stop_cookie_reminder(context.job_queue)
        await q.edit_message_text(
            "✅ Cookie alert cleared.\n\n"
            "Make sure you've updated `cookies.txt` on Render, then turn off Maintenance.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back_admin(),
        )

    # ── User callback: multi-channel join check ───────────────────────────
    elif data == "check_joined":
        unjoined = await get_unjoined_channels(context.bot, uid)
        if not unjoined:
            await q.answer("🎉 Verification successful! Welcome!", show_alert=False)
            try:
                await q.edit_message_text(
                    "🎉 *Access Granted!* 🚀\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    "All required channel memberships verified!\n"
                    "You now have full access to *InstaBot* 🤖\n\n"
                    "👉 *Paste any Instagram link below to get started!*",
                    parse_mode=ParseMode.MARKDOWN,
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
                ch_list_str = "\n".join([f"  • *{c.get('title', 'Channel')}*" for c in unjoined])
                await q.edit_message_text(
                    f"🔒 *Subscription Incomplete* ⚠️\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"You still need to join the following *{remaining}* channel(s):\n\n"
                    f"{ch_list_str}\n\n"
                    "👉 Tap **Join** below, then tap **I've Joined All**:",
                    parse_mode=ParseMode.MARKDOWN,
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
            status_up = await message.reply_text("📥 *Receiving cookie file...*", parse_mode=ParseMode.MARKDOWN)
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
                    "🍪 *Cookies Updated Successfully!*\n\n"
                    "✅ Saved fresh `cookies.txt`\n"
                    "☁️ Backed up to Neon PostgreSQL\n"
                    "✅ Maintenance mode turned *OFF*\n"
                    "✅ Cookie reminders stopped\n\n"
                    "Your bot is ready to download reels! 🚀",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=rkb_admin(),
                )
                return
            except Exception as e:
                logger.exception("Failed to process uploaded cookie file")
                await status_up.edit_text(f"❌ Error saving cookies: `{e}`", parse_mode=ParseMode.MARKDOWN)
                return

    # ── Reply Keyboard Button Presses ─────────────────────────────────────
    if text in ALL_BTNS or text.startswith("🔧 Maintenance:") or text.startswith("🍪 Cookie:") or text.startswith("📺 Channels"):
        await _handle_button(text, user, message, context)
        return

    # ── Maintenance gate ───────────────────────────────────────────────────
    if is_maintenance() and not is_admin(user.id):
        await message.reply_text(
            "🔧 *Under Maintenance*\n\n"
            "The bot is temporarily down for updates.\n"
            "Please check back in a few minutes!",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── Multi-channel membership gate ──────────────────────────────────────
    if not is_admin(user.id):
        unjoined = await get_unjoined_channels(context.bot, user.id)
        if unjoined:
            ch_list_str = "\n".join([f"  • *{c.get('title', 'Channel')}*" for c in unjoined])
            await message.reply_text(
                "🔒 *Subscription Required*\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "You must join our official channel(s) before downloading:\n\n"
                f"{ch_list_str}\n\n"
                "👉 Tap **Join** for each channel below, then tap **I've Joined All**:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_force_sub(unjoined),
            )
            return

    # ── URL check ─────────────────────────────────────────────────────────
    if not is_instagram_url(text):
        await message.reply_text(
            "🤔 That doesn't look like an Instagram link.\n\n"
            "Send something like:\n"
            "`https://www.instagram.com/reel/ABC123/`\n"
            "`https://www.instagram.com/p/ABC123/`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── Queue capacity check ───────────────────────────────────────────────
    if _waiting_count >= MAX_QUEUE:
        await message.reply_text(
            "🚦 *Queue Full*\n\n"
            "The bot is very busy right now.\n"
            "Please try again in a minute!",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── Queue and download ─────────────────────────────────────────────────
    _waiting_count += 1
    q_pos = _waiting_count

    if _semaphore and _semaphore._value == 0:
        status_msg = await message.reply_text(
            f"⏳ *Queued* — you're #{q_pos} in line.\n"
            "Hang tight, your download will start shortly...",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        status_msg = await message.reply_text(
            "⚡ *Connecting to Instagram...*\n"
            "`[▰▱▱▱▱▱▱▱▱▱]` 15%\n"
            "_Initializing secure stream..._",
            parse_mode=ParseMode.MARKDOWN,
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
                        "🚨 *Cookie Error — Maintenance ON*\n\n"
                        f"👤 User: {user.first_name} (@{user.username or 'N/A'}) · `{user.id}`\n"
                        f"🔗 `{text[:100]}`\n"
                        f"🕐 {now_str()}\n\n"
                        f"```\n{result.get('raw_error', '')[:400]}\n```\n\n"
                        "⚠️ Maintenance auto-enabled. Please send fresh cookies.txt!",
                    )
                    await status_msg.edit_text(
                        "🔧 *Bot is entering maintenance mode.*\n\nPlease try again in a little while.",
                        parse_mode=ParseMode.MARKDOWN,
                    )
                else:
                    await alert_admin(
                        context.bot,
                        f"❌ *Download Failed*\n\n"
                        f"👤 {user.first_name} (@{user.username or 'N/A'}) · `{user.id}`\n"
                        f"🔗 `{text[:100]}`\n"
                        f"🕐 {now_str()}\n"
                        f"Type: `{err_type}`\n\n"
                        f"```\n{result.get('raw_error', result.get('error', ''))[:400]}\n```",
                    )
                    if err_type == "private":
                        await status_msg.edit_text(
                            "🔒 *Private Account*\n\n"
                            "This post is from a private account.\n"
                            "I can only download from public accounts.",
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    elif err_type == "not_found":
                        await status_msg.edit_text(
                            "🗑️ *Post Not Found*\n\n"
                            "This Instagram reel or post was removed, deleted, or the link is broken.",
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    else:
                        await status_msg.edit_text(
                            "😕 *Download Failed*\n\n"
                            "Possible reasons:\n"
                            "• The link is invalid or expired\n"
                            "• The post was deleted\n"
                            "• Instagram temporarily restricted access\n\n"
                            "Please try again shortly.",
                            parse_mode=ParseMode.MARKDOWN,
                        )
                return

            # ── Upload Phase (Animated transition) ────────────────────────
            try:
                await status_msg.edit_text(
                    "📤 *Uploading to Telegram...*\n"
                    "`[▰▰▰▰▰▰▰▰▰▰]` 100%\n"
                    "_Almost there!_",
                    parse_mode=ParseMode.MARKDOWN,
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
                            caption="✅ *Done!* Metadata stripped 🔒",
                            parse_mode=ParseMode.MARKDOWN,
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
                                caption="✅ *Done!* Metadata stripped 🔒",
                                parse_mode=ParseMode.MARKDOWN,
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
                                cap = "✅ *Done!* Metadata stripped 🔒" if idx == 0 else None
                                pm = ParseMode.MARKDOWN if idx == 0 else None
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
                    safe   = caption_text.replace("`", "'")
                    chunks = [safe[i : i + 3900] for i in range(0, len(safe), 3900)]
                    header = "📋 *Caption* _(tap to copy):_\n\n"
                    for chunk in chunks:
                        await message.reply_text(
                            f"{header}`{chunk}`",
                            parse_mode=ParseMode.MARKDOWN,
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
                    f"❌ *Send Failed*\n\n"
                    f"👤 {user.first_name} · `{user.id}`\n"
                    f"🕐 {now_str()}\n"
                    f"```\n{str(exc)[:400]}\n```",
                )
                await status_msg.edit_text(
                    "😕 *Couldn't send the media.*\n\n"
                    "The file may be too large for Telegram.\n"
                    "Please try again.",
                    parse_mode=ParseMode.MARKDOWN,
                )

            finally:
                main_path = video_path or (image_paths[0] if image_paths else None)
                cleanup_session(main_path)

    except Exception as exc:
        logger.exception("Unexpected error in handle_message")
        await alert_admin(
            context.bot,
            f"💥 *Unexpected Error*\n\n"
            f"👤 {user.first_name} · `{user.id}`\n"
            f"🕐 {now_str()}\n"
            f"```\n{str(exc)[:400]}\n```",
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
        channel_line = f"• Must join *{len(channels)} required channel(s)*\n" if channels else ""
        await message.reply_text(
            "📖 *Help — InstaBot*\n\n"
            "*How to use:*\n"
            "1. Copy any public Instagram link\n"
            "2. Paste it in this chat\n"
            "3. Get your video or photos in seconds ⚡\n\n"
            "*Supported links:*\n"
            "`instagram.com/reel/...`\n"
            "`instagram.com/p/...`\n"
            "`instagram.com/tv/...`\n\n"
            "*Requirements:*\n"
            f"{channel_line}"
            "• Public accounts only\n\n"
            "*What you get:*\n"
            "• Highest quality video & photos\n"
            "• All metadata stripped 🔒\n"
            "• Caption in `monospace` for easy copy",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if text == BTN_ABOUT:
        channels = get_all_channels()
        await message.reply_text(
            "ℹ️ *About InstaBot*\n\n"
            "A fast, clean Instagram downloader bot.\n\n"
            "⚙️ *Tech stack:*\n"
            "• `yt-dlp` — download engine\n"
            "• `ffmpeg` — metadata stripper\n"
            "• `python-telegram-bot` — bot framework\n"
            "• `SQLite` — multi-channel & stats database\n\n"
            f"📺 Active Required Channels: *{len(channels)}*\n\n"
            "🔒 *Privacy:*\n"
            "All metadata is stripped before sending.\n"
            "Temp files are deleted immediately after.\n\n"
            "📬 Issues? Contact the admin.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── Admin-only buttons ─────────────────────────────────────────────────
    if not is_admin(user.id):
        await message.reply_text("⛔ Admin only.")
        return

    if text == BTN_STATS:
        await message.reply_text(build_stats_text(), parse_mode=ParseMode.MARKDOWN)

    elif text == BTN_BROADCAST:
        context.user_data["state"] = "awaiting_broadcast"
        await message.reply_text(
            "📢 *Broadcast*\n\n"
            "Send the message to broadcast to all users.\n"
            "Supports text, photos, videos — anything.\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )

    elif text.startswith("🔧 Maintenance:"):
        new_val = "0" if is_maintenance() else "1"
        set_setting("maintenance_mode", new_val)
        if new_val == "0":
            stop_cookie_reminder(context.job_queue)
        label = "🟢 ON" if new_val == "1" else "⚫ OFF"
        await message.reply_text(
            f"🔧 Maintenance mode → *{label}*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=rkb_admin(),
        )

    elif text.startswith("📺 Channels") or text == BTN_CHANNELS:
        await message.reply_text(
            build_channel_list_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_channel_management(),
            disable_web_page_preview=True,
        )

    elif text.startswith("🍪 Cookie:"):
        if get_setting("cookie_alert_active", "0") == "1":
            stop_cookie_reminder(context.job_queue)
            await message.reply_text(
                "✅ Cookie alert cleared.\n\n"
                "Remember to update `cookies.txt` on Render, then turn off Maintenance.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=rkb_admin(),
            )
        else:
            await message.reply_text(
                "🍪 Cookie status: *✅ OK*\n\nNo active alerts.",
                parse_mode=ParseMode.MARKDOWN,
            )

    elif text == BTN_CLOSE_MENU:
        await message.reply_text(
            "✅ Admin menu closed. You're back to normal user mode.",
            reply_markup=rkb_user(),
        )


# ── Admin helpers ──────────────────────────────────────────────────────────────
async def _do_broadcast(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
    user_ids = get_all_user_ids()
    progress = await message.reply_text(f"📢 Broadcasting to {len(user_ids):,} users...")

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
        f"📢 *Broadcast Complete*\n\n"
        f"✅ Sent   : `{sent:,}`\n"
        f"❌ Failed : `{failed:,}` _(blocked/deleted)_\n"
        f"🕐 {now_str()}",
        parse_mode=ParseMode.MARKDOWN,
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
                    "🔗 *Private Invite Link Received!*\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Telegram Bot API needs the channel's **Chat ID** to verify member subscriptions.\n\n"
                    "👉 **Two easy ways to finish:**\n"
                    "1️⃣ **Forward any post** from that channel into this chat.\n"
                    "2️⃣ Or send its **Chat ID** (e.g. `-1001234567890`).\n\n"
                    "_I've saved your invite link and will attach it automatically!_\n"
                    "Send /cancel to abort.",
                    parse_mode=ParseMode.MARKDOWN,
                )
                return

    if not target:
        await message.reply_text(
            "❌ *Invalid format.*\n\n"
            "Please provide:\n"
            "• **Chat ID** (e.g. `-1001234567890`)\n"
            "• **Username** (e.g. `@mychannel`)\n"
            "• **Link** (e.g. `https://t.me/mychannel` or `-100... https://t.me/+...`)\n"
            "• Or **forward any post from your channel** here!\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    status_msg = await message.reply_text("🔍 *Verifying channel and checking bot permissions...*", parse_mode=ParseMode.MARKDOWN)

    # 3. Verify channel exists and bot can access it
    try:
        chat = await context.bot.get_chat(target)
    except Exception as exc:
        logger.warning("get_chat failed for %s: %s", target, exc)
        await status_msg.edit_text(
            f"❌ *Could not find or access that channel!*\n\n"
            f"Target: `{target}`\n"
            f"Error: `{str(exc)}`\n\n"
            "⚠️ **Please make sure:**\n"
            "1. You have already added this bot to your channel!\n"
            "2. The Chat ID or username is typed correctly.\n"
            "3. Or forward any message directly from the channel into this chat.\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # 4. Verify bot is an Administrator in the channel
    try:
        me = await context.bot.get_me()
        bot_member = await context.bot.get_chat_member(chat_id=chat.id, user_id=me.id)
        if bot_member.status not in ("administrator", "creator"):
            await status_msg.edit_text(
                f"❌ *Bot is NOT an Administrator in {chat.title}!* ⛔\n\n"
                f"📺 Channel: *{chat.title}*\n"
                f"🆔 Chat ID: `{chat.id}`\n"
                f"🤖 Bot Role: `{bot_member.status}`\n\n"
                f"⚠️ Telegram **requires** bots to be an Administrator to verify if users have joined.\n\n"
                f"👉 **Steps to fix:**\n"
                f"1. Open channel *{chat.title}* in Telegram\n"
                f"2. Tap Channel Title ➔ Edit ➔ Administrators\n"
                f"3. Add @{me.username} as an Admin\n"
                f"4. Send the Chat ID `{chat.id}` or forward a message again here!",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    except Exception as exc:
        logger.warning("get_chat_member failed for bot in %s: %s", chat.id, exc)
        await status_msg.edit_text(
            f"❌ *Failed to verify bot admin permissions in {chat.title}:*\n\n`{str(exc)}`\n\n"
            "Make sure the bot is an Administrator in the channel.",
            parse_mode=ParseMode.MARKDOWN,
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
            f"✅ *Channel Added Successfully!* 🎉\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 *Channel:* {title}\n"
            f"🆔 *Chat ID:* `{chat.id}`\n"
            f"🤖 *Bot Role:* Administrator ✅\n"
            f"🔗 *Invite Link:* [Click Here]({invite_link})\n\n"
            "🔒 Users will now be required to join this channel before downloading.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_channel_management(),
        )
    else:
        context.user_data["state"] = f"awaiting_ch_link:{chat.id}"
        await message.reply_text(
            f"✅ *Channel Added:* *{title}*\n"
            f"🆔 *Chat ID:* `{chat.id}`\n"
            f"🤖 *Bot Role:* Administrator ✅\n\n"
            "⚠️ *Invite Link Needed for Private Channel*\n"
            "The bot could not auto-generate an invite link.\n\n"
            "👉 **Please send the channel's invite link now** (e.g. `https://t.me/+...`):\n"
            "*(Or send /cancel to set it later)*",
            parse_mode=ParseMode.MARKDOWN,
        )


async def _do_update_channel_link(message, context: ContextTypes.DEFAULT_TYPE, chat_id: str) -> None:
    text = (message.text or "").strip()
    context.user_data.pop("state", None)

    if text == "-":
        update_channel_link(chat_id, "")
        await message.reply_text("✅ Channel invite link cleared.", reply_markup=kb_channel_management())
        return

    if not (text.startswith("https://t.me/") or text.startswith("http://t.me/") or text.startswith("@")):
        await message.reply_text(
            "❌ *Invalid link format.*\n\nPlease send a valid link like `https://t.me/+...` or `https://t.me/channel`.\nUse /setlink to try again.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_channel_management(),
        )
        return

    update_channel_link(chat_id, text)
    channels = get_all_channels()
    ch = next((c for c in channels if str(c["chat_id"]) == str(chat_id)), None)
    title = ch["title"] if ch else chat_id

    await message.reply_text(
        f"✅ *Invite Link Updated!* 🎉\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📺 Channel: *{title}*\n"
        f"🆔 Chat ID:* `{chat_id}`\n"
        f"🔗 Link: {text}\n\n"
        "The **Join** button is now updated for users!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_channel_management(),
    )


# ── Channel Management Commands ────────────────────────────────────────────────
@admin_only
async def cmd_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manage force-sub channels."""
    await update.message.reply_text(
        build_channel_list_text(),
        parse_mode=ParseMode.MARKDOWN,
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
        "➕ *Add Required Channel*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Send any of the following:\n"
        "1️⃣ **Chat ID** (e.g. `-1001234567890`)\n"
        "2️⃣ **Username** (e.g. `@mychannel`)\n"
        "3️⃣ **Link** (e.g. `https://t.me/channel` or `-100... https://t.me/+...`)\n"
        "4️⃣ Or forward any post from your channel here!\n\n"
        "Send /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
    )


@admin_only
async def cmd_delchannel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Quick command to delete a channel."""
    args = context.args
    if args:
        target = args[0].strip()
        ok = remove_channel(target)
        if ok:
            await update.message.reply_text(f"✅ Channel `{target}` removed.", parse_mode=ParseMode.MARKDOWN, reply_markup=kb_channel_management())
        else:
            await update.message.reply_text(f"❌ Channel `{target}` not found in database.", parse_mode=ParseMode.MARKDOWN)
        return

    channels = get_all_channels()
    if not channels:
        await update.message.reply_text("⚠️ No required channels configured yet.")
        return

    await update.message.reply_text(
        "🗑️ *Select a channel to remove:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_channel_delete_menu(),
    )


@admin_only
async def cmd_setlink(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set or update invite link for a channel."""
    args = context.args
    channels = get_all_channels()
    if not channels:
        await update.message.reply_text("⚠️ No channels configured yet. Add one first with /addchannel.")
        return

    if len(args) >= 2:
        cid = args[0].strip()
        link = args[1].strip()
        update_channel_link(cid, "" if link == "-" else link)
        await update.message.reply_text(f"✅ Link updated for `{cid}`: {link}", parse_mode=ParseMode.MARKDOWN)
        return

    if len(channels) == 1:
        cid = channels[0]["chat_id"]
        context.user_data["state"] = f"awaiting_ch_link:{cid}"
        await update.message.reply_text(
            f"🔗 *Set Invite Link for {channels[0]['title']}*\n\n"
            f"Send the invite link (e.g. `https://t.me/+...` or `https://t.me/channel`):\n"
            "Send `-` to clear.\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    await update.message.reply_text(
        "🔗 *Select a channel to update its invite link:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_channel_link_menu(),
    )


# ── Error handler ──────────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", context.error, exc_info=True)
    try:
        await alert_admin(
            context.bot,
            f"💥 *Unhandled Error*\n\n"
            f"🕐 {now_str()}\n"
            f"```\n{str(context.error)[:400]}\n```",
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

