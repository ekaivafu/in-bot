"""
child/scout.py
──────────────
Autonomous Instagram Viral Reel Hunter (Child Server Worker).

Designed to run independently on a secondary Render service or locally:
• Reads creator watchlist from shared database (Neon PostgreSQL).
• Inspects target creator profiles without login using Chrome-TLS impersonation.
• Filters reels strictly matching criteria:
    - Age: 0 to 5 days old
    - Minimum Likes: >= 5,000 (configurable via MIN_LIKES)
• Skips already-seen reels with zero duplicate processing.
• Downloads qualified reels anonymously with anti-detection protection:
    - Strips all original tracking metadata
    - Micro-adjusts video stream to generate fresh cryptographic hashes
    - Strictly limits ffmpeg to 2 threads (Render 512MB RAM safe)
• Uploads video once to Telegram (Admin/Channel) to obtain a permanent file_id.
• Stores file_id in shared database (media_cache & scout_queue).
• Contains a built-in HTTP health check server on $PORT for Render Web Service hosting.
"""

import argparse
import asyncio
from datetime import datetime, timezone
import html
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
import random
import re
import sys
import threading
import time
from pathlib import Path

# Add root project directory to sys.path so we can share database.py and downloader.py
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

from curl_cffi import requests
import yt_dlp
import telegram
from telegram.request import HTTPXRequest

import database
import downloader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("scout_worker")

# ── Configuration ─────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = os.getenv("ADMIN_ID", "").strip()
TARGET_CHAT_ID = os.getenv("SCOUT_TARGET_CHAT", ADMIN_ID).strip()
MIN_LIKES = int(os.getenv("MIN_LIKES", "5000"))
MAX_AGE_DAYS = float(os.getenv("MAX_AGE_DAYS", "5.0"))
SCOUT_INTERVAL_MINUTES = int(os.getenv("SCOUT_INTERVAL_MINUTES", "30"))
CREATOR_GAP_SECONDS = int(os.getenv("CREATOR_GAP_SECONDS", "300"))  # Minimum 5-minute gap between accounts
PORT = int(os.getenv("PORT", "8080"))

# Worker ID for cluster sharding (set per Render instance, e.g. WORKER_ID=1)
_worker_id_raw = os.getenv("WORKER_ID", "").strip()
WORKER_ID: int | None = int(_worker_id_raw) if _worker_id_raw.isdigit() else None

# ── Optional Proxy Pool Rotation (e.g. Webshare 10 free proxies or custom) ───
_RAW_PROXIES = [p.strip() for p in os.getenv("PROXY_POOL", "").split(",") if p.strip()]
_proxy_index = 0
_proxy_lock = threading.Lock()


def get_next_proxy() -> str | None:
    """Return next proxy in round-robin fashion, or None if no pool configured."""
    global _proxy_index
    if not _RAW_PROXIES:
        return None
    with _proxy_lock:
        proxy = _RAW_PROXIES[_proxy_index % len(_RAW_PROXIES)]
        _proxy_index += 1
        return proxy


# ── Render Health Check HTTP Server ───────────────────────────────────────────
class ScoutHealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if WORKER_ID is not None:
            try:
                database.record_worker_heartbeat(WORKER_ID)
            except Exception:
                pass
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        w_tag = f"worker-{WORKER_ID}" if WORKER_ID is not None else "scout"
        response = json.dumps({"status": "ok", "worker": w_tag, "service": "instabot-child"}).encode("utf-8")
        self.wfile.write(response)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()

    def log_message(self, format, *args):
        # Silence access logs to keep terminal/Render stdout clean
        pass


def start_health_server(port: int = PORT) -> None:
    """Run lightweight HTTP listener so Render Web Service port check passes."""
    try:
        server = HTTPServer(("0.0.0.0", port), ScoutHealthHandler)
        logger.info("Render health server bound to 0.0.0.0:%d", port)
        server.serve_forever()
    except Exception as exc:
        logger.warning("Could not bind health server on port %d: %s", port, exc)


# ── Creator Profile Scraper ────────────────────────────────────────────────────
def fetch_creator_reel_shortcodes(username: str) -> list[str]:
    """
    Fetch public creator profile HTML using Chrome 124 TLS impersonation.
    Extracts reel and post shortcodes anonymously without account login or cookies.
    """
    clean_user = username.strip().lstrip("@").lower()
    url = f"https://www.instagram.com/{clean_user}/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
    }

    proxy = get_next_proxy()
    proxies_dict = {"http": proxy, "https": proxy} if proxy else None
    if proxy:
        logger.info("Routing scrape @%s through proxy: %s", clean_user, proxy.split("@")[-1] if "@" in proxy else proxy)

    try:
        r = requests.get(url, impersonate="chrome124", headers=headers, proxies=proxies_dict, timeout=20)
        if r.status_code != 200:
            logger.warning("Scrape @%s returned HTTP %s (profile might be private or rate limited)", clean_user, r.status_code)
            return []

        # Find all reel/post shortcodes (11 alphanumeric characters)
        found = re.findall(r'/(?:p|reel)/([A-Za-z0-9_-]{11})/', r.text)

        # Deduplicate while preserving profile order (newest first)
        seen = set()
        unique_shortcodes = []
        for sc in found:
            if sc not in seen:
                seen.add(sc)
                unique_shortcodes.append(sc)

        logger.info("Found %d recent reels on @%s profile", len(unique_shortcodes), clean_user)
        return unique_shortcodes
    except Exception as exc:
        logger.error("Scrape error for @%s: %s", clean_user, exc)
        return []


class SilentLogger:
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass


# ── Reel Evaluator ─────────────────────────────────────────────────────────────
def evaluate_reel(shortcode: str, creator: str) -> tuple[dict | None, int]:
    """
    Query live metadata anonymously without downloading the file.
    Evaluates:
      1. Has this reel already been processed?
      2. Upload age: Is it within target_max_days?
      3. Likes: Does it have at least target_min_likes?
    Returns (qualified_reel_dict or None, likes_count).
    """
    if database.is_reel_seen(shortcode):
        return None, 0

    url = f"https://www.instagram.com/reel/{shortcode}/"
    proxy = get_next_proxy()
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "socket_timeout": 15,
        "logger": SilentLogger(),
    }
    if proxy:
        ydl_opts["proxy"] = proxy

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        if not info:
            return None, 0

        timestamp = info.get("timestamp")
        upload_date_str = info.get("upload_date") or ""
        likes = info.get("like_count") or 0
        title = info.get("title") or info.get("description") or ""

        if not timestamp:
            if len(upload_date_str) == 8:
                try:
                    dt = datetime.strptime(upload_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
                    timestamp = int(dt.timestamp())
                except Exception:
                    pass

        if not timestamp:
            logger.debug("Reel %s: No timestamp found, skipping", shortcode)
            return None, likes

        upload_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        now_dt = datetime.now(timezone.utc)
        age_days = (now_dt - upload_dt).total_seconds() / 86400.0

        # Retrieve target thresholds for this creator (configured by users or admin)
        targets = database.get_creator_target_thresholds(creator)
        target_min_likes = targets.get("min_likes", MIN_LIKES)
        target_max_days = targets.get("max_days", MAX_AGE_DAYS)

        logger.info(
            "Inspecting %s (@%s): %s likes, %.2f days old (targets: >=%d likes, <=%.1fd, uploaded %s)",
            shortcode, creator, f"{likes:,}", age_days, target_min_likes, target_max_days, upload_date_str
        )

        # Check age constraint: 0 to target_max_days
        if age_days > target_max_days:
            logger.info("Reel %s is %.1f days old (> %.1f limit for @%s). Marking seen.", shortcode, age_days, target_max_days, creator)
            database.record_seen_reel(shortcode, creator, likes, upload_date_str, status="passed_too_old")
            return None, likes

        # Check likes constraint: >= target_min_likes
        if likes < target_min_likes:
            logger.info("Reel %s likes (%d) < target minimum (%d) for @%s.", shortcode, likes, target_min_likes, creator)
            # If reel is already beyond target age, mark permanently passed
            if age_days >= min(3.0, target_max_days):
                database.record_seen_reel(shortcode, creator, likes, upload_date_str, status="passed_below_likes")
            return None, likes

        # Qualified viral candidate!
        return {
            "shortcode": shortcode,
            "creator": creator,
            "likes": likes,
            "age_days": age_days,
            "upload_date": upload_date_str,
            "title": title.strip(),
            "url": url,
        }, likes
    except Exception as exc:
        logger.debug("Error evaluating reel %s: %s", shortcode, exc)
        database.record_seen_reel(shortcode, creator, 0, "", status="error_or_not_video")
        return None, 0


def inspect_single_reel_meta(shortcode: str) -> dict:
    """Extract metadata (likes, upload_date, age_days, title, url) without mutating database."""
    url = f"https://www.instagram.com/reel/{shortcode}/"
    proxy = get_next_proxy()
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "socket_timeout": 15,
        "logger": SilentLogger(),
    }
    if proxy:
        ydl_opts["proxy"] = proxy

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        if not info:
            return {"error": "No media info returned"}

        timestamp = info.get("timestamp")
        upload_date_str = info.get("upload_date") or ""
        likes = info.get("like_count") or 0
        title = info.get("title") or info.get("description") or ""

        if not timestamp and len(upload_date_str) == 8:
            try:
                dt = datetime.strptime(upload_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
                timestamp = int(dt.timestamp())
            except Exception:
                pass

        age_days = 0.0
        if timestamp:
            upload_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            now_dt = datetime.now(timezone.utc)
            age_days = (now_dt - upload_dt).total_seconds() / 86400.0

        return {
            "shortcode": shortcode,
            "likes": int(likes),
            "age_days": round(age_days, 2),
            "upload_date": upload_date_str,
            "title": title.strip()[:100],
            "url": url,
        }
    except Exception as exc:
        return {"error": str(exc)}


# ── Downloader & Telegram Dispatcher ───────────────────────────────────────────
async def process_viral_reel(reel_data: dict, bot_token: str, chat_id: int | str) -> bool:
    """
    Downloads qualified viral reel with anti-detection protection,
    uploads to Telegram to get permanent file_id, and records in database.
    """
    shortcode = reel_data["shortcode"]
    creator = reel_data["creator"]
    likes = reel_data["likes"]
    age_days = reel_data["age_days"]
    upload_date = reel_data["upload_date"]
    title = reel_data.get("title", "")
    url = reel_data["url"]

    logger.info("🔥 QUALIFIED VIRAL REEL: %s by @%s (%s likes, %.1f days old)!", shortcode, creator, f"{likes:,}", age_days)

    # 1. Download anonymously with anti-detection protection
    dl_res = downloader.download_instagram(url)
    if not dl_res.get("success") or not dl_res.get("video_path"):
        logger.error("Download failed for viral reel %s: %s", shortcode, dl_res.get("raw_error"))
        return False

    video_path = dl_res["video_path"]
    logger.info("Downloaded & protected video: %s (%.1f MB)", video_path.name, video_path.stat().st_size / (1024 * 1024))

    # 2. Build Telegram notification caption
    caption = (
        f"🎯 <b>Viral Reel Detected!</b>\n\n"
        f"👤 <b>Creator:</b> @{creator}\n"
        f"❤️ <b>Likes:</b> {likes:,}\n"
        f"📅 <b>Age:</b> {age_days:.1f} days old ({upload_date})\n"
        f"🔗 <a href='{url}'>Original Instagram Reel</a>\n"
    )
    if title:
        clean_snippet = html.escape(title[:180])
        if len(title) > 180:
            clean_snippet += "…"
        caption += f"\n📝 <i>{clean_snippet}</i>\n"

    caption += "\n🛡️ <i>Cleaned & anti-detection protected. Permanent file_id saved in DB.</i>"

    file_id = None
    try:
        t_request = HTTPXRequest(
            connection_pool_size=8,
            connect_timeout=30.0,
            read_timeout=60.0,
            write_timeout=180.0,
            media_write_timeout=180.0,
        )
        bot = telegram.Bot(token=bot_token, request=t_request)
        async with bot:
            with open(video_path, "rb") as vf:
                dest_chat = int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id
                logger.info("Uploading video to Telegram chat %s...", dest_chat)
                msg = await bot.send_video(
                    chat_id=dest_chat,
                    video=vf,
                    caption=caption,
                    parse_mode=telegram.constants.ParseMode.HTML,
                    supports_streaming=True,
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=30,
                )
                if msg.video:
                    file_id = msg.video.file_id
    except Exception as exc:
        logger.error("Failed to upload viral reel to Telegram: %s", exc)
        return False
    finally:
        # Immediate cleanup to ensure 0 MB disk footprint
        downloader.cleanup_session(video_path)

    if not file_id:
        logger.error("Upload succeeded but no video file_id was returned")
        return False

    logger.info("🎉 Video uploaded successfully! Telegram file_id: %s", file_id[:25] + "...")

    # 3. Auto-dispatch to all user subscribers of this creator whose personal targets match
    subscribers = database.get_subscribers_for_creator_filtered(creator, likes=likes, age_days=age_days)
    admin_id_int = int(chat_id) if str(chat_id).lstrip("-").isdigit() else 0
    for sub_id in subscribers:
        if sub_id != admin_id_int:
            try:
                user_cap = (
                    f"🎯 <b>Viral Reel from @{creator}!</b>\n\n"
                    f"❤️ <b>Likes:</b> {likes:,}\n"
                    f"📅 <b>Age:</b> {age_days:.1f} days old ({upload_date})\n"
                    f"🔗 <a href='{url}'>Original Instagram Reel</a>\n\n"
                    f"✨ <i>Auto-delivered from your Watchlist!</i>"
                )
                async with telegram.Bot(token=bot_token, request=t_request) as sub_bot:
                    await sub_bot.send_video(
                        chat_id=sub_id,
                        video=file_id,
                        caption=user_cap,
                        parse_mode=telegram.constants.ParseMode.HTML,
                        supports_streaming=True,
                    )
                logger.info("Auto-dispatched reel %s to subscriber %s", shortcode, sub_id)
            except Exception as e_sub:
                logger.warning("Could not dispatch to subscriber %s: %s", sub_id, e_sub)

    # 4. Store in database: media_cache, scout_queue, and scout_seen_reels
    database.set_cached_media(shortcode=shortcode, video_file_id=file_id, caption=caption)
    database.enqueue_viral_reel(shortcode=shortcode, creator=creator, likes=likes, video_file_id=file_id, caption=caption)
    database.record_seen_reel(shortcode=shortcode, creator=creator, likes=likes, posted_date=upload_date, status="viral_enqueued")
    logger.info("Saved %s to database scout_queue and media_cache.", shortcode)
    return True


# ── Scout Engine Cycle ─────────────────────────────────────────────────────────
async def run_scout_cycle(enforce_gap: bool = True) -> int:
    """
    Executes a complete inspection across all active creators assigned to this worker.
    Enforces a minimum 5-minute gap (CREATOR_GAP_SECONDS) between creators for safe, rate-limit-proof pacing.
    Returns total count of new viral reels processed.
    """
    w_label = f"Worker #{WORKER_ID}" if WORKER_ID is not None else "Standalone Worker"
    creators = database.get_active_watchlist_for_worker(WORKER_ID)

    # If watchlist is empty and running standalone, check environment variable WATCHLIST_CREATORS
    if not creators and WORKER_ID is None:
        env_creators = os.getenv("WATCHLIST_CREATORS", "").strip()
        if env_creators:
            for c in [x.strip() for x in env_creators.split(",") if x.strip()]:
                database.add_watchlist_creator(c)
            creators = database.get_active_watchlist_for_worker(WORKER_ID)

    if not creators:
        if WORKER_ID is not None:
            logger.info("Watchlist for %s is empty. Waiting for cluster assignments...", w_label)
        else:
            logger.warning("Watchlist is empty. Add creators with `python child/scout.py --add <username>`")
        return 0

    logger.info("=== Starting Scout Cycle for %s (%d creators: %s) ===", w_label, len(creators), ", ".join(f"@{c}" for c in creators))
    viral_found_count = 0

    for idx, creator in enumerate(creators, 1):
        logger.info("--- Inspecting creator (%d/%d): @%s ---", idx, len(creators), creator)
        shortcodes = fetch_creator_reel_shortcodes(creator)
        reels_inspected = len(shortcodes)
        max_likes_seen = 0
        new_viral_this_creator = 0

        for sc in shortcodes:
            # Immediate zero-cost check: skip if reel already processed
            if database.is_reel_seen(sc):
                continue

            # Check criteria
            reel_data, likes = evaluate_reel(sc, creator)
            if likes > max_likes_seen:
                max_likes_seen = likes

            if reel_data:
                # Qualified! Download, protect, upload, enqueue
                success = await process_viral_reel(reel_data, BOT_TOKEN, TARGET_CHAT_ID)
                if success:
                    viral_found_count += 1
                    new_viral_this_creator += 1

            # Polite jitter between live reel queries to stay undetected
            await asyncio.sleep(random.uniform(1.5, 3.5))

        status_note = f"Active ({reels_inspected} reels checked"
        if max_likes_seen > 0:
            status_note += f", top: {max_likes_seen:,} likes"
        if new_viral_this_creator > 0:
            status_note += f", {new_viral_this_creator} queued"
        status_note += ")"

        database.record_creator_scout_activity(
            creator=creator,
            reels_count=reels_inspected,
            max_likes=max_likes_seen,
            status_note=status_note,
            worker_id=WORKER_ID,
        )
        if WORKER_ID is not None:
            database.record_worker_heartbeat(WORKER_ID)

        # 5-minute minimum pacing gap between different creators
        if enforce_gap and idx < len(creators):
            logger.info("⏳ Pacing: Waiting %d seconds (5 min) before inspecting next creator...", CREATOR_GAP_SECONDS)
            await asyncio.sleep(CREATOR_GAP_SECONDS)

    logger.info("=== Scout Cycle Complete: %d new viral reels processed ===", viral_found_count)
    return viral_found_count


# ── Main Entrypoint & Daemon Loop ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="InstaBot Child Scout Worker")
    parser.add_argument("--once", action="store_true", help="Run a single scout cycle and exit")
    parser.add_argument("--add", type=str, help="Add a creator username to the watchlist")
    parser.add_argument("--remove", type=str, help="Remove a creator username from the watchlist")
    parser.add_argument("--list", action="store_true", help="List active creators in watchlist")
    args = parser.parse_args()

    database.init_db()

    # Watchlist management CLI
    if args.add:
        success = database.add_watchlist_creator(args.add)
        print(f"Added @{args.add.lstrip('@')} to watchlist: {success}")
        return

    if args.remove:
        success = database.remove_watchlist_creator(args.remove)
        print(f"Removed @{args.remove.lstrip('@')} from watchlist: {success}")
        return

    if args.list:
        creators = database.get_active_watchlist()
        print(f"Active watchlist ({len(creators)}): {', '.join('@' + c for c in creators) if creators else 'Empty'}")
        return

    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing! Please set BOT_TOKEN in .env")
        return

    if not TARGET_CHAT_ID:
        logger.error("ADMIN_ID or SCOUT_TARGET_CHAT is missing in .env")
        return

    if args.once:
        logger.info("Running single scout operation (--once)...")
        found = asyncio.run(run_scout_cycle(enforce_gap=False))
        logger.info("One-shot run finished. Found %d viral reels.", found)
        sys.exit(0)

    # Start health server in background thread for Render Web Service deployment
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    # Daemon loop for Render or background worker
    logger.info("Starting Scout Worker Daemon (interval: %d min, min_likes: %d, max_age: %.1f days)...",
                SCOUT_INTERVAL_MINUTES, MIN_LIKES, MAX_AGE_DAYS)

    while True:
        try:
            asyncio.run(run_scout_cycle())
        except Exception as exc:
            logger.error("Scout cycle encountered an unexpected error: %s", exc)

        sleep_seconds = SCOUT_INTERVAL_MINUTES * 60
        logger.info("Sleeping for %d minutes until next inspection...", SCOUT_INTERVAL_MINUTES)
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
