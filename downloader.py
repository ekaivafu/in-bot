"""
downloader.py
─────────────
Handles Instagram media downloading via yt-dlp and
metadata stripping via ffmpeg.
"""

import os
import re
import json
import uuid
import shutil
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yt_dlp

logger = logging.getLogger(__name__)

DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ── Cookie file detection (runtime, not import-time) ──────────────────────────
_COOKIES_SEARCH_PATHS = [
    lambda: os.getenv("COOKIES_FILE", ""),   # env override (loaded after dotenv)
    lambda: "/etc/secrets/cookies.txt",       # Render Secret File
    lambda: "cookies.txt",                    # project root fallback
]


def convert_json_cookies_to_netscape(json_path: Path, output_path: Path) -> bool:
    """Convert browser-exported JSON cookies to Netscape cookies.txt format."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        if not isinstance(cookies, list):
            return False

        lines = [
            "# Netscape HTTP Cookie File",
            "# Generated automatically from " + json_path.name,
            "",
        ]
        for c in cookies:
            if not isinstance(c, dict):
                continue
            domain = c.get("domain", "")
            include_subdomains = "TRUE" if domain.startswith(".") else "FALSE"
            path = c.get("path", "/")
            secure = "TRUE" if c.get("secure", False) else "FALSE"
            expiry = c.get("expirationDate", 0)
            try:
                expiry_int = int(float(expiry)) if expiry else 0
            except (ValueError, TypeError):
                expiry_int = 0
            name = c.get("name", "")
            value = c.get("value", "")
            lines.append(f"{domain}\t{include_subdomains}\t{path}\t{secure}\t{expiry_int}\t{name}\t{value}")

        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("Auto-converted %s -> %s (%d cookies)", json_path, output_path, len(cookies))
        return True
    except Exception as exc:
        logger.warning("Failed to auto-convert %s: %s", json_path, exc)
        return False


def find_cookies_file() -> str | None:
    """Find a valid cookies.txt at runtime. Prioritizes the most recently updated file."""
    # 0. Explicit env override
    env_path = os.getenv("COOKIES_FILE", "").strip()
    if env_path and Path(env_path).is_file() and Path(env_path).stat().st_size > 0:
        return env_path

    # 1. Compare local cookies.txt and Render secret file by newest mtime
    candidates = [Path("cookies.txt"), Path("/etc/secrets/cookies.txt")]
    valid_candidates = [p for p in candidates if p.is_file() and p.stat().st_size > 0]
    if valid_candidates:
        valid_candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return str(valid_candidates[0])

    # 2. Check if database-stored cookies exist in Neon DB (cloud persist)
    try:
        from database import get_setting
        saved_cookies = get_setting("active_cookies", "")
        if saved_cookies and saved_cookies.strip():
            target_txt = Path("cookies.txt")
            target_txt.write_text(saved_cookies, encoding="utf-8")
            return str(target_txt)
    except Exception:
        pass

    # 3. Check if a JSON cookie file exists and convert it
    for json_file in Path(".").glob("*instagram*.json"):
        target_txt = Path("cookies.txt")
        if convert_json_cookies_to_netscape(json_file, target_txt):
            return str(target_txt)

    # 3. Check any other .json files in root that might be cookies
    for json_file in Path(".").glob("*.json"):
        if json_file.name.startswith("package") or json_file.name.startswith("tsconfig"):
            continue
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict) and "domain" in data[0]:
                target_txt = Path("cookies.txt")
                if convert_json_cookies_to_netscape(json_file, target_txt):
                    return str(target_txt)
        except Exception:
            pass

    return None


# ── Regex to loosely validate Instagram URLs ──────────────────────────────────
INSTAGRAM_URL_RE = re.compile(
    r"(https?://)?(www\.)?instagram\.com"
    r"/(p|reel|tv|stories)/[\w\-]+",
    re.IGNORECASE,
)


def is_instagram_url(url: str) -> bool:
    return bool(INSTAGRAM_URL_RE.search(url))


def _build_ydl_opts(output_dir: Path, filename_stem: str, use_cookies: bool = True) -> dict:
    """
    Return yt-dlp options tuned for Render 512MB free tier:
    - 1 concurrent fragment to prevent RAM exhaustion
    - 32KB buffer size to avoid memory bloat
    - 50MB max file size limit (matching Telegram API maximum)
    - 25s socket timeout and 2 retries
    - No JSON dump to save disk I/O
    """
    opts = {
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "outtmpl": str(output_dir / f"{filename_stem}.%(ext)s"),
        "writeinfojson": False,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "concurrent_fragment_downloads": 1,
        "buffersize": 1024 * 32,
        "max_filesize": 50 * 1024 * 1024,
        "socket_timeout": 25,
        "retries": 2,
    }

    if use_cookies:
        cookies = find_cookies_file()
        if cookies:
            logger.info("Using cookies file: %s", cookies)
            opts["cookiefile"] = cookies
        else:
            logger.info("No cookies.txt found — operating in anonymous public mode.")

    return opts



def _strip_and_protect_video(input_path: Path, output_path: Path) -> bool:
    """
    Advanced Anti-Detection & Metadata Injection (Render 512MB RAM safe):
    1. Wipes ALL original tracking metadata (-map_metadata -1).
    2. Applies an imperceptible pixel/color micro-adjustment filter:
       eq=contrast=1.004:brightness=0.001:saturation=1.002
       This alters DCT coefficients and generates 100% brand new cryptographic & perceptual
       hashes, preventing duplicate detection / copyright matching when reposted.
    3. Re-encodes audio with high-quality AAC (128kbps) with fresh waveforms.
    4. Limits ffmpeg to 2 worker threads (-threads 2) to eliminate cloud RAM spikes.
    5. Injects brand new, realistic modern device/camera metadata tags with current UTC timestamp.
    6. Optimizes container with -movflags +faststart.
    7. Graceful automatic fallback: if re-encoding encounters any issue or takes too long,
       falls back to fast stream copy with fresh metadata tags so downloads never fail.
    """
    if not shutil.which("ffmpeg"):
        logger.warning("ffmpeg not found - skipping video protection.")
        return False

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # If file is unusually large (> 35MB), prefer fast stream copy to avoid Render memory timeouts
    file_size_mb = input_path.stat().st_size / (1024 * 1024) if input_path.exists() else 0
    if file_size_mb > 35:
        logger.info("Large video (%.1f MB) — using fast metadata strip to conserve Render RAM", file_size_mb)
        cmd_direct_copy = [
            "ffmpeg", "-y",
            "-threads", "2",
            "-i", str(input_path),
            "-map_metadata", "-1",
            "-c", "copy",
            "-metadata", f"creation_time={now_iso}",
            "-metadata", "encoder=Core Media Engine v2.1",
            "-metadata:s:v:0", f"creation_time={now_iso}",
            "-metadata:s:a:0", f"creation_time={now_iso}",
            "-movflags", "+faststart",
            str(output_path),
        ]
        try:
            res_dc = subprocess.run(cmd_direct_copy, capture_output=True, text=True, timeout=30)
            if res_dc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1000:
                return True
        except Exception as exc:
            logger.warning("Fast direct copy error: %s", exc)

    # Tier 1: Advanced micro-adjustment + fresh metadata injection (strict -threads 2)
    cmd_advanced = [
        "ffmpeg", "-y",
        "-threads", "2",
        "-i", str(input_path),
        "-map_metadata", "-1",  # wipe all original container metadata
        "-vf", "eq=contrast=1.004:brightness=0.001:saturation=1.002",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "22",
        "-c:a", "aac", "-b:a", "128k",
        "-metadata", f"creation_time={now_iso}",
        "-metadata", "encoder=Core Media Engine v2.1",
        "-metadata:s:v:0", f"creation_time={now_iso}",
        "-metadata:s:v:0", "handler_name=Core Media Video",
        "-metadata:s:a:0", f"creation_time={now_iso}",
        "-metadata:s:a:0", "handler_name=Core Media Audio",
        "-movflags", "+faststart",
        str(output_path),
    ]

    try:
        result = subprocess.run(cmd_advanced, capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1000:
            logger.info("Advanced video protection & metadata injection successful: %s", output_path.name)
            return True
        logger.warning("Advanced video protection failed (code %d), trying fast fallback...", result.returncode)
    except Exception as exc:
        logger.warning("Advanced video protection exception (%s), trying fast fallback...", exc)

    # Tier 2 Fallback: Fast stream copy with fresh metadata injection
    cmd_fallback = [
        "ffmpeg", "-y",
        "-threads", "2",
        "-i", str(input_path),
        "-map_metadata", "-1",
        "-c", "copy",
        "-metadata", f"creation_time={now_iso}",
        "-metadata", "encoder=Core Media Engine v2.1",
        "-metadata:s:v:0", f"creation_time={now_iso}",
        "-metadata:s:a:0", f"creation_time={now_iso}",
        "-movflags", "+faststart",
        str(output_path),
    ]

    try:
        res_fb = subprocess.run(cmd_fallback, capture_output=True, text=True, timeout=30)
        if res_fb.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1000:
            logger.info("Fast metadata strip & injection fallback successful: %s", output_path.name)
            return True
        logger.error("Fast metadata strip fallback failed: %s", res_fb.stderr[-300:] if res_fb.stderr else "unknown")
        return False
    except Exception as exc:
        logger.error("Fast metadata strip fallback error: %s", exc)
        return False


def _extract_media_result(work_dir: Path, info: dict, result: dict) -> bool:
    """Find and protect downloaded video or photos in work_dir."""
    caption = info.get("description") or info.get("title") or ""
    result["caption"] = caption.strip()

    video_files = list(work_dir.glob("*.mp4"))
    if not video_files:
        for ext in ("mkv", "webm", "mov"):
            video_files = list(work_dir.glob(f"*.{ext}"))
            if video_files:
                break

    if video_files:
        raw_video = sorted(video_files, key=lambda f: f.stat().st_size, reverse=True)[0]
        clean_video = work_dir / "clean_media.mp4"
        stripped = _strip_and_protect_video(raw_video, clean_video)

        final_video = clean_video if (stripped and clean_video.exists() and clean_video.stat().st_size > 1000) else raw_video
        result["video_path"] = final_video
        result["is_video"] = True
        result["success"] = True
        return True
    else:
        # Check for downloaded images/photos (posts, carousels)
        image_files = []
        for ext in ("jpg", "jpeg", "png", "webp"):
            image_files.extend(list(work_dir.glob(f"*.{ext}")))
        image_files = [p for p in image_files if not p.name.endswith(".info.json")]

        if image_files:
            result["image_paths"] = sorted(image_files)
            result["is_video"] = False
            result["success"] = True
            return True

    return False


def download_instagram(url: str) -> dict:
    """
    Download an Instagram post/reel/TV video with dual-tier resilience:
    1. Primary tier: Attempt with cookies if available (or anonymous if no cookies exist).
    2. Fallback tier: If cookie session errors occur (400, login required, expired),
       automatically retry in anonymous cookie-free mode so public reels never fail!
    3. Memory safe: strictly bounded for Render 512MB RAM.
    """
    session_id = uuid.uuid4().hex[:8]
    work_dir = DOWNLOAD_DIR / session_id
    work_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "success":    False,
        "video_path": None,
        "caption":    "",
        "error":      None,   # friendly message for user
        "error_type": None,   # 'cookie' | 'private' | 'not_found' | 'generic'
        "raw_error":  None,   # full error string for admin log
    }

    cookies_available = bool(find_cookies_file())

    # ── Attempt 1: Download with active cookies (or anonymous if no cookies) ──
    try:
        ydl_opts = _build_ydl_opts(work_dir, "media", use_cookies=cookies_available)
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        if _extract_media_result(work_dir, info, result):
            return result

    except yt_dlp.utils.DownloadError as exc_1:
        err_msg = str(exc_1)
        err_lower = err_msg.lower()
        result["raw_error"] = err_msg

        # If cookies failed with session or bad request errors, retry in cookie-free mode!
        is_cookie_or_session_err = any(
            k in err_lower for k in ("login_required", "logged out", "checkpoint", "use --cookies", "empty media response", "400: bad request", "bad request")
        )

        if cookies_available and is_cookie_or_session_err:
            logger.info("Primary cookie download encountered error (%s). Retrying in cookie-free public mode...", err_msg)
            try:
                # Clean any partial artifacts from first attempt
                for tmp_f in work_dir.glob("media*"):
                    try:
                        tmp_f.unlink(missing_ok=True)
                    except Exception:
                        pass

                ydl_opts_anon = _build_ydl_opts(work_dir, "media_anon", use_cookies=False)
                with yt_dlp.YoutubeDL(ydl_opts_anon) as ydl_anon:
                    info_anon = ydl_anon.extract_info(url, download=True)

                if _extract_media_result(work_dir, info_anon, result):
                    logger.info("Cookie-free public download fallback succeeded!")
                    return result
            except Exception as exc_anon:
                logger.warning("Cookie-free fallback also failed: %s", exc_anon)
                result["raw_error"] = f"{err_msg} | Fallback: {exc_anon}"

        # Classify the final error
        if any(w in err_lower for w in ("not found", "unavailable", "does not exist", "removed")):
            result["error_type"] = "not_found"
            result["error"] = "This post or reel was deleted or is no longer available on Instagram."
        elif "private" in err_lower and "rate" not in err_lower:
            result["error_type"] = "private"
            result["error"] = "This account or post is *private*."
        elif is_cookie_or_session_err:
            result["error_type"] = "cookie"
            result["error"] = "Instagram session expired. Admin notified."
        else:
            result["error_type"] = "generic"
            result["error"] = "Download failed. Please try again later."

        logger.error("Download failed | type=%s | error=%s", result["error_type"], err_msg)

    except Exception as exc:
        result["error_type"] = "generic"
        result["error"] = "An unexpected error occurred."
        result["raw_error"] = str(exc)
        logger.exception("Unexpected download error")

    return result


def cleanup_session(video_path: Path | str | None) -> None:
    """Delete session work directory and trigger garbage collection immediately."""
    try:
        if video_path:
            p = Path(video_path)
            work_dir = p.parent if p.is_file() else p
            if work_dir.exists() and work_dir.resolve() != DOWNLOAD_DIR.resolve():
                shutil.rmtree(work_dir, ignore_errors=True)
    except Exception as exc:
        logger.warning("cleanup_session error: %s", exc)
    finally:
        import gc
        gc.collect()


def extract_shortcode(url: str) -> str | None:
    """Extract Instagram shortcode from URL (reel/post/tv)."""
    if not url:
        return None
    match = re.search(r"instagram\.com/(?:reel|reels|p|tv)/([A-Za-z0-9_-]+)", url)
    return match.group(1) if match else None


def extract_audio_from_video(video_path: Path, output_audio_path: Path | None = None) -> Path | None:
    """Extract audio stream from video file as MP3 via ffmpeg (Render safe)."""
    if not video_path or not video_path.exists():
        return None
    if output_audio_path is None:
        output_audio_path = video_path.parent / f"{video_path.stem}.mp3"
    cmd = [
        "ffmpeg", "-y",
        "-threads", "2",
        "-i", str(video_path),
        "-vn",
        "-acodec", "libmp3lame",
        "-q:a", "2",
        str(output_audio_path),
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, timeout=60)
        if res.returncode == 0 and output_audio_path.exists() and output_audio_path.stat().st_size > 0:
            return output_audio_path
    except Exception as exc:
        logger.warning("extract_audio_from_video failed: %s", exc)
    return None


def check_cookies_health() -> tuple[bool, str]:
    """
    Validates Instagram session cookies:
    1. Checks if cookies.txt exists and is non-empty
    2. Inspects sessionid presence and expiry timestamp
    """
    cookie_file = find_cookies_file()
    if not cookie_file or not Path(cookie_file).is_file() or Path(cookie_file).stat().st_size == 0:
        return False, "No active cookies.txt found on server"

    try:
        content = Path(cookie_file).read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, f"Could not read cookies file: {exc}"

    if "sessionid" not in content:
        return False, "Cookies file does not contain an active Instagram 'sessionid'"

    import time
    now_ts = int(time.time())
    for line in content.splitlines():
        if "sessionid" in line:
            parts = line.strip().split("\t")
            if len(parts) >= 5:
                try:
                    exp = int(parts[4])
                    if 0 < exp < now_ts:
                        return False, f"Instagram sessionid expired on {time.ctime(exp)}"
                except ValueError:
                    pass

    return True, "Cookies are valid"

