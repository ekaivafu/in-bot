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


def _build_ydl_opts(output_dir: Path, filename_stem: str) -> dict:
    """Return yt-dlp options for best quality video+audio merge."""
    opts = {
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "outtmpl": str(output_dir / f"{filename_stem}.%(ext)s"),
        "writeinfojson": True,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
    }

    cookies = find_cookies_file()
    if cookies:
        logger.info("Using cookies file: %s", cookies)
        opts["cookiefile"] = cookies
    else:
        logger.warning("No cookies.txt found — Instagram may block the request!")

    return opts



def _strip_metadata(input_path: Path, output_path: Path) -> bool:
    """
    Re-encode with ffmpeg to strip ALL metadata (Exif, comment, encoder tags,
    creation time, etc.) so Instagram cannot detect the original source.

    Returns True on success, False if ffmpeg is not available.
    """
    if not shutil.which("ffmpeg"):
        logger.warning("ffmpeg not found - skipping metadata strip.")
        return False

    cmd = [
        "ffmpeg",
        "-y",                       # overwrite without asking
        "-i", str(input_path),
        # Strip ALL metadata from container
        "-map_metadata", "-1",
        # Stream copy = no re-encode → near-instant, no quality loss
        # Metadata is still fully wiped by -map_metadata -1
        "-c", "copy",
        # Optimise for streaming (moves moov atom to front)
        "-movflags", "+faststart",
        str(output_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            logger.error("ffmpeg error: %s", result.stderr[-500:])
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg timed out")
        return False
    except Exception as exc:
        logger.error("ffmpeg exception: %s", exc)
        return False


def download_instagram(url: str) -> dict:
    """
    Download an Instagram post/reel/TV video.

    Returns a dict:
        {
            "success": bool,
            "video_path": Path | None,   # path to final (metadata-stripped) video
            "caption": str,              # post caption (empty string if none)
            "error": str | None,
        }
    """
    session_id = uuid.uuid4().hex[:8]
    work_dir = DOWNLOAD_DIR / session_id
    work_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "success":    False,
        "video_path": None,
        "caption":    "",
        "error":      None,   # friendly message for user
        "error_type": None,   # 'cookie' | 'private' | 'generic'
        "raw_error":  None,   # full error string for admin log
    }

    try:
        ydl_opts = _build_ydl_opts(work_dir, "media")

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        # ── Extract caption ───────────────────────────────────────────────
        caption = info.get("description") or info.get("title") or ""
        result["caption"] = caption.strip()

        # ── Find downloaded video file ────────────────────────────────────
        video_files = list(work_dir.glob("media.mp4"))
        if not video_files:
            for ext in ("mp4", "mkv", "webm", "mov"):
                video_files = list(work_dir.glob(f"*.{ext}"))
                if video_files:
                    break

        if video_files:
            raw_video = video_files[0]
            # ── Strip metadata ────────────────────────────────────────────────
            clean_video = work_dir / "clean_media.mp4"
            stripped    = _strip_metadata(raw_video, clean_video)

            final_video          = clean_video if stripped else raw_video
            result["video_path"] = final_video
            result["is_video"]   = True
            result["success"]    = True
        else:
            # Check for downloaded images/photos (posts, carousels)
            image_files = []
            for ext in ("jpg", "jpeg", "png", "webp"):
                image_files.extend(list(work_dir.glob(f"*.{ext}")))
            image_files = [p for p in image_files if not p.name.endswith(".info.json")]

            if image_files:
                result["image_paths"] = sorted(image_files)
                result["is_video"]    = False
                result["success"]     = True
            else:
                result["error"]      = "Could not find downloaded video or image."
                result["error_type"] = "generic"
                result["raw_error"]  = "No media files found after download"
                return result

    except yt_dlp.utils.DownloadError as exc:
        err_msg              = str(exc)
        err_lower            = err_msg.lower()
        result["raw_error"]  = err_msg

        if any(w in err_lower for w in ("not found", "unavailable", "does not exist", "removed", "400: bad request", "bad request")):
            result["error_type"] = "not_found"
            result["error"]      = "This post or reel was deleted or is no longer available on Instagram."
        elif "private" in err_lower and "rate" not in err_lower:
            result["error_type"] = "private"
            result["error"]      = "This account or post is *private*."
        elif any(k in err_lower for k in ("login_required", "logged out", "checkpoint", "use --cookies", "empty media response")):
            result["error_type"] = "cookie"
            result["error"]      = "Instagram session expired. Admin notified."
        else:
            result["error_type"] = "generic"
            result["error"]      = "Download failed. Please try again later."

        logger.error("yt-dlp DownloadError: %s", exc)

    except Exception as exc:
        result["error_type"] = "generic"
        result["error"]      = "An unexpected error occurred."
        result["raw_error"]  = str(exc)
        logger.exception("Unexpected download error")

    return result


def cleanup_session(video_path: Path) -> None:
    """Delete the session work directory after the file has been sent."""
    if video_path and video_path.parent.exists():
        shutil.rmtree(video_path.parent, ignore_errors=True)
