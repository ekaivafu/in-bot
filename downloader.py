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
import random
import logging
import html
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

from curl_cffi import requests as cffi_requests
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


# ── Regex to validate Instagram & TikTok URLs ───────────────────────────────
INSTAGRAM_URL_RE = re.compile(
    r"(https?://)?([a-zA-Z0-9-]+\.)?instagram\.com/(?:p|reel|reels|tv|stories)/[\w\-]+",
    re.IGNORECASE,
)

TIKTOK_URL_RE = re.compile(
    r"(https?://)?([a-zA-Z0-9-]+\.)?tiktok\.com/(?:@[\w.-]+/(?:video|photo)/\d+|v/\d+|t/[\w-]+|[\w.-]+(?:/[\w.-]+)*)",
    re.IGNORECASE,
)


def is_instagram_url(url: str) -> bool:
    return bool(INSTAGRAM_URL_RE.search(url)) if url else False


def is_tiktok_url(url: str) -> bool:
    return bool(TIKTOK_URL_RE.search(url)) if url else False


def is_supported_url(url: str) -> bool:
    return is_instagram_url(url) or is_tiktok_url(url)


def _build_ydl_opts(output_dir: Path, filename_stem: str, use_cookies: bool = False) -> dict:
    """
    Return yt-dlp options tuned for Render 512MB free tier:
    - 1 concurrent fragment to prevent RAM exhaustion
    - 32KB buffer size to avoid memory bloat
    - 50MB max file size limit (matching Telegram API maximum)
    - 25s socket timeout and 2 retries
    - No JSON dump to save disk I/O
    - Anonymous cookie-free by default for hassle-free operation
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
            logger.info("Using cookies file for authenticated fallback: %s", cookies)
            opts["cookiefile"] = cookies
        else:
            logger.info("No cookies.txt found — operating in cookie-free mode.")
    else:
        logger.info("Downloading in primary cookie-free public mode (no account required).")

    return opts



# ── Anti-Detection Real Device Profiles ────────────────────────────────────────
DEVICE_PROFILES = [
    {
        "name": "Apple iPhone 15 Pro Max",
        "artist": "Apple iPhone 15 Pro Max",
        "title": "Camera Recording",
        "comment": "Apple iOS 17.5.1 / Camera 4K HDR",
        "encoder": "QuickTime / Apple iOS 17.5.1",
        "video_handler": "Core Media Video",
        "audio_handler": "Core Media Audio",
    },
    {
        "name": "Apple iPhone 14 Pro",
        "artist": "Apple iPhone 14 Pro",
        "title": "QuickTime Movie",
        "comment": "Apple iOS 17.4 / Cinematic Capture",
        "encoder": "QuickTime / Apple iOS 17.4",
        "video_handler": "Apple Video Media Handler",
        "audio_handler": "Apple Sound Media Handler",
    },
    {
        "name": "Samsung Galaxy S24 Ultra",
        "artist": "Samsung Galaxy S24 Ultra",
        "title": "Samsung Camera",
        "comment": "Samsung One UI 6.1 / Android 14",
        "encoder": "Samsung Android Video Engine v14",
        "video_handler": "VideoHandle",
        "audio_handler": "SoundHandle",
    },
    {
        "name": "Google Pixel 8 Pro",
        "artist": "Google Pixel 8 Pro",
        "title": "Pixel Cinematic Pan",
        "comment": "Google Camera 9.2 / Android 14",
        "encoder": "Google Camera Engine v9.2",
        "video_handler": "VideoHandler",
        "audio_handler": "SoundHandler",
    },
    {
        "name": "Adobe Premiere Pro 2024",
        "artist": "Adobe Systems Inc.",
        "title": "Exported Sequence",
        "comment": "Adobe Premiere Pro 2024.3 (Build 61)",
        "encoder": "Adobe Media Encoder 2024",
        "video_handler": "MainConcept Video Media Handler",
        "audio_handler": "MainConcept Audio Media Handler",
    },
    {
        "name": "DaVinci Resolve Studio 19",
        "artist": "Blackmagic Design",
        "title": "Resolve Master Delivery",
        "comment": "DaVinci Resolve Studio 19.0.1",
        "encoder": "DaVinci Resolve Studio 19.0",
        "video_handler": "Blackmagic Video Handler",
        "audio_handler": "Blackmagic Audio Handler",
    },
    {
        "name": "Apple Final Cut Pro",
        "artist": "Apple Inc.",
        "title": "FCP Project Render",
        "comment": "Apple Final Cut Pro 10.7.1",
        "encoder": "Apple Final Cut Pro 10.7.1",
        "video_handler": "QuickTime Video Handler",
        "audio_handler": "QuickTime Audio Handler",
    },
]

def _has_audio_stream(video_path: Path) -> bool:
    """Check if the video has an audio stream via ffprobe."""
    if not shutil.which("ffprobe"):
        return True
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_type",
            "-of", "csv=p=0",
            str(video_path),
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return "audio" in res.stdout.strip().lower()
    except Exception:
        return True


def _get_random_anti_detect_params(has_audio: bool = True) -> dict:
    """
    Elite Anti-Detection Parameter Matrix:
    1. Keyframe 0 Focus-Pull Intro: Replaces Frame 0 / Thumbnail with 7 blurred intro frames (~0.23s)
       to completely defeat spatial perceptual cover hashes and initial keyframe signatures.
    2. Coordinate Micro-Rotation: 0.012 rad (~0.69°) rotation to shift every pixel coordinate vector.
    3. Dimension Normalization: Crop & scale ensuring strictly even dimensions for H.264 yuv420p.
    4. Perceptual Masks: Dynamic radial vignette + enhanced saturation + temporal noise grain.
    5. Acoustic Evasion: 233ms sync delay + 1.2ms flanger + dual-band parametric EQ.
    6. GOP Structure: Fixed GOP 30 (-g 30 -bf 2) for fresh bitstream cadence.
    7. Container Profile: Randomized device metadata from real camera models.
    """
    profile = random.choice(DEVICE_PROFILES)

    # Random realistic timestamp offset (between 2 to 300 seconds ago)
    jitter_sec = random.randint(2, 300)
    creation_dt = datetime.now(timezone.utc) - timedelta(seconds=jitter_sec)
    iso_time = creation_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    rot_angle = round(random.choice([0.012, -0.012]), 4)
    vignette_angle = round(random.uniform(0.38, 0.44), 3)

    # Strong cinematic color grading: rich saturation + deep crushed blacks/shadows
    contrast = round(random.uniform(1.15, 1.22), 3)
    brightness = round(random.uniform(-0.045, -0.030), 4)
    saturation = round(random.uniform(1.30, 1.40), 3)
    gamma = round(random.uniform(0.90, 0.94), 3)

    noise_luma = random.choice([3, 4])
    noise_chroma = random.choice([2, 3])

    # 0.5 second (15 frames @ 30fps) visible freeze intro + strong color grading
    v_filter = (
        "[0:v]split=2[main][first];"
        "[first]trim=end_frame=1,boxblur=15:5,loop=loop=15:size=1:start=0,setpts=N/30/TB[intro];"
        "[intro][main]concat=n=2:v=1:a=0[v_cat];"
        f"[v_cat]rotate={rot_angle}:ow='rotw({rot_angle})':oh='roth({rot_angle})',"
        "crop=trunc((in_w-8)/2)*2:trunc((in_h-8)/2)*2,"
        "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p,"
        f"vignette=angle={vignette_angle},"
        f"eq=contrast={contrast}:brightness={brightness}:saturation={saturation}:gamma={gamma},"
        f"noise=c0s={noise_luma}:c0f=t+u:c1s={noise_chroma}:c1f=t,"
        "unsharp=3:3:0.40[v_out]"
    )

    low_freq = random.randint(115, 135)
    low_gain = round(random.uniform(1.8, 2.4), 2)
    high_freq = random.randint(3200, 3600)
    high_gain = round(random.uniform(-1.8, -2.4), 2)
    vol_scale = round(random.uniform(0.995, 1.008), 3)

    if has_audio:
        # 500ms delay perfectly aligns with 0.5s (15 frames) intro freeze
        a_filter = (
            "[0:a]adelay=500|500,"
            "flanger=delay=1.2:depth=0.4:speed=0.2,"
            f"equalizer=f={low_freq}:t=q:w=1:g={low_gain},"
            f"equalizer=f={high_freq}:t=q:w=1:g={high_gain},"
            f"volume={vol_scale}[a_out]"
        )
        filter_complex = f"{v_filter};{a_filter}"
    else:
        filter_complex = v_filter

    audio_bitrate = random.choice(["128k", "132k", "125k", "130k"])
    crf_val = str(random.choice([21, 22]))

    metadata_args = [
        "-metadata", f"creation_time={iso_time}",
        "-metadata", f"title={profile['title']}",
        "-metadata", f"artist={profile['artist']}",
        "-metadata", f"comment={profile['comment']}",
        "-metadata", f"encoder={profile['encoder']}",
        "-metadata:s:v:0", f"creation_time={iso_time}",
        "-metadata:s:v:0", f"handler_name={profile['video_handler']}",
    ]
    if has_audio:
        metadata_args.extend([
            "-metadata:s:a:0", f"creation_time={iso_time}",
            "-metadata:s:a:0", f"handler_name={profile['audio_handler']}",
        ])

    return {
        "profile": profile["name"],
        "iso_time": iso_time,
        "rot_angle": rot_angle,
        "has_audio": has_audio,
        "filter_complex": filter_complex,
        "audio_bitrate": audio_bitrate,
        "crf": crf_val,
        "metadata_args": metadata_args,
    }


def _strip_and_protect_video(input_path: Path, output_path: Path) -> bool:
    """
    Elite Anti-Detection & Unique Device Profile Injection (Render 512MB RAM safe):
    1. Wipes ALL original tracking metadata (-map_metadata -1).
    2. Overwrites Keyframe 0 with a 0.23s cinematic focus-pull intro.
    3. Coordinate micro-rotation + auto-crop + even dimensions.
    4. Dynamic vignette, temporal noise, and micro-color grading.
    5. Acoustic flanger, dual-band parametric EQ, and 233ms sync delay.
    6. Fixed GOP 30 cadence with 2 B-frames.
    7. Injects realistic randomized device profile headers.
    8. Strictly limits ffmpeg to 2 worker threads (-threads 2) to eliminate cloud RAM spikes.
    """
    if not shutil.which("ffmpeg"):
        logger.warning("ffmpeg not found - skipping video protection.")
        return False

    has_audio = _has_audio_stream(input_path)
    params = _get_random_anti_detect_params(has_audio=has_audio)
    logger.info(
        "Applying elite anti-detection profile '%s' (rot=%.4frad, audio=%s): %s",
        params["profile"],
        params["rot_angle"],
        has_audio,
        output_path.name,
    )

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
            *params["metadata_args"],
            "-movflags", "+faststart",
            str(output_path),
        ]
        try:
            res_dc = subprocess.run(cmd_direct_copy, capture_output=True, text=True, timeout=30)
            if res_dc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1000:
                return True
        except Exception as exc:
            logger.warning("Fast direct copy error: %s", exc)

    # Tier 1: Advanced micro-adjustment + fresh randomized metadata (strict -threads 2)
    cmd_advanced = [
        "ffmpeg", "-y",
        "-threads", "2",
        "-i", str(input_path),
        "-map_metadata", "-1",  # wipe all original container metadata
        "-filter_complex", params["filter_complex"],
        "-map", "[v_out]",
    ]
    if has_audio:
        cmd_advanced.extend([
            "-map", "[a_out]",
            "-c:a", "aac", "-b:a", params["audio_bitrate"],
        ])
    cmd_advanced.extend([
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", params["crf"],
        "-g", "30", "-bf", "2",
        "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709",
        *params["metadata_args"],
        "-movflags", "+faststart",
        str(output_path),
    ])

    try:
        result = subprocess.run(cmd_advanced, capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1000:
            out_mb = output_path.stat().st_size / (1024 * 1024)
            if out_mb <= 48:
                logger.info("Elite video protection (%s) successful: %s (%.1f MB)", params["profile"], output_path.name, out_mb)
                return True
            logger.warning("Protected video is too large for Telegram (%.1f MB), falling back to fast copy", out_mb)
        else:
            logger.warning("Advanced video protection failed (code %d), trying fast fallback...", result.returncode)
    except Exception as exc:
        logger.warning("Advanced video protection exception (%s), trying fast fallback...", exc)

    # Tier 2 Fallback: Fast stream copy with fresh randomized metadata
    cmd_fallback = [
        "ffmpeg", "-y",
        "-threads", "2",
        "-i", str(input_path),
        "-map_metadata", "-1",
        "-c", "copy",
        *params["metadata_args"],
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


def clean_instagram_caption(raw_text_or_html: str, username: str = "") -> tuple[str, str]:
    """
    Cleans raw Instagram caption (from embed HTML or yt-dlp):
    1. Extracts username if present in <a class="CaptionUsername">...</a>
    2. Completely removes <div class="CaptionComments">...</div> and comments block
    3. Strips the leading CaptionUsername anchor so username is not glued to hashtags
    4. Strips remaining HTML tags and unescapes entities
    5. Strips trailing 'View all X comments', 'View more on Instagram', 'X comments'
    6. Strips leading username text if still prepended to caption
    """
    if not raw_text_or_html:
        return "", username

    # 1. Extract username from CaptionUsername anchor if present
    user_match = re.search(r'<a[^>]*class="[^"]*CaptionUsername[^"]*"[^>]*>(.*?)</a>', raw_text_or_html, re.I | re.DOTALL)
    if user_match:
        extracted_user = re.sub(r'<[^>]+>', '', user_match.group(1)).strip()
        if extracted_user and not username:
            username = extracted_user

    # 2. Strip comments container (<div class="CaptionComments">...</div> or <a class="CaptionComments">...</a>)
    cleaned = re.sub(r'<[^>]*class="[^"]*CaptionComments[^"]*"[^>]*>.*?<\/[^>]+>', '', raw_text_or_html, flags=re.DOTALL | re.I)

    # 3. Strip the leading CaptionUsername anchor
    cleaned = re.sub(r'<a[^>]*class="[^"]*CaptionUsername[^"]*"[^>]*>.*?</a>', '', cleaned, flags=re.DOTALL | re.I)

    # 4. Strip any remaining HTML tags and unescape entities
    text = re.sub(r'<[^>]+>', '', cleaned)
    text = html.unescape(text).strip()

    # 5. Remove trailing "View all X comments", "View more on Instagram", etc.
    text = re.sub(r'View all \d+ comments.*$', '', text, flags=re.DOTALL | re.I).strip()
    text = re.sub(r'View more on Instagram.*$', '', text, flags=re.DOTALL | re.I).strip()
    text = re.sub(r'\b\d+\s+comments\s*$', '', text, flags=re.I).strip()

    # 6. If caption still starts with username (e.g. "baldkid67#..." or "baldkid67 ..."), strip it
    if username:
        user_pattern = rf'^{re.escape(username)}\s*'
        text = re.sub(user_pattern, '', text, flags=re.I).strip()

    return text.strip(), username


def _extract_media_result(work_dir: Path, info: dict, result: dict) -> bool:
    """Find and protect downloaded video or photos in work_dir."""
    if not info or not isinstance(info, dict):
        return False
    raw_caption = info.get("description") or info.get("title") or ""
    uploader = info.get("uploader") or info.get("channel") or ""
    if uploader and not result.get("author"):
        result["author"] = uploader

    cleaned_cap, _ = clean_instagram_caption(raw_caption, uploader)
    result["caption"] = cleaned_cap

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


def download_via_direct_embed(url: str, work_dir: Path, result: dict) -> bool:
    """
    100% Cookie-free direct media extractor using Instagram's public embed endpoints.
    - Zero accounts, sessions, or cookies required.
    - Directly extracts high-speed .mp4 CDN streams from Meta servers.
    - Extracts complete post captions (cleanly strips username prefix & comments suffix).
    - Processes video with ffmpeg anti-detection device profile injection.
    - Strictly prevents downloading static UI assets (e.g. App Store badges, icons).
    - Strictly enforces that Reel links (/reel/) must ONLY return video media.
    """
    shortcode = extract_shortcode(url)
    if not shortcode:
        return False

    is_reel_url = bool(re.search(r"instagram\.com/(?:reel|reels)/", url, re.I))

    # Fast route: Only probe matching endpoint type with tight timeouts to prevent datacenter stalls
    if is_reel_url:
        endpoints = [
            (f"https://www.instagram.com/reel/{shortcode}/embed/captioned/", 5),
            (f"https://www.instagram.com/reel/{shortcode}/embed/", 4),
        ]
    else:
        endpoints = [
            (f"https://www.instagram.com/p/{shortcode}/embed/captioned/", 5),
            (f"https://www.instagram.com/p/{shortcode}/embed/", 4),
        ]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    BAD_IMAGE_PATTERNS = [
        "static.cdninstagram.com",
        "rsrc.php",
        "app_store",
        "play_store",
        "favicon",
        "assets",
        "glyph",
        "badge",
        "button",
    ]

    for endpoint, ep_timeout in endpoints:
        try:
            r = cffi_requests.get(endpoint, headers=headers, impersonate="chrome124", timeout=ep_timeout)
            if r.status_code != 200 or len(r.text) < 1000:
                continue

            # Check if this embed is an empty login shell or unavailable post
            if 'isRichEmbed":false' in r.text and '"contextJSON":null' in r.text:
                logger.debug("Embed returned minimal shell (login wall / restricted): %s", shortcode)
                continue

            # Extract and clean caption + author
            caption = ""
            author_from_embed = ""
            cap_match = re.search(r'<div class="Caption"[^>]*>(.*?)</div>', r.text, re.DOTALL)
            if cap_match:
                caption, author_from_embed = clean_instagram_caption(cap_match.group(0))

            if author_from_embed and not result.get("author"):
                result["author"] = author_from_embed

            # Search for video URL
            v_url = None
            clean_page = r.text.replace(r"\/", "/").replace(r"\u0026", "&").replace("&amp;", "&")

            # Strategy A: "video_url" in JSON scripts
            v_matches = re.findall(r'"video_url"\s*:\s*"([^"]+)"', r.text)
            for vm in v_matches:
                cand = vm.replace(r"\/", "/").replace(r"\u0026", "&").replace("&amp;", "&").replace("\\", "")
                if ".mp4" in cand:
                    v_url = cand
                    break

            # Strategy B: "contentUrl" in JSON-LD schema
            if not v_url:
                ld_matches = re.findall(r'"contentUrl"\s*:\s*"([^"]+)"', r.text)
                for ldm in ld_matches:
                    cand = ldm.replace(r"\/", "/").replace(r"\u0026", "&").replace("&amp;", "&").replace("\\", "")
                    if ".mp4" in cand:
                        v_url = cand
                        break

            # Strategy C: direct .mp4 URLs in cleaned page
            if not v_url:
                mp4_matches = re.findall(r'https://[^"\'<>\s]+?\.mp4[^"\'<>\s]*', clean_page)
                for m in mp4_matches:
                    cand = m.replace("\\", "")
                    if "fbcdn.net" in cand or "cdninstagram.com" in cand:
                        v_url = cand
                        break

            # Download and protect video if found
            if v_url:
                raw_video = work_dir / "raw_media.mp4"
                v_resp = cffi_requests.get(v_url, impersonate="chrome124", timeout=18)
                if v_resp.status_code == 200 and len(v_resp.content) > 10000:
                    raw_video.write_bytes(v_resp.content)

                    clean_video = work_dir / "clean_media.mp4"
                    stripped = _strip_and_protect_video(raw_video, clean_video)
                    final_path = clean_video if (stripped and clean_video.exists() and clean_video.stat().st_size > 1000) else raw_video

                    result["success"] = True
                    result["video_path"] = final_path
                    result["caption"] = caption
                    result["is_video"] = True
                    logger.info("Direct cookie-free embed video download succeeded: %s (%.1f MB)", shortcode, final_path.stat().st_size / (1024 * 1024))
                    return True

            # If the user requested a Reel, NEVER fallback to image! Reels must ONLY be videos.
            if is_reel_url:
                continue

            # Strategy D: Real user photos / carousel (ONLY for photo posts /p/, NEVER for reels)
            img_matches = re.findall(r'https:[^"\'<>\s]+?\.(?:jpg|jpeg|webp)[^"\'<>\s]*', r.text)
            seen_cand = set()
            cand_urls = []
            for m in img_matches:
                img_url = m.replace(r"\/", "/").replace(r"\u0026", "&").replace("&amp;", "&").replace("\\", "")
                if any(bad in img_url.lower() for bad in BAD_IMAGE_PATTERNS):
                    continue
                if ("scontent" in img_url or "instagram.f" in img_url or "fbcdn.net" in img_url) and "150x150" not in img_url:
                    base_key = img_url.split("?")[0]
                    if base_key not in seen_cand:
                        seen_cand.add(base_key)
                        cand_urls.append(img_url)

            if cand_urls:
                downloaded_images = []
                for idx, c_url in enumerate(cand_urls[:10]):
                    try:
                        img_resp = cffi_requests.get(c_url, impersonate="chrome124", timeout=12)
                        if img_resp.status_code == 200 and len(img_resp.content) > 15000:
                            img_path = work_dir / f"slide_{idx+1}.jpg" if len(cand_urls) > 1 else work_dir / "media.jpg"
                            img_path.write_bytes(img_resp.content)
                            downloaded_images.append(img_path)
                    except Exception as e:
                        logger.debug("Failed downloading embed slide %d: %s", idx, e)

                if downloaded_images:
                    result["success"] = True
                    result["image_paths"] = downloaded_images
                    result["media_items"] = [{"type": "photo", "path": p} for p in downloaded_images]
                    result["caption"] = caption
                    result["is_video"] = False
                    logger.info("Direct cookie-free embed image(s) succeeded: %s (%d photos)", shortcode, len(downloaded_images))
                    return True

        except Exception as exc:
            logger.debug("Direct embed extractor error on %s: %s", endpoint, exc)

    return False


def shortcode_to_media_id(shortcode: str) -> int:
    """Convert Instagram Base64 shortcode to numeric media ID."""
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    media_id = 0
    for char in shortcode:
        if char in alphabet:
            media_id = (media_id * 64) + alphabet.index(char)
    return media_id


def download_instagram_polaris(shortcode: str, work_dir: Path, result: dict) -> bool:
    """
    Direct public extraction via Instagram's Polaris Desktop GraphQL API.
    Zero login, zero cookies, zero external paid dependencies.
    Extracts Single Photos, Multi-Photo Carousels, and Videos/Reels with 100% fidelity.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    try:
        session = cffi_requests.Session(impersonate="chrome124")
        base_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }

        r_home = session.get("https://www.instagram.com/", headers=base_headers, timeout=12)
        lsd_match = re.search(r'["\']LSD["\'],\[\],\{["\']token["\']:["\']([^"\']+)["\']\}', r_home.text)
        if not lsd_match:
            lsd_match = re.search(r'name=["\']lsd["\']\s+value=["\']([^"\']+)["\']', r_home.text)
        lsd_token = lsd_match.group(1) if lsd_match else ""
        csrf_token = session.cookies.get("csrftoken") or ""

        media_id = shortcode_to_media_id(shortcode)
        if not media_id:
            return False

        gql_headers = {
            "User-Agent": base_headers["User-Agent"],
            "X-IG-App-ID": "936619743392459",
            "X-ASBD-ID": "129477",
            "X-FB-Friendly-Name": "PolarisLoggedOutDesktopWWWPostRootContentQuery",
            "X-CSRFToken": csrf_token,
            "X-FB-LSD": lsd_token,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"https://www.instagram.com/p/{shortcode}/",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "lsd": lsd_token,
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "PolarisLoggedOutDesktopWWWPostRootContentQuery",
            "server_timestamps": "true",
            "variables": json.dumps({"media_id": str(media_id)}, separators=(",", ":")),
            "doc_id": "27130156389949648",
        }

        r_gql = session.post("https://www.instagram.com/api/graphql", data=payload, headers=gql_headers, timeout=15)
        if r_gql.status_code != 200 or not r_gql.text:
            return False

        data = r_gql.json()
        polaris_media = (data.get("data") or {}).get("xig_polaris_media") or {}
        prod = polaris_media.get("if_not_gated_logged_out")
        if not prod:
            logger.debug("Polaris post %s gated or unavailable", shortcode)
            return False

        uploader = prod.get("user", {}).get("username") or ""
        raw_cap = prod.get("caption", {}).get("text", "") if prod.get("caption") else ""
        cleaned_cap, _ = clean_instagram_caption(raw_cap, uploader)

        result["author"] = uploader
        result["caption"] = cleaned_cap
        result["shortcode"] = shortcode

        carousel = prod.get("carousel_media") or []
        if carousel and isinstance(carousel, list):
            # ── Multi-Slide Carousel (Photos and/or Videos) ──
            downloaded_images = []
            downloaded_videos = []
            media_items = []

            for idx, item in enumerate(carousel[:10]):
                if not isinstance(item, dict):
                    continue
                vids = item.get("video_versions") or []
                image_v2 = item.get("image_versions2") or {}
                imgs = image_v2.get("candidates") if isinstance(image_v2, dict) else []

                if vids and isinstance(vids, list) and len(vids) > 0 and isinstance(vids[0], dict) and vids[0].get("url"):
                    v_url = vids[0]["url"]
                    resp = session.get(v_url, timeout=30)
                    if resp.status_code == 200 and len(resp.content) > 5000:
                        v_file = work_dir / f"slide_{idx+1}.mp4"
                        v_file.write_bytes(resp.content)
                        clean_v = work_dir / f"slide_{idx+1}_clean.mp4"
                        if _strip_and_protect_video(v_file, clean_v) and clean_v.exists() and clean_v.stat().st_size > 1000:
                            downloaded_videos.append(clean_v)
                            media_items.append({"type": "video", "path": clean_v})
                        else:
                            downloaded_videos.append(v_file)
                            media_items.append({"type": "video", "path": v_file})
                elif imgs and isinstance(imgs, list) and len(imgs) > 0 and isinstance(imgs[0], dict) and imgs[0].get("url"):
                    img_url = imgs[0]["url"]
                    resp = session.get(img_url, timeout=20)
                    if resp.status_code == 200 and len(resp.content) > 1000:
                        img_file = work_dir / f"slide_{idx+1}.jpg"
                        img_file.write_bytes(resp.content)
                        downloaded_images.append(img_file)
                        media_items.append({"type": "photo", "path": img_file})

            if downloaded_images or downloaded_videos:
                result["image_paths"] = downloaded_images
                result["media_items"] = media_items
                result["is_video"] = (len(downloaded_videos) > 0 and len(downloaded_images) == 0)
                if len(downloaded_videos) == 1 and len(downloaded_images) == 0:
                    result["video_path"] = downloaded_videos[0]
                result["success"] = True
                logger.info(
                    "Polaris carousel download succeeded: %s (%d images, %d videos)",
                    shortcode, len(downloaded_images), len(downloaded_videos),
                )
                return True
        else:
            # ── Single Item (Photo or Video) ──
            vids = prod.get("video_versions") or []
            image_v2 = prod.get("image_versions2") or {}
            imgs = image_v2.get("candidates") if isinstance(image_v2, dict) else []

            if vids and isinstance(vids, list) and len(vids) > 0 and isinstance(vids[0], dict) and vids[0].get("url"):
                v_url = vids[0]["url"]
                resp = session.get(v_url, timeout=35)
                if resp.status_code == 200 and len(resp.content) > 5000:
                    raw_video = work_dir / "raw_media.mp4"
                    raw_video.write_bytes(resp.content)

                    clean_video = work_dir / "clean_media.mp4"
                    stripped = _strip_and_protect_video(raw_video, clean_video)
                    final_path = clean_video if (stripped and clean_video.exists() and clean_video.stat().st_size > 1000) else raw_video

                    result["video_path"] = final_path
                    result["is_video"] = True
                    result["success"] = True
                    logger.info("Polaris video download succeeded: %s (%.1f MB)", shortcode, final_path.stat().st_size / (1024 * 1024))
                    return True
            elif imgs and isinstance(imgs, list) and len(imgs) > 0 and isinstance(imgs[0], dict) and imgs[0].get("url"):
                img_url = imgs[0]["url"]
                resp = session.get(img_url, timeout=20)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    img_file = work_dir / "media.jpg"
                    img_file.write_bytes(resp.content)
                    result["image_paths"] = [img_file]
                    result["media_items"] = [{"type": "photo", "path": img_file}]
                    result["is_video"] = False
                    result["success"] = True
                    logger.info("Polaris single photo download succeeded: %s", shortcode)
                    return True

    except Exception as exc:
        logger.debug("Polaris extraction error for %s: %s", shortcode, exc)

    return False


def download_instagram(url: str) -> dict:
    """
    Download an Instagram reel/post/video with multi-tier architecture:
    1. Primary Tier: Direct Polaris GraphQL stream & carousel extractor (Instant, zero cookies).
    2. Secondary Tier: Direct public Meta CDN embed stream extractor.
    3. Fallback Tier A: Anonymous yt-dlp cookie-free public extraction.
    4. Fallback Tier B: Authenticated yt-dlp extraction (if cookies exist, for login-walled reels).
    5. Anti-Detection: Unique device profile injection & perceptual protection.
    6. Safe: Never requires accounts for public reels, posts, or carousels!
    """
    session_id = uuid.uuid4().hex[:8]
    work_dir = DOWNLOAD_DIR / session_id
    work_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "success":     False,
        "video_path":  None,
        "image_paths": None,
        "media_items": [],
        "caption":     "",
        "author":      "",
        "error":       None,   # friendly message for user
        "error_type":  None,   # 'private' | 'not_found' | 'generic'
        "raw_error":   None,   # full error string for admin log
    }

    shortcode = extract_shortcode(url)

    # ── Tier 1: Direct Cookie-Free Polaris GraphQL Engine (Photos, Carousels, Reels) ──
    if shortcode:
        try:
            if download_instagram_polaris(shortcode, work_dir, result):
                return result
        except Exception as exc:
            logger.debug("Polaris tier exception: %s", exc)

    # ── Tier 2: Direct Cookie-Free Embed Extractor (High-Speed Meta CDN) ──
    try:
        if download_via_direct_embed(url, work_dir, result):
            return result
    except Exception as exc:
        logger.debug("Direct embed tier exception: %s", exc)

    # ── Tier 2: Anonymous yt-dlp Cookie-Free Fallback ──
    try:
        ydl_opts_anon = _build_ydl_opts(work_dir, "media", use_cookies=False)
        with yt_dlp.YoutubeDL(ydl_opts_anon) as ydl_anon:
            info = ydl_anon.extract_info(url, download=True)

        if info and _extract_media_result(work_dir, info, result):
            logger.info("Cookie-free yt-dlp download succeeded!")
            return result

    except Exception as exc_anon:
        err_msg = str(exc_anon)
        err_lower = err_msg.lower()
        result["raw_error"] = err_msg
        logger.debug("Anonymous yt-dlp download failed: %s", exc_anon)

        if any(w in err_lower for w in ("audience", "available to everyone", "age", "certain audiences", "restricted", "sensitive", "login")):
            result["error_type"] = "cookie_needed"
            result["error"] = "This post requires cookies or login."
        elif "private" in err_lower and "rate" not in err_lower:
            result["error_type"] = "private"
            result["error"] = "This account or post is *private*."
        elif any(w in err_lower for w in ("not found", "does not exist", "removed")):
            result["error_type"] = "not_found"
            result["error"] = "This post or reel was deleted or is no longer available on Instagram."

    # ── Tier 3: Authenticated yt-dlp Fallback (Cookie System) ──
    cookies_path = find_cookies_file()
    if cookies_path:
        try:
            logger.info("Attempting authenticated yt-dlp fallback via %s...", cookies_path)
            ydl_opts_auth = _build_ydl_opts(work_dir, "media", use_cookies=True)
            with yt_dlp.YoutubeDL(ydl_opts_auth) as ydl_auth:
                info_auth = ydl_auth.extract_info(url, download=True)

            if info_auth and _extract_media_result(work_dir, info_auth, result):
                logger.info("Authenticated yt-dlp download succeeded via cookies!")
                return result
        except Exception as exc_auth:
            err_msg = str(exc_auth)
            err_lower = err_msg.lower()
            result["raw_error"] = err_msg

            if any(w in err_lower for w in ("not found", "unavailable", "does not exist", "removed")):
                result["error_type"] = "not_found"
                result["error"] = "This post or reel was deleted or is no longer available on Instagram."
            elif "private" in err_lower and "rate" not in err_lower:
                result["error_type"] = "private"
                result["error"] = "This account or post is *private*."
            else:
                result["error_type"] = "generic"
                result["error"] = "Download failed. Please ensure the link is a valid public reel."

            logger.warning("Authenticated yt-dlp fallback failed | type=%s | error=%s", result["error_type"], err_msg)
    else:
        logger.info("No cookies.txt available for authenticated fallback.")

    # If all tiers failed, return user-friendly error
    if not result.get("error"):
        if "/reel/" in url.lower() or "/reels/" in url.lower():
            result["error_type"] = result.get("error_type") or "not_found"
            result["error"] = "Could not retrieve this reel. Please verify that the link is valid."
        else:
            result["error_type"] = result.get("error_type") or "generic"
            result["error"] = "Download failed. Please ensure the link is a valid public reel or post."

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
    """
    Extract shortcode or video identifier from Instagram or TikTok URL.
    - Instagram: returns reel/post shortcode, e.g. 'C123456789'
    - TikTok: returns 'tt_' prefixed ID, e.g. 'tt_7106594312292453675' or 'tt_ZMxxxxxx'
    """
    if not url:
        return None

    # 1. Instagram shortcode
    ig_match = re.search(r"instagram\.com/(?:reel|reels|p|tv)/([A-Za-z0-9_-]+)", url)
    if ig_match:
        return ig_match.group(1)

    # 2. TikTok standard video/photo URL (@user/video/<id>, @user/photo/<id>, or /v/<id>)
    tt_id_match = re.search(r"tiktok\.com/(?:@[^/?#\s]+/(?:video|photo)/|v/)(\d+)", url)
    if tt_id_match:
        return f"tt_{tt_id_match.group(1)}"

    # 3. TikTok short / share link (vm.tiktok.com/<code/>, vt.tiktok.com/<code/>, tiktok.com/t/<code/>)
    tt_short_match = re.search(r"(?:vm|vt)\.tiktok\.com/([A-Za-z0-9_-]+)|tiktok\.com/t/([A-Za-z0-9_-]+)", url)
    if tt_short_match:
        code = tt_short_match.group(1) or tt_short_match.group(2)
        return f"tt_{code}"

    # 4. Fallback TikTok URL match
    tt_any = re.search(r"tiktok\.com/([A-Za-z0-9_.-]+)", url)
    if tt_any:
        return f"tt_{tt_any.group(1)}"

    return None


def download_tiktok(url: str) -> dict:
    """
    Download TikTok video, reel, or photo slideshow with 100% cookie-free architecture:
    1. Primary Tier: TikWM public API (watermark-free HD MP4, full captions, zero rate limits).
    2. Fallback Tier: Anonymous yt-dlp public extractor.
    3. Anti-Detection: Unique device profile injection & perceptual hash protection.
    4. Photo Slides: Automatically extracts and packages multi-image slideshows.
    5. Delivers complete creator details, caption, sound/music info, and audio extraction support.
    """
    session_id = uuid.uuid4().hex[:8]
    work_dir = DOWNLOAD_DIR / f"tt_{session_id}"
    work_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "success":    False,
        "video_path": None,
        "image_paths": [],
        "is_video":   True,
        "caption":    "",
        "author":     "",
        "music":      "",
        "shortcode":  None,
        "error":      None,
        "error_type": None,
        "raw_error":  None,
    }

    # Extract clean URL from input
    clean_url = url.strip()
    url_m = re.search(r"https?://[^\s]+", url)
    if url_m:
        clean_url = url_m.group(0).strip()
    elif not clean_url.startswith("http"):
        clean_url = f"https://{clean_url}"

    logger.info("Starting TikTok download: %s", clean_url)

    # ── Tier 1: TikWM Direct Public API (Watermark-free HD stream) ───────────────
    tikwm_endpoints = [
        "https://www.tikwm.com/api/",
        "https://tikwm.com/api/",
    ]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    }

    for ep in tikwm_endpoints:
        try:
            resp = cffi_requests.post(
                ep,
                data={"url": clean_url, "count": 12, "cursor": 0, "web": 1, "hd": 1},
                headers=headers,
                impersonate="chrome124",
                timeout=20,
            )
            if resp.status_code != 200:
                continue

            data = resp.json()
            code = data.get("code")
            if code != 0 or not data.get("data"):
                msg = (data.get("msg") or "").lower()
                if "private" in msg:
                    result["error_type"] = "private"
                    result["error"] = "This TikTok video or account is private."
                elif any(w in msg for w in ("not found", "deleted", "remove", "failed")):
                    result["error_type"] = "not_found"
                    result["error"] = "This TikTok video was deleted or is no longer available."
                continue

            d = data["data"]
            video_id = str(d.get("id") or "")
            caption = (d.get("title") or "").strip()
            author_info = d.get("author") or {}
            author_name = author_info.get("nickname") or author_info.get("unique_id") or ""
            music_info = d.get("music_info") or {}
            music_title = music_info.get("title") or ""

            result["caption"] = caption
            result["author"] = author_name
            result["music"] = music_title
            result["shortcode"] = f"tt_{video_id}" if video_id else (extract_shortcode(clean_url) or f"tt_{session_id}")

            # Check for photo slides / gallery
            images = d.get("images")
            if images and isinstance(images, list) and len(images) > 0:
                downloaded_images = []
                img_headers = {
                    "User-Agent": headers["User-Agent"],
                    "Referer": "https://www.tiktok.com/",
                    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                }
                for idx, img_url in enumerate(images[:10]):
                    try:
                        img_resp = cffi_requests.get(img_url, headers=img_headers, impersonate="chrome124", timeout=20)
                        if img_resp.status_code == 200 and len(img_resp.content) > 1000:
                            img_file = work_dir / f"slide_{idx + 1}.jpg"
                            img_file.write_bytes(img_resp.content)
                            downloaded_images.append(img_file)
                    except Exception as e:
                        logger.warning("Failed to download TikTok slide %d: %s", idx, e)

                if downloaded_images:
                    result["image_paths"] = downloaded_images
                    result["media_items"] = [{"type": "photo", "path": p} for p in downloaded_images]
                    result["is_video"] = False
                    result["success"] = True

                    # Extract and download background music track if available
                    music_url = d.get("music") or (d.get("music_info") or {}).get("play")
                    if music_url:
                        try:
                            m_resp = cffi_requests.get(music_url, headers=img_headers, impersonate="chrome124", timeout=20)
                            if m_resp.status_code == 200 and len(m_resp.content) > 2000:
                                audio_file = work_dir / "audio.mp3"
                                audio_file.write_bytes(m_resp.content)
                                result["audio_path"] = audio_file
                        except Exception as m_exc:
                            logger.debug("Failed downloading TikTok background audio: %s", m_exc)

                    logger.info("TikTok photo slides downloaded successfully: %d images", len(downloaded_images))
                    return result

            # Download video stream (prefer hdplay)
            play_url = d.get("hdplay") or d.get("play")
            if play_url:
                if play_url.startswith("/"):
                    play_url = "https://www.tikwm.com" + play_url

                raw_video = work_dir / "raw_media.mp4"
                v_resp = cffi_requests.get(play_url, headers=headers, impersonate="chrome124", timeout=45)
                if v_resp.status_code == 200 and len(v_resp.content) > 5000:
                    raw_video.write_bytes(v_resp.content)

                    clean_video = work_dir / "clean_media.mp4"
                    stripped = _strip_and_protect_video(raw_video, clean_video)
                    final_video = clean_video if (stripped and clean_video.exists() and clean_video.stat().st_size > 1000) else raw_video

                    result["video_path"] = final_video
                    result["is_video"] = True
                    result["success"] = True
                    logger.info(
                        "TikWM video download succeeded: %s (%.1f MB)",
                        result["shortcode"],
                        final_video.stat().st_size / (1024 * 1024),
                    )
                    return result

        except Exception as exc:
            logger.debug("TikWM attempt on %s failed: %s", ep, exc)

    # ── Tier 2: Anonymous yt-dlp Public Extractor Fallback ────────────────────────
    try:
        ydl_opts_anon = _build_ydl_opts(work_dir, "media", use_cookies=False)
        with yt_dlp.YoutubeDL(ydl_opts_anon) as ydl_anon:
            info = ydl_anon.extract_info(clean_url, download=True)

        if _extract_media_result(work_dir, info, result):
            result["shortcode"] = result.get("shortcode") or extract_shortcode(clean_url) or f"tt_{session_id}"
            uploader = info.get("uploader") or info.get("channel") or ""
            if uploader and not result.get("author"):
                result["author"] = uploader
            logger.info("TikTok download succeeded via anonymous yt-dlp fallback!")
            return result

    except yt_dlp.utils.DownloadError as exc_1:
        err_msg = str(exc_1)
        err_lower = err_msg.lower()
        result["raw_error"] = err_msg

        if any(w in err_lower for w in ("not found", "unavailable", "does not exist", "removed")):
            result["error_type"] = "not_found"
            result["error"] = "This TikTok video was deleted or is no longer available."
        elif "private" in err_lower:
            result["error_type"] = "private"
            result["error"] = "This TikTok account or video is private."
        else:
            result["error_type"] = "generic"
            result["error"] = "Could not download this TikTok video. Please ensure the link is public and valid."

        logger.warning("TikTok yt-dlp fallback error: %s", err_msg)

    except Exception as exc:
        result["error_type"] = "generic"
        result["error"] = "Could not download this TikTok video."
        result["raw_error"] = str(exc)
        logger.exception("Unexpected TikTok download error")

    if not result.get("error"):
        result["error_type"] = "generic"
        result["error"] = "Could not download this TikTok video. Please ensure the link is public and valid."

    return result


def download_media(url: str) -> dict:
    """
    Unified media downloader:
    - Auto-detects whether the URL is TikTok or Instagram.
    - Routes to the appropriate high-speed cookie-free engine.
    - Always applies anti-detection metadata and perceptual protection.
    - Never returns None or throws unhandled exception.
    """
    try:
        if is_tiktok_url(url):
            return download_tiktok(url)
        return download_instagram(url)
    except Exception as exc:
        logger.exception("Top-level download_media failure: %s", exc)
        return {
            "success": False,
            "video_path": None,
            "image_paths": None,
            "media_items": [],
            "caption": "",
            "author": "",
            "error": "Failed to process download link. Please try again.",
            "error_type": "generic",
            "raw_error": str(exc),
        }


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
    """Check if cookies are present and available for authenticated fallback."""
    cf = find_cookies_file()
    if cf and Path(cf).exists() and Path(cf).stat().st_size > 50:
        return True, f"Cookies active ({Path(cf).stat().st_size:,} bytes)"
    return False, "Operating in cookie-free public mode"


