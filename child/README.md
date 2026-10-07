# 🤖 InstaBot Child Worker: Autonomous Viral Reel Scout

This directory contains the independent **Child Worker** (`child/scout.py`) designed to run on a separate server or Render Web Service instance while sharing the same PostgreSQL database (`DATABASE_URL`).

---

## 🚀 How It Works
1. **Target Inspection:** Loads creators from the shared PostgreSQL database table (`creator_watchlist`).
2. **Stealth Scraper:** Uses `curl_cffi` with Chrome 124 TLS fingerprint impersonation to inspect creator profiles anonymously without login or account risks.
3. **Smart Viral Filter:**
   - **Age:** 0 to 5 days old (`MAX_AGE_DAYS=5.0`).
   - **Engagement:** Minimum 5,000 likes (`MIN_LIKES=5000`).
   - **Zero Duplicates:** Checks `scout_seen_reels` before any downloading.
4. **Anti-Detection Video Engine:**
   - Strips all original tracking metadata (`-map_metadata -1`).
   - Applies subtle micro-adjustments to change cryptographic and perceptual hashes.
   - Constrained to 2 threads (`-threads 2`) to ensure Render 512MB RAM safety.
5. **Telegram Cloud Storage:**
   - Uploads the protected video directly to Telegram (Admin or designated dump channel).
   - Retrieves Telegram's persistent `file_id`.
   - Records the reel in `scout_queue` and `media_cache`.
6. **Zero Disk Footprint:** Immediately purges temporary files so disk storage stays at 0 MB.

---

## 🛠️ CLI Commands (Local Testing & Management)

```bash
# Add a creator to the watchlist
python child/scout.py --add mkbhd

# List current active creators in watchlist
python child/scout.py --list

# Run a single scout cycle and exit (great for testing or cron jobs)
python child/scout.py --once

# Remove a creator from the watchlist
python child/scout.py --remove mkbhd

# Run in continuous 24/7 daemon mode
python child/scout.py
```

---

## 🌐 Deploying on Render (Free Web Service)

1. Create a **New Web Service** on Render pointing to your repository.
2. Set **Root Directory:** (leave empty or set to repository root).
3. Set **Build Command:** `pip install -r requirements.txt`
4. Set **Start Command:** `python child/scout.py`
5. Configure Environment Variables:
   - `DATABASE_URL`: Your shared Neon PostgreSQL connection string.
   - `BOT_TOKEN`: Your Telegram Bot token.
   - `ADMIN_ID`: Your Telegram numeric User ID.
   - `SCOUT_TARGET_CHAT`: (Optional) Channel ID or Chat ID to receive viral reel alerts (defaults to `ADMIN_ID`).
   - `MIN_LIKES`: `5000` (default)
   - `MAX_AGE_DAYS`: `5.0` (default)
   - `SCOUT_INTERVAL_MINUTES`: `30` (default)
   - `PORT`: `8080` (or leave default, Render sets this automatically)

> 💡 **Why Render Free Web Service Works:** `child/scout.py` includes a built-in background HTTP health server that responds `200 OK` on `$PORT`. This prevents Render's free tier port scan timeouts!
