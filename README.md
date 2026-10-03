# InstaLoader Bot 🤖

A Telegram bot that downloads Instagram Reels, Posts, and IGTV videos in the highest quality, strips metadata, and sends back the caption in monospace for easy copy-paste.

---

## Features

- 🎬 Downloads Reels, Posts, IGTV in **best available quality**
- 📋 Sends the post **caption in monospace** (easy copy-paste)
- 🔒 **Strips all metadata** via ffmpeg re-encode (no Instagram fingerprint)
- ⚡ Powered by `yt-dlp` + ffmpeg — no Instagram account needed on server
- ☁️ **Ready to deploy on Render**

---

## Requirements

- Python 3.10+
- A Telegram Bot Token from [@BotFather](https://t.me/BotFather)
- An `cookies.txt` file exported from your browser (Instagram login required)
- **On Render:** ffmpeg is pre-installed — no extra setup needed

---

## Setup

### 1. Export Instagram Cookies (Required)

Instagram blocks unauthenticated requests. You need to export cookies from your browser:

1. Install the **"Get cookies.txt LOCALLY"** Chrome extension:
   👉 https://chrome.google.com/webstore/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc

2. Open Instagram in Chrome and **make sure you're logged in**

3. Click the extension icon → select `instagram.com` → click **Export**

4. Save the file as `cookies.txt`

> ⚠️ Cookies expire over time. If the bot starts failing, re-export and re-upload.

---

## Local Development

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy env template
copy .env.example .env

# 3. Edit .env — add your bot token
# BOT_TOKEN=your_token_here

# 4. Place cookies.txt in the project root

# 5. Run
python bot.py
```

---

## 🚀 Deploy on Render

### Step 1 — Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/instabot.git
git push -u origin main
```

### Step 2 — Create a Render Web Service

1. Go to [render.com](https://render.com) → **New → Web Service**
2. Connect your GitHub repo
3. Configure:
   | Field | Value |
   |---|---|
   | Environment | Python 3 |
   | Build Command | `pip install -r requirements.txt` |
   | Start Command | `python bot.py` |

### Step 3 — Add Environment Variables

In Render → **Environment** tab, add:

| Key | Value |
|---|---|
| `BOT_TOKEN` | Your Telegram bot token |

### Step 4 — Upload cookies.txt as Secret File

In Render → **Secret Files** tab:

| Filename | Content |
|---|---|
| `/etc/secrets/cookies.txt` | Paste the full content of your `cookies.txt` |

> The bot automatically looks for cookies at `/etc/secrets/cookies.txt` on Render.

### Step 5 — Deploy!

Click **Deploy** — your bot will be live in ~2 minutes.

---

## Project Structure

```
instabot/
├── bot.py           # Telegram bot entry point
├── downloader.py    # yt-dlp download + ffmpeg metadata stripping
├── requirements.txt
├── .env.example
├── .env             # Your secrets (not committed)
├── cookies.txt      # Instagram cookies (not committed — upload to Render)
└── downloads/       # Temp download folder (auto-cleaned)
```

---

## Notes

- Only **public** Instagram accounts are supported
- Downloaded files are automatically deleted after sending
- Cookies expire — if downloads fail, re-export and update the Render Secret File
- ffmpeg is available by default on Render's Python environment
