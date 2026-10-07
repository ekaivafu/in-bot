"""
cloud_manager.py
────────────────
Cloud Automation Client for Render API and UptimeRobot API.

Enables automated:
1. Render Web Service provisioning for Child Scout Workers.
2. External 24/7 Keep-Alive monitoring via UptimeRobot REST API v2 (safe from Render ban).
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger("cloud_manager")

UPTIMEROBOT_API_URL = "https://api.uptimerobot.com/v2"
RENDER_API_URL = "https://api.render.com/v1"


# ── UptimeRobot API v2 ─────────────────────────────────────────────────────────
def create_uptimerobot_monitor(
    server_url: str,
    friendly_name: str,
    api_key: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Creates an external HTTP keep-alive monitor via UptimeRobot v2 REST API.
    Pings the child server every 5 minutes (standard free tier) to keep it alive 24/7.
    Returns: (True, monitor_id) or (False, error_reason)
    """
    key = (api_key or os.getenv("UPTIMEROBOT_API_KEY", "")).strip()
    if not key:
        return False, "UPTIMEROBOT_API_KEY is not configured in .env"

    clean_url = server_url.strip()
    if not clean_url.startswith("http://") and not clean_url.startswith("https://"):
        clean_url = f"https://{clean_url}"

    payload = {
        "api_key": key,
        "format": "json",
        "type": "1",  # 1 = HTTP(s)
        "url": clean_url,
        "friendly_name": friendly_name.strip() or "InstaBot Child Worker",
        "interval": "300",  # 300 seconds = 5 minutes
    }

    try:
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{UPTIMEROBOT_API_URL}/newMonitor",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "InstaBot-Cluster/1.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            res = json.loads(body)

        if res.get("stat") == "ok":
            monitor_id = str(res.get("monitor", {}).get("id", ""))
            logger.info("Created UptimeRobot monitor '%s' for %s (ID: %s)", friendly_name, clean_url, monitor_id)
            return True, monitor_id
        else:
            err_msg = res.get("error", {}).get("message", "UptimeRobot API error")
            logger.warning("UptimeRobot failed to create monitor for %s: %s", clean_url, err_msg)
            return False, err_msg
    except urllib.error.HTTPError as exc:
        err_text = exc.read().decode("utf-8", errors="ignore")
        logger.warning("UptimeRobot HTTP error %d: %s", exc.code, err_text)
        return False, f"HTTP {exc.code}: {err_text[:120]}"
    except Exception as exc:
        logger.warning("UptimeRobot request exception for %s: %s", clean_url, exc)
        return False, str(exc)


def delete_uptimerobot_monitor(
    monitor_id: str,
    api_key: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Deletes an existing monitor from UptimeRobot.
    Returns: (True, "deleted") or (False, error_reason)
    """
    key = (api_key or os.getenv("UPTIMEROBOT_API_KEY", "")).strip()
    if not key:
        return False, "UPTIMEROBOT_API_KEY is not configured"

    if not monitor_id:
        return False, "Missing monitor ID"

    payload = {
        "api_key": key,
        "format": "json",
        "id": str(monitor_id).strip(),
    }

    try:
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{UPTIMEROBOT_API_URL}/deleteMonitor",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            res = json.loads(body)

        if res.get("stat") == "ok":
            logger.info("Deleted UptimeRobot monitor ID: %s", monitor_id)
            return True, "deleted"
        else:
            err_msg = res.get("error", {}).get("message", "Could not delete monitor")
            return False, err_msg
    except Exception as exc:
        logger.warning("Error deleting UptimeRobot monitor %s: %s", monitor_id, exc)
        return False, str(exc)


# ── Render API v1 ─────────────────────────────────────────────────────────────
def get_render_owner_id(render_api_key: str) -> Tuple[bool, str]:
    """
    Queries Render API to get the user / team owner ID required for service creation.
    Returns: (True, owner_id) or (False, error_message)
    """
    key = render_api_key.strip()
    if not key:
        return False, "Empty Render API key"

    try:
        req = urllib.request.Request(
            f"{RENDER_API_URL}/owners?limit=5",
            headers={
                "Authorization": f"Bearer {key}",
                "Accept": "application/json",
                "User-Agent": "InstaBot-Cluster/1.0",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            owners_data = json.loads(body)

        if isinstance(owners_data, list) and owners_data:
            first_entry = owners_data[0]
            owner_info = first_entry.get("owner", first_entry)
            owner_id = owner_info.get("id", "")
            if owner_id:
                return True, owner_id

        return False, "No valid owner/workspace found for this Render API key."
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="ignore")
        return False, f"Render API HTTP {exc.code}: {err_body[:150]}"
    except Exception as exc:
        return False, f"Render connection error: {exc}"


def deploy_render_child_service(
    render_api_key: str,
    repo_url: str,
    db_url: str,
    bot_token: str,
    admin_id: str,
    worker_id: int,
    service_name: str = "",
) -> Tuple[bool, Any]:
    """
    Automatically creates a Render Web Service running the child scout worker.
    Returns:
      (True, {"service_id": str, "url": str, "name": str})
      or
      (False, error_message)
    """
    key = render_api_key.strip()
    if not key:
        return False, "Render API key is required."

    clean_repo = repo_url.strip()
    if not clean_repo:
        clean_repo = os.getenv("GITHUB_REPO_URL", "").strip()

    if not clean_repo:
        return False, "GITHUB_REPO_URL is not set in .env or provided. Please set your GitHub repository link."

    # Step 1: Find owner ID
    ok, owner_id = get_render_owner_id(key)
    if not ok:
        return False, f"Failed to authenticate with Render: {owner_id}"

    # Step 2: Format service payload
    name = service_name.strip() or f"instabot-worker-{worker_id}"
    payload = {
        "type": "web_service",
        "name": name,
        "ownerId": owner_id,
        "repo": clean_repo,
        "autoDeploy": "yes",
        "branch": "main",
        "serviceDetails": {
            "env": "python",
            "envSpecificDetails": {
                "buildCommand": "pip install -r requirements.txt",
                "startCommand": "python child/scout.py",
            },
            "plan": "free",
            "region": "oregon",
        },
        "envVars": [
            {"key": "DATABASE_URL", "value": db_url},
            {"key": "BOT_TOKEN", "value": bot_token},
            {"key": "ADMIN_ID", "value": str(admin_id)},
            {"key": "WORKER_ID", "value": str(worker_id)},
            {"key": "PORT", "value": "8080"},
            {"key": "CREATOR_GAP_SECONDS", "value": "300"},
        ],
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{RENDER_API_URL}/services",
            data=data,
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "InstaBot-Cluster/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            body = resp.read().decode("utf-8")
            res = json.loads(body)

        srv_id = res.get("id", "")
        slug = res.get("slug", name)
        srv_url = res.get("serviceDetails", {}).get("url") or f"https://{slug}.onrender.com"

        logger.info("Successfully created Render child service: %s (ID: %s, URL: %s)", name, srv_id, srv_url)
        return True, {
            "service_id": srv_id,
            "url": srv_url,
            "name": name,
        }
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="ignore")
        logger.warning("Render deployment HTTP %d: %s", exc.code, err_body)
        return False, f"Render API error (HTTP {exc.code}): {err_body[:200]}"
    except Exception as exc:
        logger.warning("Render deployment failed: %s", exc)
        return False, str(exc)


def delete_render_service(render_api_key: str, service_id: str) -> Tuple[bool, str]:
    """Deletes a Render service by service ID."""
    key = render_api_key.strip()
    if not key or not service_id:
        return False, "Missing key or service ID"

    try:
        req = urllib.request.Request(
            f"{RENDER_API_URL}/services/{service_id}",
            headers={"Authorization": f"Bearer {key}"},
            method="DELETE",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return True, "deleted"
    except Exception as exc:
        return False, str(exc)
