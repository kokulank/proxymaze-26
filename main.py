"""
ProxyMaze'26 - Complete Implementation
Targets 270/270 score
"""

import asyncio
import uuid
import time
import logging
from datetime import datetime, timezone
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
config = {
    "check_interval_seconds": 60,
    "request_timeout_ms": 5000,
}

# proxy_id -> proxy dict
proxies: dict[str, dict] = {}

# alert_id -> alert dict
alerts: dict[str, dict] = {}

# webhook_id -> webhook dict
webhooks: dict[str, dict] = {}

# integration_id -> integration dict
integrations: dict[str, dict] = {}

# metrics counters
metrics = {
    "total_checks": 0,
    "webhook_deliveries": 0,
}

# alert state
active_alert_id: Optional[str] = None

# lock for alert state mutations
alert_lock = asyncio.Lock()

THRESHOLD = 0.20

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def now_ts() -> int:
    return int(time.time())

def extract_proxy_id(url: str) -> str:
    return url.rstrip("/").split("/")[-1]

def compute_failure_rate() -> tuple[int, int, int, list[str]]:
    """Returns (total, up_count, down_count, failed_ids). Pending not counted as down."""
    total = len(proxies)
    down_ids = [pid for pid, p in proxies.items() if p["status"] == "down"]
    up_count = sum(1 for p in proxies.values() if p["status"] == "up")
    return total, up_count, len(down_ids), down_ids

# ---------------------------------------------------------------------------
# Background monitoring
# ---------------------------------------------------------------------------

async def probe_proxy(proxy: dict) -> bool:
    """Returns True if proxy is up."""
    url = proxy["url"]
    timeout_s = config["request_timeout_ms"] / 1000.0
    try:
        async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
            resp = await client.get(url)
            return 200 <= resp.status_code < 300
    except Exception:
        return False

async def check_all_proxies():
    """Run one health-check cycle for all proxies."""
    if not proxies:
        return

    tasks = {pid: asyncio.create_task(probe_proxy(p)) for pid, p in list(proxies.items())}
    results = {}
    for pid, task in tasks.items():
        try:
            results[pid] = await task
        except Exception:
            results[pid] = False

    checked_at = now_iso()
    for pid, is_up in results.items():
        if pid not in proxies:
            continue
        p = proxies[pid]
        old_status = p["status"]
        new_status = "up" if is_up else "down"
        p["status"] = new_status
        p["last_checked_at"] = checked_at

        if new_status == "down":
            p["consecutive_failures"] = p.get("consecutive_failures", 0) + 1
        else:
            p["consecutive_failures"] = 0

        p["total_checks"] = p.get("total_checks", 0) + 1
        up_checks = p.get("up_checks", 0) + (1 if is_up else 0)
        p["up_checks"] = up_checks
        p["uptime_percentage"] = round((up_checks / p["total_checks"]) * 100, 1)

        # Append to history
        p["history"].append({"checked_at": checked_at, "status": new_status})

        metrics["total_checks"] += 1

    # Evaluate alert state after all checks
    await evaluate_alert()

async def deliver_webhook(url: str, payload: dict, retries: int = 10):
    """Deliver webhook with retry on 5xx."""
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                if resp.status_code in (500, 502, 503, 504):
                    logger.warning(f"Webhook {url} returned {resp.status_code}, retrying...")
                    await asyncio.sleep(min(2 ** attempt, 30))
                    continue
                # Any non-5xx = success (including 4xx - we stop)
                metrics["webhook_deliveries"] += 1
                logger.info(f"Webhook delivered to {url} on attempt {attempt+1}")
                return True
        except Exception as e:
            logger.warning(f"Webhook delivery error to {url}: {e}, attempt {attempt+1}")
            await asyncio.sleep(min(2 ** attempt, 30))
    logger.error(f"Webhook delivery failed after {retries} attempts: {url}")
    return False

async def deliver_to_all_webhooks(payload: dict):
    """Fire webhook delivery to all registered receivers (non-blocking)."""
    for wh in list(webhooks.values()):
        asyncio.create_task(deliver_webhook(wh["url"], payload))

async def deliver_slack_integration(alert: dict, event_type: str):
    """Send Slack-formatted payload to registered Slack integrations."""
    for intg in list(integrations.values()):
        if intg["type"] != "slack":
            continue
        if event_type not in intg.get("events", []):
            continue

        fired_at = alert.get("fired_at", now_iso())
        ts = int(datetime.strptime(fired_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())

        if event_type == "alert.fired":
            color = "#FF0000"
            text = f":red_circle: *ALERT FIRED* — Proxy pool failure rate exceeded threshold"
            fields = [
                {"title": "Alert ID", "value": alert["alert_id"], "short": True},
                {"title": "Failure Rate", "value": str(alert["failure_rate"]), "short": True},
                {"title": "Failed Proxies", "value": str(alert["failed_proxies"]), "short": True},
                {"title": "Threshold", "value": str(alert["threshold"]), "short": True},
                {"title": "Failed IDs", "value": ", ".join(alert.get("failed_proxy_ids", [])), "short": False},
                {"title": "Fired At", "value": fired_at, "short": True},
            ]
        else:  # alert.resolved
            color = "#00FF00"
            text = f":large_green_circle: *ALERT RESOLVED* — Proxy pool failure rate dropped below threshold"
            resolved_at = alert.get("resolved_at", now_iso())
            fields = [
                {"title": "Alert ID", "value": alert["alert_id"], "short": True},
                {"title": "Failure Rate", "value": str(alert.get("failure_rate", 0)), "short": True},
                {"title": "Failed Proxies", "value": str(alert.get("failed_proxies", 0)), "short": True},
                {"title": "Threshold", "value": str(alert["threshold"]), "short": True},
                {"title": "Failed IDs", "value": ", ".join(alert.get("failed_proxy_ids", [])) or "none", "short": False},
                {"title": "Fired At", "value": fired_at, "short": True},
            ]
            ts = int(datetime.strptime(resolved_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())

        payload = {
            "username": intg.get("username", "ProxyWatch"),
            "text": text,
            "attachments": [
                {
                    "color": color,
                    "fields": fields,
                    "footer": "ProxyMaze'26 by Torch Labs",
                    "ts": ts,
                }
            ],
        }
        asyncio.create_task(deliver_webhook(intg["webhook_url"], payload))

async def deliver_discord_integration(alert: dict, event_type: str):
    """Send Discord-formatted payload to registered Discord integrations."""
    for intg in list(integrations.values()):
        if intg["type"] != "discord":
            continue
        if event_type not in intg.get("events", []):
            continue

        fired_at = alert.get("fired_at", now_iso())

        if event_type == "alert.fired":
            color = 16711680  # Red
            title = "🚨 Alert Fired — Proxy Pool Threshold Breached"
            description = f"Proxy pool failure rate has exceeded the threshold of {alert['threshold']}."
            fields = [
                {"name": "Alert ID", "value": alert["alert_id"], "inline": True},
                {"name": "Failure Rate", "value": str(alert["failure_rate"]), "inline": True},
                {"name": "Failed Proxies", "value": str(alert["failed_proxies"]), "inline": True},
                {"name": "Threshold", "value": str(alert["threshold"]), "inline": True},
                {"name": "Failed IDs", "value": ", ".join(alert.get("failed_proxy_ids", [])) or "none", "inline": False},
                {"name": "Fired At", "value": fired_at, "inline": True},
            ]
        else:  # alert.resolved
            color = 65280  # Green
            title = "✅ Alert Resolved — Proxy Pool Recovered"
            description = f"Proxy pool failure rate has dropped below the threshold of {alert['threshold']}."
            fields = [
                {"name": "Alert ID", "value": alert["alert_id"], "inline": True},
                {"name": "Failure Rate", "value": str(alert.get("failure_rate", 0)), "inline": True},
                {"name": "Failed Proxies", "value": str(alert.get("failed_proxies", 0)), "inline": True},
                {"name": "Threshold", "value": str(alert["threshold"]), "inline": True},
                {"name": "Failed IDs", "value": ", ".join(alert.get("failed_proxy_ids", [])) or "none", "inline": False},
            ]

        payload = {
            "username": intg.get("username", "ProxyWatch"),
            "embeds": [
                {
                    "title": title,
                    "description": description,
                    "color": color,
                    "fields": fields,
                    "footer": {"text": "ProxyMaze'26 by Torch Labs"},
                }
            ],
        }
        asyncio.create_task(deliver_webhook(intg["webhook_url"], payload))

async def fire_alert(total: int, down_count: int, failed_ids: list[str], failure_rate: float):
    """Create and fire a new alert."""
    global active_alert_id
    alert_id = f"alert-{uuid.uuid4().hex[:8]}"
    fired_at = now_iso()
    alert = {
        "alert_id": alert_id,
        "status": "active",
        "failure_rate": round(failure_rate, 4),
        "total_proxies": total,
        "failed_proxies": down_count,
        "failed_proxy_ids": list(failed_ids),
        "threshold": THRESHOLD,
        "fired_at": fired_at,
        "resolved_at": None,
        "message": "Proxy pool failure rate exceeded threshold",
    }
    alerts[alert_id] = alert
    active_alert_id = alert_id
    logger.info(f"Alert fired: {alert_id}, failure_rate={failure_rate}")

    payload = {
        "event": "alert.fired",
        "alert_id": alert_id,
        "fired_at": fired_at,
        "failure_rate": alert["failure_rate"],
        "total_proxies": total,
        "failed_proxies": down_count,
        "failed_proxy_ids": list(failed_ids),
        "threshold": THRESHOLD,
        "message": "Proxy pool failure rate exceeded threshold",
    }
    await deliver_to_all_webhooks(payload)
    await deliver_slack_integration(alert, "alert.fired")
    await deliver_discord_integration(alert, "alert.fired")

async def resolve_alert(alert_id: str):
    """Resolve an active alert."""
    global active_alert_id
    if alert_id not in alerts:
        return
    alert = alerts[alert_id]
    resolved_at = now_iso()
    alert["status"] = "resolved"
    alert["resolved_at"] = resolved_at
    active_alert_id = None
    logger.info(f"Alert resolved: {alert_id}")

    payload = {
        "event": "alert.resolved",
        "alert_id": alert_id,
        "resolved_at": resolved_at,
    }
    await deliver_to_all_webhooks(payload)
    await deliver_slack_integration(alert, "alert.resolved")
    await deliver_discord_integration(alert, "alert.resolved")

async def evaluate_alert():
    """Check pool state and fire/resolve alerts as needed."""
    global active_alert_id

    async with alert_lock:
        total = len(proxies)
        # Only consider proxies that have been checked (not pending) for failure rate
        checked = [p for p in proxies.values() if p["status"] != "pending"]
        if total == 0:
            return

        down_ids = [pid for pid, p in proxies.items() if p["status"] == "down"]
        down_count = len(down_ids)
        failure_rate = down_count / total

        if failure_rate >= THRESHOLD:
            if active_alert_id is None:
                # No active alert — fire one
                await fire_alert(total, down_count, down_ids, failure_rate)
            else:
                # Update existing active alert's failed_proxy_ids to stay consistent
                alert = alerts[active_alert_id]
                alert["failed_proxy_ids"] = list(down_ids)
                alert["failed_proxies"] = down_count
                alert["failure_rate"] = round(failure_rate, 4)
                alert["total_proxies"] = total
        else:
            if active_alert_id is not None:
                # Resolve the active alert
                await resolve_alert(active_alert_id)

async def monitoring_loop():
    """Background monitoring loop."""
    logger.info("Monitoring loop started")
    while True:
        try:
            interval = config["check_interval_seconds"]
            await asyncio.sleep(interval)
            await check_all_proxies()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Monitoring loop error: {e}")
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(monitoring_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

# ---------------------------------------------------------------------------
# Chapter 01: GET /health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}

# ---------------------------------------------------------------------------
# Chapter 02: POST /config
# ---------------------------------------------------------------------------

class ConfigRequest(BaseModel):
    check_interval_seconds: int
    request_timeout_ms: int
    model_config = {"extra": "allow"}

@app.post("/config")
async def set_config(body: ConfigRequest):
    config["check_interval_seconds"] = body.check_interval_seconds
    config["request_timeout_ms"] = body.request_timeout_ms
    return {"check_interval_seconds": body.check_interval_seconds, "request_timeout_ms": body.request_timeout_ms}

# ---------------------------------------------------------------------------
# Chapter 03: GET /config
# ---------------------------------------------------------------------------

@app.get("/config")
async def get_config():
    return config

# ---------------------------------------------------------------------------
# Chapter 04: POST /proxies
# ---------------------------------------------------------------------------

class ProxiesRequest(BaseModel):
    proxies: list[str]
    replace: Optional[bool] = False
    model_config = {"extra": "allow"}

@app.post("/proxies", status_code=201)
async def add_proxies(body: ProxiesRequest):
    if body.replace:
        proxies.clear()

    accepted = []
    for url in body.proxies:
        pid = extract_proxy_id(url)
        if pid not in proxies:
            proxies[pid] = {
                "id": pid,
                "url": url,
                "status": "pending",
                "last_checked_at": None,
                "consecutive_failures": 0,
                "total_checks": 0,
                "up_checks": 0,
                "uptime_percentage": 0.0,
                "history": [],
            }
        accepted.append(proxies[pid])

    # Immediately kick off a check in background (don't wait)
    asyncio.create_task(check_all_proxies())

    return {
        "accepted": len(accepted),
        "proxies": [{"id": p["id"], "url": p["url"], "status": p["status"]} for p in accepted],
    }

# ---------------------------------------------------------------------------
# Chapter 05: GET /proxies
# ---------------------------------------------------------------------------

@app.get("/proxies")
async def list_proxies():
    total = len(proxies)
    up = sum(1 for p in proxies.values() if p["status"] == "up")
    down = sum(1 for p in proxies.values() if p["status"] == "down")
    failure_rate = round(down / total, 4) if total > 0 else 0.0

    proxy_list = []
    for p in proxies.values():
        proxy_list.append({
            "id": p["id"],
            "url": p["url"],
            "status": p["status"],
            "last_checked_at": p["last_checked_at"],
            "consecutive_failures": p["consecutive_failures"],
        })

    return {
        "total": total,
        "up": up,
        "down": down,
        "failure_rate": failure_rate,
        "proxies": proxy_list,
    }

# ---------------------------------------------------------------------------
# Chapter 06: GET /proxies/{id}
# ---------------------------------------------------------------------------

@app.get("/proxies/{proxy_id}")
async def get_proxy(proxy_id: str):
    if proxy_id not in proxies:
        raise HTTPException(status_code=404, detail="Proxy not found")
    p = proxies[proxy_id]
    return {
        "id": p["id"],
        "url": p["url"],
        "status": p["status"],
        "last_checked_at": p["last_checked_at"],
        "consecutive_failures": p["consecutive_failures"],
        "total_checks": p["total_checks"],
        "uptime_percentage": p["uptime_percentage"],
        "history": p["history"],
    }

# ---------------------------------------------------------------------------
# Chapter 07: GET /proxies/{id}/history
# ---------------------------------------------------------------------------

@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    if proxy_id not in proxies:
        raise HTTPException(status_code=404, detail="Proxy not found")
    return proxies[proxy_id]["history"]

# ---------------------------------------------------------------------------
# Chapter 08: DELETE /proxies
# ---------------------------------------------------------------------------

@app.delete("/proxies", status_code=204)
async def delete_proxies():
    proxies.clear()
    return None

# ---------------------------------------------------------------------------
# Chapter 09: GET /alerts
# ---------------------------------------------------------------------------

@app.get("/alerts")
async def list_alerts():
    return list(alerts.values())

# ---------------------------------------------------------------------------
# Chapter 10: POST /webhooks
# ---------------------------------------------------------------------------

class WebhookRequest(BaseModel):
    url: str
    model_config = {"extra": "allow"}

@app.post("/webhooks", status_code=201)
async def register_webhook(body: WebhookRequest):
    wh_id = f"wh-{uuid.uuid4().hex[:8]}"
    webhooks[wh_id] = {"webhook_id": wh_id, "url": body.url}
    return {"webhook_id": wh_id, "url": body.url}

# ---------------------------------------------------------------------------
# Chapter 11: POST /integrations
# ---------------------------------------------------------------------------

class IntegrationRequest(BaseModel):
    type: str
    webhook_url: str
    username: Optional[str] = "ProxyWatch"
    events: Optional[list[str]] = ["alert.fired", "alert.resolved"]
    model_config = {"extra": "allow"}

@app.post("/integrations", status_code=201)
async def register_integration(body: IntegrationRequest):
    intg_id = f"intg-{uuid.uuid4().hex[:8]}"
    integrations[intg_id] = {
        "integration_id": intg_id,
        "type": body.type,
        "webhook_url": body.webhook_url,
        "username": body.username,
        "events": body.events,
    }
    return {"integration_id": intg_id, "type": body.type, "webhook_url": body.webhook_url}

# ---------------------------------------------------------------------------
# Chapter 12: GET /metrics
# ---------------------------------------------------------------------------

@app.get("/metrics")
async def get_metrics():
    active_count = sum(1 for a in alerts.values() if a["status"] == "active")
    return {
        "total_checks": metrics["total_checks"],
        "current_pool_size": len(proxies),
        "active_alerts": active_count,
        "total_alerts": len(alerts),
        "webhook_deliveries": metrics["webhook_deliveries"],
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)