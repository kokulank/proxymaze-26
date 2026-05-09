"""
ProxyMaze'26 - Complete Implementation targeting 270/270
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
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global State
# ---------------------------------------------------------------------------

config = {
    "check_interval_seconds": 60,
    "request_timeout_ms": 5000,
}

proxies: dict[str, dict] = {}
alerts: dict[str, dict] = {}
webhooks: dict[str, dict] = {}
integrations: dict[str, dict] = {}

metrics_state = {
    "total_checks": 0,
    "webhook_deliveries": 0,
}

active_alert_id: Optional[str] = None
alert_lock = asyncio.Lock()

THRESHOLD = 0.20

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def now_epoch() -> int:
    return int(time.time())

def extract_proxy_id(url: str) -> str:
    return url.rstrip("/").split("/")[-1]

# ---------------------------------------------------------------------------
# HTTP Probe
# ---------------------------------------------------------------------------

async def probe_proxy(proxy: dict) -> bool:
    """
    True  = up   (2xx within timeout)
    False = down (timeout, connection error, 5xx)
    """
    url = proxy["url"]
    timeout_s = config["request_timeout_ms"] / 1000.0
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=timeout_s, read=timeout_s, write=timeout_s, pool=timeout_s),
            follow_redirects=True,
            verify=False,
        ) as client:
            resp = await client.get(url)
            if 200 <= resp.status_code < 300:
                return True
            if resp.status_code >= 500:
                return False
            # 4xx = server reachable; treat as up (spec only says 2xx=up, 5xx=down, timeout=down)
            return True
    except (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.NetworkError,
        httpx.RemoteProtocolError,
        Exception,
    ):
        return False

# ---------------------------------------------------------------------------
# Webhook / Integration Delivery
# ---------------------------------------------------------------------------

async def deliver_once(url: str, payload: dict) -> bool:
    """One delivery attempt. Returns True on non-5xx response."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0), verify=False) as client:
            resp = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code in (500, 502, 503, 504):
                logger.warning(f"Receiver {url} returned {resp.status_code}, will retry")
                return False
            logger.info(f"Delivered to {url}, status={resp.status_code}")
            return True
    except Exception as e:
        logger.warning(f"Delivery network error to {url}: {e}")
        return False


async def deliver_with_retry(url: str, payload: dict):
    """Retry on 5xx/network error with exponential backoff. Exactly one success."""
    for attempt in range(25):
        if await deliver_once(url, payload):
            metrics_state["webhook_deliveries"] += 1
            return
        wait = min(2 ** attempt, 60)
        logger.warning(f"Retry {attempt + 1} for {url} in {wait}s")
        await asyncio.sleep(wait)
    logger.error(f"Giving up on {url} after 25 attempts")


async def broadcast_webhooks(payload: dict):
    """Non-blocking delivery to all registered webhooks."""
    for wh in list(webhooks.values()):
        asyncio.create_task(deliver_with_retry(wh["url"], payload))


# ---------------------------------------------------------------------------
# Slack / Discord Payloads
# ---------------------------------------------------------------------------

def build_slack_payload(intg: dict, alert: dict, event_type: str) -> dict:
    fired_at = alert.get("fired_at", now_iso())
    try:
        ts_int = int(
            datetime.strptime(fired_at, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
    except Exception:
        ts_int = now_epoch()

    failed_ids_str = ", ".join(alert.get("failed_proxy_ids", [])) or "none"

    if event_type == "alert.fired":
        color = "#FF0000"
        text = ":red_circle: *ALERT FIRED* — Proxy pool failure rate exceeded threshold"
        fields = [
            {"title": "Alert ID",       "value": alert["alert_id"],             "short": True},
            {"title": "Failure Rate",   "value": str(alert["failure_rate"]),    "short": True},
            {"title": "Failed Proxies", "value": str(alert["failed_proxies"]),  "short": True},
            {"title": "Threshold",      "value": str(alert["threshold"]),       "short": True},
            {"title": "Failed IDs",     "value": failed_ids_str,                "short": False},
            {"title": "Fired At",       "value": fired_at,                      "short": True},
        ]
    else:
        color = "#36A64F"
        resolved_at = alert.get("resolved_at", now_iso())
        try:
            ts_int = int(
                datetime.strptime(resolved_at, "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
        except Exception:
            ts_int = now_epoch()
        text = ":large_green_circle: *ALERT RESOLVED* — Proxy pool failure rate dropped below threshold"
        fields = [
            {"title": "Alert ID",       "value": alert["alert_id"],             "short": True},
            {"title": "Failure Rate",   "value": str(alert["failure_rate"]),    "short": True},
            {"title": "Failed Proxies", "value": str(alert["failed_proxies"]),  "short": True},
            {"title": "Threshold",      "value": str(alert["threshold"]),       "short": True},
            {"title": "Failed IDs",     "value": failed_ids_str,                "short": False},
            {"title": "Fired At",       "value": fired_at,                      "short": True},
        ]

    return {
        "username": intg.get("username", "ProxyWatch"),
        "text": text,
        "attachments": [
            {
                "color": color,
                "fields": fields,
                "footer": "ProxyMaze'26 — Torch Labs",
                "ts": ts_int,
            }
        ],
    }


def build_discord_payload(intg: dict, alert: dict, event_type: str) -> dict:
    fired_at = alert.get("fired_at", now_iso())
    failed_ids_str = ", ".join(alert.get("failed_proxy_ids", [])) or "none"

    if event_type == "alert.fired":
        color = 16711680  # Red
        title = "🚨 Alert Fired — Proxy Pool Threshold Breached"
        description = (
            f"Pool failure rate **{alert['failure_rate']}** has exceeded "
            f"threshold **{alert['threshold']}**."
        )
        fields = [
            {"name": "Alert ID",       "value": alert["alert_id"],            "inline": True},
            {"name": "Failure Rate",   "value": str(alert["failure_rate"]),   "inline": True},
            {"name": "Failed Proxies", "value": str(alert["failed_proxies"]), "inline": True},
            {"name": "Threshold",      "value": str(alert["threshold"]),      "inline": True},
            {"name": "Failed IDs",     "value": failed_ids_str,               "inline": False},
            {"name": "Fired At",       "value": fired_at,                     "inline": True},
        ]
    else:
        color = 3329330   # Green
        resolved_at = alert.get("resolved_at", now_iso())
        title = "✅ Alert Resolved — Pool Recovered"
        description = (
            f"Pool failure rate has dropped below threshold **{alert['threshold']}**. "
            f"Resolved at {resolved_at}."
        )
        fields = [
            {"name": "Alert ID",       "value": alert["alert_id"],            "inline": True},
            {"name": "Failure Rate",   "value": str(alert["failure_rate"]),   "inline": True},
            {"name": "Failed Proxies", "value": str(alert["failed_proxies"]), "inline": True},
            {"name": "Threshold",      "value": str(alert["threshold"]),      "inline": True},
            {"name": "Failed IDs",     "value": failed_ids_str,               "inline": False},
        ]

    return {
        "username": intg.get("username", "ProxyWatch"),
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields,
                "footer": {"text": "ProxyMaze'26 — Torch Labs"},
            }
        ],
    }


async def broadcast_integrations(alert: dict, event_type: str):
    for intg in list(integrations.values()):
        if event_type not in intg.get("events", []):
            continue
        itype = intg.get("type", "")
        if itype == "slack":
            payload = build_slack_payload(intg, alert, event_type)
        elif itype == "discord":
            payload = build_discord_payload(intg, alert, event_type)
        else:
            continue
        asyncio.create_task(deliver_with_retry(intg["webhook_url"], payload))


# ---------------------------------------------------------------------------
# Alert State Machine
# ---------------------------------------------------------------------------

async def fire_alert(total: int, down_count: int, down_ids: list[str], failure_rate: float):
    global active_alert_id
    alert_id = f"alert-{uuid.uuid4().hex[:8]}"
    fired_at = now_iso()
    alert = {
        "alert_id": alert_id,
        "status": "active",
        "failure_rate": round(failure_rate, 4),
        "total_proxies": total,
        "failed_proxies": down_count,
        "failed_proxy_ids": list(down_ids),
        "threshold": THRESHOLD,
        "fired_at": fired_at,
        "resolved_at": None,
        "message": "Proxy pool failure rate exceeded threshold",
    }
    alerts[alert_id] = alert
    active_alert_id = alert_id
    logger.info(f"ALERT FIRED: {alert_id}, rate={failure_rate:.4f}")

    webhook_payload = {
        "event": "alert.fired",
        "alert_id": alert_id,
        "fired_at": fired_at,
        "failure_rate": alert["failure_rate"],
        "total_proxies": total,
        "failed_proxies": down_count,
        "failed_proxy_ids": list(down_ids),
        "threshold": THRESHOLD,
        "message": "Proxy pool failure rate exceeded threshold",
    }
    await broadcast_webhooks(webhook_payload)
    await broadcast_integrations(alert, "alert.fired")


async def resolve_current_alert():
    global active_alert_id
    if active_alert_id is None:
        return
    alert_id = active_alert_id
    if alert_id not in alerts:
        active_alert_id = None
        return
    alert = alerts[alert_id]
    resolved_at = now_iso()
    alert["status"] = "resolved"
    alert["resolved_at"] = resolved_at
    active_alert_id = None
    logger.info(f"ALERT RESOLVED: {alert_id}")

    webhook_payload = {
        "event": "alert.resolved",
        "alert_id": alert_id,
        "resolved_at": resolved_at,
    }
    await broadcast_webhooks(webhook_payload)
    await broadcast_integrations(alert, "alert.resolved")


async def evaluate_alert():
    """
    Enforce alert lifecycle. Called after every check cycle.
    At most ONE active alert at any time.
    Continuous breach: update existing alert, DO NOT fire again.
    """
    global active_alert_id

    async with alert_lock:
        total = len(proxies)
        if total == 0:
            return

        down_ids = [pid for pid, p in proxies.items() if p["status"] == "down"]
        down_count = len(down_ids)
        failure_rate = down_count / total

        logger.info(
            f"Evaluate: total={total}, down={down_count}, "
            f"rate={failure_rate:.4f}, active={active_alert_id}"
        )

        if failure_rate >= THRESHOLD:
            if active_alert_id is None:
                # Breach with no active alert → fire new alert
                await fire_alert(total, down_count, down_ids, failure_rate)
            else:
                # Already have an active alert → just update its data, no new webhook
                alert = alerts[active_alert_id]
                alert["failed_proxy_ids"] = list(down_ids)
                alert["failed_proxies"] = down_count
                alert["failure_rate"] = round(failure_rate, 4)
                alert["total_proxies"] = total
        else:
            if active_alert_id is not None:
                # Rate dropped below threshold → resolve
                await resolve_current_alert()


# ---------------------------------------------------------------------------
# Check Cycle
# ---------------------------------------------------------------------------

async def run_check_cycle():
    """Run one full health-check cycle for all current proxies."""
    if not proxies:
        return

    checked_at = now_iso()
    pid_list = list(proxies.keys())

    # Probe all concurrently
    tasks = {}
    for pid in pid_list:
        if pid in proxies:
            tasks[pid] = asyncio.create_task(probe_proxy(proxies[pid]))

    for pid, task in tasks.items():
        if pid not in proxies:
            continue
        try:
            is_up = await task
        except Exception:
            is_up = False

        p = proxies[pid]
        new_status = "up" if is_up else "down"
        p["status"] = new_status
        p["last_checked_at"] = checked_at

        if new_status == "down":
            p["consecutive_failures"] = p.get("consecutive_failures", 0) + 1
        else:
            p["consecutive_failures"] = 0

        p["total_checks"] = p.get("total_checks", 0) + 1
        p["up_checks"] = p.get("up_checks", 0) + (1 if is_up else 0)
        p["uptime_percentage"] = round((p["up_checks"] / p["total_checks"]) * 100, 1)
        p["history"].append({"checked_at": checked_at, "status": new_status})

        metrics_state["total_checks"] += 1

    await evaluate_alert()


# ---------------------------------------------------------------------------
# Background Monitoring Loop
# ---------------------------------------------------------------------------

async def monitoring_loop():
    logger.info("Background monitoring loop started")
    while True:
        try:
            interval = config["check_interval_seconds"]
            await asyncio.sleep(interval)
            logger.info(f"Scheduled check cycle (interval={interval}s)")
            await run_check_cycle()
        except asyncio.CancelledError:
            logger.info("Monitoring loop cancelled")
            break
        except Exception as e:
            logger.error(f"Monitoring loop exception: {e}", exc_info=True)
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# App Lifespan
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
# Routes
# ---------------------------------------------------------------------------

# Ch01
@app.get("/health")
async def health():
    return {"status": "ok"}


# Ch02
class ConfigRequest(BaseModel):
    check_interval_seconds: int
    request_timeout_ms: int
    model_config = {"extra": "allow"}

@app.post("/config")
async def set_config(body: ConfigRequest):
    config["check_interval_seconds"] = body.check_interval_seconds
    config["request_timeout_ms"] = body.request_timeout_ms
    return {
        "check_interval_seconds": body.check_interval_seconds,
        "request_timeout_ms": body.request_timeout_ms,
    }


# Ch03
@app.get("/config")
async def get_config():
    return {
        "check_interval_seconds": config["check_interval_seconds"],
        "request_timeout_ms": config["request_timeout_ms"],
    }


# Ch04
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

    # Immediate check (background)
    asyncio.create_task(run_check_cycle())

    return {
        "accepted": len(accepted),
        "proxies": [
            {"id": p["id"], "url": p["url"], "status": p["status"]}
            for p in accepted
        ],
    }


# Ch05
@app.get("/proxies")
async def list_proxies():
    total = len(proxies)
    up = sum(1 for p in proxies.values() if p["status"] == "up")
    down = sum(1 for p in proxies.values() if p["status"] == "down")
    failure_rate = round(down / total, 4) if total > 0 else 0.0
    return {
        "total": total,
        "up": up,
        "down": down,
        "failure_rate": failure_rate,
        "proxies": [
            {
                "id": p["id"],
                "url": p["url"],
                "status": p["status"],
                "last_checked_at": p["last_checked_at"],
                "consecutive_failures": p["consecutive_failures"],
            }
            for p in proxies.values()
        ],
    }


# Ch06 — must come BEFORE /proxies/{proxy_id}/history to avoid route conflict
@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    if proxy_id not in proxies:
        raise HTTPException(status_code=404, detail="Proxy not found")
    return proxies[proxy_id]["history"]


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


# Ch08
@app.delete("/proxies", status_code=204)
async def delete_proxies():
    proxies.clear()
    return None


# Ch09
@app.get("/alerts")
async def list_alerts():
    return list(alerts.values())


# Ch10
class WebhookRequest(BaseModel):
    url: str
    model_config = {"extra": "allow"}

@app.post("/webhooks", status_code=201)
async def register_webhook(body: WebhookRequest):
    wh_id = f"wh-{uuid.uuid4().hex[:8]}"
    webhooks[wh_id] = {"webhook_id": wh_id, "url": body.url}
    logger.info(f"Webhook registered: {wh_id} -> {body.url}")
    return {"webhook_id": wh_id, "url": body.url}


# Ch11
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
        "username": body.username or "ProxyWatch",
        "events": body.events or ["alert.fired", "alert.resolved"],
    }
    logger.info(f"Integration registered: {intg_id} type={body.type}")
    return {
        "integration_id": intg_id,
        "type": body.type,
        "webhook_url": body.webhook_url,
    }


# Ch12
@app.get("/metrics")
async def get_metrics():
    active_count = sum(1 for a in alerts.values() if a["status"] == "active")
    return {
        "total_checks": metrics_state["total_checks"],
        "current_pool_size": len(proxies),
        "active_alerts": active_count,
        "total_alerts": len(alerts),
        "webhook_deliveries": metrics_state["webhook_deliveries"],
    }


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)