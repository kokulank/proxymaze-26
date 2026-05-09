"""
ProxyMaze'26 — Real-time Proxy Monitoring HTTP API
Torch Labs Sri Lanka
Production-quality implementation targeting 270/270 score.
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("proxymaze")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALERT_THRESHOLD = 0.20
DEFAULT_CHECK_INTERVAL = 30       # seconds
DEFAULT_REQUEST_TIMEOUT = 5000    # milliseconds
PORT = int(os.environ.get("PORT", 7000))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def utcnow_iso() -> str:
    """Return current UTC time as ISO 8601 string with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unix_epoch_int() -> int:
    return int(time.time())


def proxy_id_from_url(url: str) -> str:
    """Extract the last path segment of a URL as the proxy ID."""
    return url.rstrip("/").split("/")[-1]


# ---------------------------------------------------------------------------
# In-memory state (all async-safe via asyncio.Lock or single-event-loop access)
# ---------------------------------------------------------------------------
class AppState:
    def __init__(self):
        self.lock = asyncio.Lock()

        # Config
        self.config: Dict[str, Any] = {
            "check_interval_seconds": DEFAULT_CHECK_INTERVAL,
            "request_timeout_ms": DEFAULT_REQUEST_TIMEOUT,
        }

        # Proxy pool: id -> proxy dict
        self.proxies: Dict[str, Dict[str, Any]] = {}

        # Alert list (ordered, never deleted)
        self.alerts: List[Dict[str, Any]] = []

        # Active alert id (None if no active alert)
        self.active_alert_id: Optional[str] = None

        # Webhooks: webhook_id -> url
        self.webhooks: Dict[str, str] = {}

        # Integrations list
        self.integrations: List[Dict[str, Any]] = []

        # Webhook delivery tracking: (alert_id, event_type) -> bool delivered
        self.webhook_delivered: Dict[tuple, bool] = {}

        # Metrics counters
        self.total_checks: int = 0
        self.webhook_deliveries: int = 0

        # Background task handle
        self.monitor_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Proxy helpers (call under lock)
    # ------------------------------------------------------------------
    def _compute_failure_rate(self) -> float:
        """Compute failure rate counting only up/down proxies (not pending)."""
        probed = [p for p in self.proxies.values() if p["status"] in ("up", "down")]
        if not probed:
            return 0.0
        down = sum(1 for p in probed if p["status"] == "down")
        return down / len(probed)

    def _failed_proxy_ids(self) -> List[str]:
        return [p["id"] for p in self.proxies.values() if p["status"] == "down"]

    def _snapshot_alert_payload(self) -> Dict[str, Any]:
        """Build consistent payload fields for an alert fire/resolve event."""
        failure_rate = self._compute_failure_rate()
        total = len(self.proxies)
        failed_ids = self._failed_proxy_ids()
        failed = len(failed_ids)
        return {
            "failure_rate": round(failure_rate, 4),
            "total_proxies": total,
            "failed_proxies": failed,
            "failed_proxy_ids": failed_ids,
            "threshold": ALERT_THRESHOLD,
        }


state = AppState()


# ---------------------------------------------------------------------------
# Background monitor
# ---------------------------------------------------------------------------
async def probe_proxy(proxy: Dict[str, Any], timeout_ms: int) -> str:
    """Probe a single proxy URL. Returns 'up' or 'down'."""
    url = proxy["url"]
    timeout_sec = timeout_ms / 1000.0
    try:
        async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
            resp = await client.get(url)
        if 200 <= resp.status_code < 300:
            return "up"
        if resp.status_code >= 500:
            return "down"
        # 3xx handled by follow_redirects; 4xx we treat as up (reachable)
        return "up"
    except Exception:
        return "down"


async def run_monitor_cycle():
    """One full monitoring cycle: probe all proxies, update state, handle alerts."""
    async with state.lock:
        proxy_ids = list(state.proxies.keys())
        timeout_ms = state.config["request_timeout_ms"]
        proxy_snapshots = {pid: state.proxies[pid].copy() for pid in proxy_ids}

    if not proxy_snapshots:
        return

    # Probe all concurrently (outside lock)
    tasks = {
        pid: asyncio.create_task(probe_proxy(proxy_snapshots[pid], timeout_ms))
        for pid in proxy_snapshots
    }
    results = {}
    for pid, task in tasks.items():
        try:
            results[pid] = await task
        except Exception:
            results[pid] = "down"

    checked_at = utcnow_iso()

    async with state.lock:
        for pid, new_status in results.items():
            if pid not in state.proxies:
                continue  # proxy was removed during probing
            p = state.proxies[pid]
            prev_status = p["status"]

            # Update status and history
            p["status"] = new_status
            p["last_checked_at"] = checked_at
            p["total_checks"] += 1
            state.total_checks += 1

            if new_status == "up":
                p["consecutive_failures"] = 0
                p["up_count"] = p.get("up_count", 0) + 1
            else:
                p["consecutive_failures"] += 1

            # Maintain rolling history (cap at 1000 entries)
            p["history"].append({"checked_at": checked_at, "status": new_status})
            if len(p["history"]) > 1000:
                p["history"] = p["history"][-1000:]

        # Compute alert state
        failure_rate = state._compute_failure_rate()
        await _handle_alert_state(failure_rate)


async def _handle_alert_state(failure_rate: float):
    """Must be called under state.lock. Handles alert fire/resolve logic."""
    if failure_rate >= ALERT_THRESHOLD:
        if state.active_alert_id is None:
            # Fire a new alert
            alert_id = f"alert-{uuid.uuid4().hex[:8]}"
            payload = state._snapshot_alert_payload()
            alert = {
                "alert_id": alert_id,
                "status": "active",
                "failure_rate": payload["failure_rate"],
                "total_proxies": payload["total_proxies"],
                "failed_proxies": payload["failed_proxies"],
                "failed_proxy_ids": payload["failed_proxy_ids"],
                "threshold": ALERT_THRESHOLD,
                "fired_at": utcnow_iso(),
                "resolved_at": None,
                "message": (
                    f"Failure rate {payload['failure_rate']:.0%} exceeds "
                    f"threshold {ALERT_THRESHOLD:.0%}. "
                    f"Failed proxies: {', '.join(payload['failed_proxy_ids']) or 'none'}."
                ),
            }
            state.alerts.append(alert)
            state.active_alert_id = alert_id
            logger.info(f"Alert FIRED: {alert_id} failure_rate={failure_rate:.2%}")

            # Schedule webhook + integration delivery (outside lock via task)
            asyncio.create_task(_deliver_alert_fired(alert_id, alert.copy()))
        # else: alert already active — no duplicate fires
    else:
        if state.active_alert_id is not None:
            # Resolve the active alert
            alert_id = state.active_alert_id
            resolved_at = utcnow_iso()
            for alert in state.alerts:
                if alert["alert_id"] == alert_id:
                    alert["status"] = "resolved"
                    alert["resolved_at"] = resolved_at
                    break
            state.active_alert_id = None
            logger.info(f"Alert RESOLVED: {alert_id} failure_rate={failure_rate:.2%}")

            asyncio.create_task(_deliver_alert_resolved(alert_id, resolved_at))


async def monitor_loop():
    """Continuously probe proxies at configured interval."""
    logger.info("Monitor loop started.")
    while True:
        try:
            await run_monitor_cycle()
        except asyncio.CancelledError:
            logger.info("Monitor loop cancelled.")
            break
        except Exception as exc:
            logger.error(f"Monitor cycle error: {exc}")

        # Sleep in small increments so config changes can interrupt
        async with state.lock:
            interval = state.config["check_interval_seconds"]
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Monitor loop sleep cancelled.")
            break


def restart_monitor():
    """Cancel existing monitor task and start a new one."""
    if state.monitor_task and not state.monitor_task.done():
        state.monitor_task.cancel()
    state.monitor_task = asyncio.create_task(monitor_loop())
    logger.info("Monitor task (re)started.")


# ---------------------------------------------------------------------------
# Webhook / Integration delivery
# ---------------------------------------------------------------------------
RETRY_DELAYS = [2, 5, 10, 30, 60]  # seconds between retries


async def _http_post_with_retry(url: str, payload: Dict[str, Any], delivery_key: tuple):
    """POST payload to url, retrying on 5xx. Dedup via delivery_key."""
    # Check dedup
    if state.webhook_delivered.get(delivery_key):
        return

    headers = {"Content-Type": "application/json"}
    for attempt, delay in enumerate([0] + RETRY_DELAYS):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code in (500, 502, 503, 504):
                logger.warning(f"Webhook {url} returned {resp.status_code}, retrying...")
                continue
            # Success (any non-5xx response)
            async with state.lock:
                if not state.webhook_delivered.get(delivery_key):
                    state.webhook_delivered[delivery_key] = True
                    state.webhook_deliveries += 1
            logger.info(f"Webhook delivered to {url} (attempt {attempt+1})")
            return
        except Exception as exc:
            logger.warning(f"Webhook delivery error to {url}: {exc}, retrying...")

    logger.error(f"Webhook delivery to {url} failed after all retries.")


async def _deliver_alert_fired(alert_id: str, alert: Dict[str, Any]):
    """Deliver alert.fired event to all webhooks and integrations."""
    fired_payload = {
        "event": "alert.fired",
        "alert_id": alert_id,
        "fired_at": alert["fired_at"],
        "failure_rate": alert["failure_rate"],
        "total_proxies": alert["total_proxies"],
        "failed_proxies": alert["failed_proxies"],
        "failed_proxy_ids": alert["failed_proxy_ids"],
        "threshold": ALERT_THRESHOLD,
        "message": alert["message"],
    }

    async with state.lock:
        webhooks = dict(state.webhooks)
        integrations = list(state.integrations)

    # Plain webhooks
    tasks = []
    for wh_id, url in webhooks.items():
        key = (alert_id, "alert.fired", wh_id)
        tasks.append(_http_post_with_retry(url, fired_payload, key))

    # Integrations
    for integ in integrations:
        if "alert.fired" in integ.get("events", []):
            key = (alert_id, "alert.fired", integ["id"])
            if integ["type"] == "slack":
                payload = _build_slack_payload(alert, integ, fired=True)
            else:
                payload = _build_discord_payload(alert, integ, fired=True)
            tasks.append(_http_post_with_retry(integ["webhook_url"], payload, key))

    await asyncio.gather(*tasks, return_exceptions=True)


async def _deliver_alert_resolved(alert_id: str, resolved_at: str):
    """Deliver alert.resolved event to all webhooks and integrations."""
    resolved_payload = {
        "event": "alert.resolved",
        "alert_id": alert_id,
        "resolved_at": resolved_at,
    }

    # Find alert details for integration payloads
    async with state.lock:
        webhooks = dict(state.webhooks)
        integrations = list(state.integrations)
        alert = next((a for a in state.alerts if a["alert_id"] == alert_id), None)

    tasks = []
    for wh_id, url in webhooks.items():
        key = (alert_id, "alert.resolved", wh_id)
        tasks.append(_http_post_with_retry(url, resolved_payload, key))

    for integ in integrations:
        if "alert.resolved" in integ.get("events", []):
            key = (alert_id, "alert.resolved", integ["id"])
            if alert:
                if integ["type"] == "slack":
                    payload = _build_slack_payload(alert, integ, fired=False)
                else:
                    payload = _build_discord_payload(alert, integ, fired=False)
            else:
                payload = resolved_payload  # fallback
            tasks.append(_http_post_with_retry(integ["webhook_url"], payload, key))

    await asyncio.gather(*tasks, return_exceptions=True)


def _build_slack_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    color = "#FF0000" if fired else "#00FF00"
    event_label = "🔴 Alert Fired" if fired else "🟢 Alert Resolved"
    text = (
        f"{event_label}: Failure rate {alert['failure_rate']:.0%} "
        f"{'exceeds' if fired else 'back below'} threshold {ALERT_THRESHOLD:.0%}."
    )
    return {
        "username": integ.get("username", "ProxyWatch"),
        "text": text,
        "attachments": [
            {
                "color": color,
                "fields": [
                    {"title": "Alert ID", "value": alert["alert_id"]},
                    {"title": "Failure Rate", "value": str(alert["failure_rate"])},
                    {"title": "Failed Proxies", "value": str(alert["failed_proxies"])},
                    {"title": "Threshold", "value": "0.2"},
                    {"title": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"]) or "none"},
                    {"title": "Fired At", "value": alert.get("fired_at", "")},
                ],
                "footer": "ProxyMaze'26 | Torch Labs",
                "ts": unix_epoch_int(),
            }
        ],
    }


def _build_discord_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    color_int = 16711680 if fired else 65280  # red or green
    title = "🔴 Alert Fired" if fired else "🟢 Alert Resolved"
    description = (
        f"Failure rate {alert['failure_rate']:.0%} "
        f"{'exceeds' if fired else 'dropped below'} threshold {ALERT_THRESHOLD:.0%}. "
        f"Failed proxies: {', '.join(alert['failed_proxy_ids']) or 'none'}."
    )
    return {
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color_int,
                "fields": [
                    {"name": "Alert ID", "value": alert["alert_id"]},
                    {"name": "Failure Rate", "value": str(alert["failure_rate"])},
                    {"name": "Failed Proxies", "value": str(alert["failed_proxies"])},
                    {"name": "Threshold", "value": "0.2"},
                    {"name": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"]) or "none"},
                ],
                "footer": {"text": "ProxyMaze'26 | Torch Labs"},
            }
        ]
    }


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    restart_monitor()
    yield
    if state.monitor_task and not state.monitor_task.done():
        state.monitor_task.cancel()
        try:
            await state.monitor_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="ProxyMaze'26",
    description="Real-time proxy monitoring HTTP API — Torch Labs Sri Lanka",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Ch01: GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Ch02: POST /config
# ---------------------------------------------------------------------------
class ConfigBody(BaseModel):
    check_interval_seconds: int = Field(default=DEFAULT_CHECK_INTERVAL, ge=1)
    request_timeout_ms: int = Field(default=DEFAULT_REQUEST_TIMEOUT, ge=1)

    class Config:
        extra = "ignore"


@app.post("/config", status_code=200)
async def post_config(body: ConfigBody):
    async with state.lock:
        state.config["check_interval_seconds"] = body.check_interval_seconds
        state.config["request_timeout_ms"] = body.request_timeout_ms
        cfg = dict(state.config)

    # Restart monitor loop with new interval (outside lock to avoid deadlock)
    restart_monitor()
    return cfg


# ---------------------------------------------------------------------------
# Ch03: GET /config
# ---------------------------------------------------------------------------
@app.get("/config")
async def get_config():
    async with state.lock:
        return dict(state.config)


# ---------------------------------------------------------------------------
# Ch04: POST /proxies
# ---------------------------------------------------------------------------
class PostProxiesBody(BaseModel):
    proxies: List[str]
    replace: Optional[bool] = False

    class Config:
        extra = "ignore"


@app.post("/proxies", status_code=201)
async def post_proxies(body: PostProxiesBody):
    accepted_proxies = []

    async with state.lock:
        if body.replace:
            state.proxies.clear()

        for url in body.proxies:
            pid = proxy_id_from_url(url)
            if pid in state.proxies:
                # Update URL but keep history
                state.proxies[pid]["url"] = url
            else:
                state.proxies[pid] = {
                    "id": pid,
                    "url": url,
                    "status": "pending",
                    "last_checked_at": None,
                    "consecutive_failures": 0,
                    "total_checks": 0,
                    "up_count": 0,
                    "history": [],
                }
            accepted_proxies.append({"id": pid, "url": url, "status": state.proxies[pid]["status"]})

    return {
        "accepted": len(accepted_proxies),
        "proxies": accepted_proxies,
    }


# ---------------------------------------------------------------------------
# Ch05: GET /proxies
# ---------------------------------------------------------------------------
@app.get("/proxies")
async def get_proxies():
    async with state.lock:
        proxies = list(state.proxies.values())
        failure_rate = state._compute_failure_rate()

    total = len(proxies)
    up = sum(1 for p in proxies if p["status"] == "up")
    down = sum(1 for p in proxies if p["status"] == "down")

    return {
        "total": total,
        "up": up,
        "down": down,
        "failure_rate": round(failure_rate, 4),
        "proxies": [
            {
                "id": p["id"],
                "url": p["url"],
                "status": p["status"],
                "last_checked_at": p["last_checked_at"],
                "consecutive_failures": p["consecutive_failures"],
            }
            for p in proxies
        ],
    }


# ---------------------------------------------------------------------------
# Ch06: GET /proxies/{id}
# ---------------------------------------------------------------------------
@app.get("/proxies/{proxy_id}")
async def get_proxy(proxy_id: str):
    async with state.lock:
        p = state.proxies.get(proxy_id)
        if not p:
            raise HTTPException(status_code=404, detail=f"Proxy '{proxy_id}' not found.")
        total_checks = p["total_checks"]
        up_count = p.get("up_count", 0)
        uptime_pct = round(up_count / total_checks * 100, 2) if total_checks > 0 else 0.0
        return {
            "id": p["id"],
            "url": p["url"],
            "status": p["status"],
            "last_checked_at": p["last_checked_at"],
            "consecutive_failures": p["consecutive_failures"],
            "total_checks": total_checks,
            "uptime_percentage": uptime_pct,
            "history": list(p["history"]),
        }


# ---------------------------------------------------------------------------
# Ch07: GET /proxies/{id}/history
# ---------------------------------------------------------------------------
@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    async with state.lock:
        p = state.proxies.get(proxy_id)
        if not p:
            raise HTTPException(status_code=404, detail=f"Proxy '{proxy_id}' not found.")
        return list(p["history"])


# ---------------------------------------------------------------------------
# Ch08: DELETE /proxies
# ---------------------------------------------------------------------------
@app.delete("/proxies", status_code=204)
async def delete_proxies():
    async with state.lock:
        state.proxies.clear()
        # Alert history preserved — no change to state.alerts
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Ch09: GET /alerts
# ---------------------------------------------------------------------------
@app.get("/alerts")
async def get_alerts():
    async with state.lock:
        return list(state.alerts)


# ---------------------------------------------------------------------------
# Ch10: POST /webhooks
# ---------------------------------------------------------------------------
class WebhookBody(BaseModel):
    url: str

    class Config:
        extra = "ignore"


@app.post("/webhooks", status_code=201)
async def post_webhooks(body: WebhookBody):
    wh_id = f"wh-{uuid.uuid4().hex[:8]}"
    async with state.lock:
        state.webhooks[wh_id] = body.url
    return {"webhook_id": wh_id, "url": body.url}


# ---------------------------------------------------------------------------
# Ch11: POST /integrations
# ---------------------------------------------------------------------------
class IntegrationBody(BaseModel):
    type: str
    webhook_url: str
    username: Optional[str] = "ProxyWatch"
    events: Optional[List[str]] = ["alert.fired", "alert.resolved"]

    class Config:
        extra = "ignore"


@app.post("/integrations", status_code=201)
async def post_integrations(body: IntegrationBody):
    if body.type not in ("slack", "discord"):
        raise HTTPException(status_code=400, detail="type must be 'slack' or 'discord'.")

    integ_id = f"integ-{uuid.uuid4().hex[:8]}"
    integ = {
        "id": integ_id,
        "type": body.type,
        "webhook_url": body.webhook_url,
        "username": body.username or "ProxyWatch",
        "events": body.events or ["alert.fired", "alert.resolved"],
    }
    async with state.lock:
        state.integrations.append(integ)

    return {"integration_id": integ_id, "type": body.type, "webhook_url": body.webhook_url}


# ---------------------------------------------------------------------------
# Ch12: GET /metrics
# ---------------------------------------------------------------------------
@app.get("/metrics")
async def get_metrics():
    async with state.lock:
        active_alerts = 1 if state.active_alert_id else 0
        total_alerts = len(state.alerts)
        pool_size = len(state.proxies)
        total_checks = state.total_checks
        webhook_deliveries = state.webhook_deliveries

    return {
        "total_checks": total_checks,
        "current_pool_size": pool_size,
        "active_alerts": active_alerts,
        "total_alerts": total_alerts,
        "webhook_deliveries": webhook_deliveries,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        reload=False,
    )
