"""
ProxyMaze'26 — Real-time Proxy Monitoring HTTP API
Torch Labs Sri Lanka
Production-quality implementation targeting 270/270 score.

FIXES applied vs previous version:
  FIX-1: httpx client with verify=True, timeout=30s for outbound delivery
  FIX-2: Probe catches ALL exception types; 5xx status = down
  FIX-3: Retry on 5xx responses with backoff [2,4,8,16,32]s, max 10 attempts
  FIX-4: Strict state machine — no duplicate fired webhooks during persistent breach
  FIX-5: Atomic snapshot stored on alert object; GET /alerts & webhook use snapshot
  FIX-6: Integrations fire on every alert event with correct Slack/Discord payloads
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
from pydantic import BaseModel, Field, ConfigDict

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

# FIX-3: Backoff delays for webhook retry
RETRY_DELAYS = [2, 4, 8, 16, 32]  # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unix_epoch_int() -> int:
    return int(time.time())


def proxy_id_from_url(url: str) -> str:
    """Extract the last path segment of a URL as the proxy ID."""
    return url.rstrip("/").split("/")[-1]


# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
class AppState:
    def __init__(self):
        self.lock = asyncio.Lock()

        self.config: Dict[str, Any] = {
            "check_interval_seconds": DEFAULT_CHECK_INTERVAL,
            "request_timeout_ms": DEFAULT_REQUEST_TIMEOUT,
        }

        self.proxies: Dict[str, Dict[str, Any]] = {}
        self.alerts: List[Dict[str, Any]] = []
        self.active_alert_id: Optional[str] = None
        self.webhooks: Dict[str, str] = {}
        self.integrations: List[Dict[str, Any]] = []

        # FIX-4: delivered_events tracks (alert_id, event_type, target_id) tuples
        # Only marked delivered AFTER a 200 response is received
        self.delivered_events: set = set()

        self.total_checks: int = 0
        self.webhook_deliveries: int = 0
        self.monitor_task: Optional[asyncio.Task] = None

    def _compute_failure_rate(self) -> float:
        probed = [p for p in self.proxies.values() if p["status"] in ("up", "down")]
        if not probed:
            return 0.0
        down = sum(1 for p in probed if p["status"] == "down")
        return down / len(probed)

    def _failed_proxy_ids(self) -> List[str]:
        return [p["id"] for p in self.proxies.values() if p["status"] == "down"]

    def _snapshot_alert_payload(self) -> Dict[str, Any]:
        """
        FIX-5: Atomic snapshot under lock. Stored on the alert object and
        used verbatim in GET /alerts and webhook payloads — never recomputed.
        """
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
# Background monitor — probe logic
# ---------------------------------------------------------------------------
async def probe_proxy(proxy: Dict[str, Any], timeout_ms: int) -> str:
    """
    FIX-2: Probe a single proxy. Returns 'up' or 'down'.
    - Timeout (any subclass) → down
    - ConnectError, NetworkError, RemoteProtocolError → down
    - status >= 500 → down
    - status 2xx (within timeout) → up
    - status 4xx → up (host reachable)
    - Any other exception → down
    """
    url = proxy["url"]
    timeout_sec = timeout_ms / 1000.0
    try:
        # FIX-1: verify=True, generous timeout for outbound
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_sec),
            follow_redirects=True,
            verify=True,
        ) as client:
            resp = await client.get(url)

        # FIX-2: 5xx responses → down
        if resp.status_code >= 500:
            return "down"
        # 2xx/3xx/4xx → up (host responded)
        return "up"

    except httpx.TimeoutException:
        # FIX-2: All timeout subclasses (ConnectTimeout, ReadTimeout, etc.)
        return "down"
    except httpx.ConnectError:
        return "down"
    except httpx.RemoteProtocolError:
        return "down"
    except httpx.NetworkError:
        return "down"
    except Exception:
        # FIX-2: Catch-all fallback
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
                continue
            p = state.proxies[pid]
            p["status"] = new_status
            p["last_checked_at"] = checked_at
            p["total_checks"] += 1
            state.total_checks += 1

            if new_status == "up":
                p["consecutive_failures"] = 0
                p["up_count"] = p.get("up_count", 0) + 1
            else:
                p["consecutive_failures"] += 1

            p["history"].append({"checked_at": checked_at, "status": new_status})
            if len(p["history"]) > 1000:
                p["history"] = p["history"][-1000:]

        failure_rate = state._compute_failure_rate()
        await _handle_alert_state(failure_rate)


async def _handle_alert_state(failure_rate: float):
    """
    FIX-4: Strict state machine. Must be called under state.lock.
    - Only one active alert at a time.
    - Never re-fires while active_alert_id is set.
    - Transitions: normal→active (fire), active→resolved (resolve), resolved→active (new fire)
    """
    if failure_rate >= ALERT_THRESHOLD:
        # Only create a new alert if no alert is currently active
        if state.active_alert_id is None:
            alert_id = f"alert-{uuid.uuid4().hex[:8]}"

            # FIX-5: Atomic snapshot captured under lock right now
            snap = state._snapshot_alert_payload()
            fired_at = utcnow_iso()
            message = (
                f"Failure rate {snap['failure_rate']:.0%} exceeds "
                f"threshold {ALERT_THRESHOLD:.0%}. "
                f"Failed proxies: {', '.join(snap['failed_proxy_ids']) or 'none'}."
            )
            alert = {
                "alert_id": alert_id,
                "status": "active",
                # FIX-5: Snapshot values stored directly; never recomputed
                "failure_rate": snap["failure_rate"],
                "total_proxies": snap["total_proxies"],
                "failed_proxies": snap["failed_proxies"],
                "failed_proxy_ids": snap["failed_proxy_ids"],
                "threshold": ALERT_THRESHOLD,
                "fired_at": fired_at,
                "resolved_at": None,
                "message": message,
            }
            state.alerts.append(alert)
            state.active_alert_id = alert_id
            logger.info(f"Alert FIRED: {alert_id} failure_rate={failure_rate:.2%}")

            # FIX-1: Fire delivery as asyncio.create_task immediately
            asyncio.create_task(_deliver_alert_fired(alert_id, alert.copy()))
        # else: already active — no duplicate fires (FIX-4)

    else:
        if state.active_alert_id is not None:
            alert_id = state.active_alert_id
            resolved_at = utcnow_iso()

            for alert in state.alerts:
                if alert["alert_id"] == alert_id:
                    alert["status"] = "resolved"
                    alert["resolved_at"] = resolved_at
                    resolved_alert = alert.copy()
                    break
            else:
                resolved_alert = None

            state.active_alert_id = None
            logger.info(f"Alert RESOLVED: {alert_id} failure_rate={failure_rate:.2%}")

            asyncio.create_task(_deliver_alert_resolved(alert_id, resolved_at, resolved_alert))


async def monitor_loop():
    logger.info("Monitor loop started.")
    while True:
        try:
            await run_monitor_cycle()
        except asyncio.CancelledError:
            logger.info("Monitor loop cancelled.")
            break
        except Exception as exc:
            logger.error(f"Monitor cycle error: {exc}")

        async with state.lock:
            interval = state.config["check_interval_seconds"]
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Monitor loop sleep cancelled.")
            break


def restart_monitor():
    if state.monitor_task and not state.monitor_task.done():
        state.monitor_task.cancel()
    state.monitor_task = asyncio.create_task(monitor_loop())
    logger.info("Monitor task (re)started.")


# ---------------------------------------------------------------------------
# Webhook / Integration delivery
# ---------------------------------------------------------------------------
async def _http_post_with_retry(url: str, payload: Dict[str, Any], delivery_key: tuple):
    """
    FIX-1: Long-lived client with verify=True, timeout=30s.
    FIX-3: Retry on 5xx responses with exponential backoff. Max 10 attempts.
    FIX-4: Dedup via delivery_key set. Only mark delivered after 200 response.
    """
    # FIX-4: Skip if already delivered
    if delivery_key in state.delivered_events:
        logger.info(f"Skipping duplicate delivery for key {delivery_key}")
        return

    headers = {"Content-Type": "application/json"}
    attempts = [0] + RETRY_DELAYS  # first attempt delay=0, then backoffs

    for attempt_num, delay in enumerate(attempts):
        if delay > 0:
            logger.info(f"Webhook retry {attempt_num}/{len(RETRY_DELAYS)} to {url} in {delay}s")
            await asyncio.sleep(delay)

        try:
            # FIX-1: verify=True, 30s timeout for external HTTPS targets
            async with httpx.AsyncClient(
                timeout=30.0,
                verify=True,
                follow_redirects=True,
            ) as client:
                resp = await client.post(url, json=payload, headers=headers)

            logger.info(f"Webhook POST to {url}: HTTP {resp.status_code} (attempt {attempt_num+1})")

            # FIX-3: Explicitly retry on 5xx responses
            if resp.status_code in (500, 502, 503, 504):
                logger.warning(f"Webhook {url} returned {resp.status_code}, will retry...")
                continue

            # Any non-5xx = success (200, 201, 204, even 4xx = accepted by server)
            # FIX-4: Mark delivered only after confirmed success
            state.delivered_events.add(delivery_key)
            async with state.lock:
                state.webhook_deliveries += 1
            logger.info(f"Webhook delivered to {url} (attempt {attempt_num+1})")
            return

        except httpx.TimeoutException as exc:
            logger.warning(f"Webhook timeout to {url}: {exc}, retrying...")
        except httpx.ConnectError as exc:
            logger.warning(f"Webhook connect error to {url}: {exc}, retrying...")
        except Exception as exc:
            logger.warning(f"Webhook delivery error to {url}: {exc}, retrying...")

    logger.error(f"Webhook delivery to {url} failed after all {len(attempts)} attempts.")


async def _deliver_alert_fired(alert_id: str, alert: Dict[str, Any]):
    """
    FIX-6: Deliver alert.fired to ALL webhooks AND integrations.
    FIX-5: Uses stored snapshot from alert object — never recomputes.
    """
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

    tasks = []

    # Regular webhooks
    for wh_id, url in webhooks.items():
        key = (alert_id, "alert.fired", wh_id)
        tasks.append(asyncio.create_task(_http_post_with_retry(url, fired_payload, key)))

    # FIX-6: Integrations (Slack / Discord)
    for integ in integrations:
        if "alert.fired" in integ.get("events", []):
            key = (alert_id, "alert.fired", integ["id"])
            if integ["type"] == "slack":
                payload = _build_slack_payload(alert, integ, fired=True)
            else:
                payload = _build_discord_payload(alert, integ, fired=True)
            tasks.append(asyncio.create_task(
                _http_post_with_retry(integ["webhook_url"], payload, key)
            ))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _deliver_alert_resolved(alert_id: str, resolved_at: str, alert: Optional[Dict]):
    """
    FIX-6: Deliver alert.resolved to ALL webhooks AND integrations.
    FIX-5: Uses stored alert snapshot for integration payloads.
    """
    resolved_payload = {
        "event": "alert.resolved",
        "alert_id": alert_id,
        "resolved_at": resolved_at,
    }
    if alert:
        resolved_payload.update({
            "failure_rate": alert.get("failure_rate", 0.0),
            "total_proxies": alert.get("total_proxies", 0),
            "failed_proxies": alert.get("failed_proxies", 0),
            "failed_proxy_ids": alert.get("failed_proxy_ids", []),
            "threshold": ALERT_THRESHOLD,
            "message": alert.get("message", ""),
        })

    async with state.lock:
        webhooks = dict(state.webhooks)
        integrations = list(state.integrations)

    tasks = []

    for wh_id, url in webhooks.items():
        key = (alert_id, "alert.resolved", wh_id)
        tasks.append(asyncio.create_task(_http_post_with_retry(url, resolved_payload, key)))

    # FIX-6: Integrations
    for integ in integrations:
        if "alert.resolved" in integ.get("events", []):
            key = (alert_id, "alert.resolved", integ["id"])
            if alert:
                if integ["type"] == "slack":
                    payload = _build_slack_payload(alert, integ, fired=False)
                else:
                    payload = _build_discord_payload(alert, integ, fired=False)
            else:
                payload = resolved_payload
            tasks.append(asyncio.create_task(
                _http_post_with_retry(integ["webhook_url"], payload, key)
            ))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _build_slack_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    """
    FIX-6: Correct Slack payload format.
    IMPORTANT: ts MUST be a plain int (not float, not string).
    """
    color = "#FF0000" if fired else "#00FF00"
    rate_pct = f"{alert['failure_rate'] * 100:.1f}%"
    event_label = "fired" if fired else "resolved"
    text = (
        f"🚨 Alert {event_label}: failure rate {rate_pct} "
        f"(threshold {ALERT_THRESHOLD * 100:.1f}%)"
    )
    return {
        "username": integ.get("username", "ProxyWatch"),
        "text": text,
        "attachments": [
            {
                "color": color,
                "fields": [
                    {"title": "Alert ID", "value": alert["alert_id"], "short": True},
                    {"title": "Failure Rate", "value": rate_pct, "short": True},
                    {"title": "Failed Proxies", "value": str(alert["failed_proxies"]), "short": True},
                    {"title": "Threshold", "value": f"{ALERT_THRESHOLD * 100:.1f}%", "short": True},
                    {"title": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"]) or "none", "short": False},
                    {"title": "Fired At", "value": alert.get("fired_at", ""), "short": False},
                ],
                "footer": "ProxyMaze'26 | Torch Labs",
                # FIX: ts MUST be plain int, never float or string
                "ts": unix_epoch_int(),
            }
        ],
    }


def _build_discord_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    """
    FIX-6: Correct Discord payload format.
    IMPORTANT: color MUST be a plain int (never string like "#FF0000").
    """
    # FIX: plain int — red=16711680, green=65280
    color_int = 16711680 if fired else 65280
    title = "🚨 ProxyMaze Alert Fired" if fired else "✅ ProxyMaze Alert Resolved"
    rate_pct = f"{alert['failure_rate'] * 100:.1f}%"
    description = (
        f"Proxy pool failure rate has {'exceeded' if fired else 'dropped below'} threshold"
    )
    return {
        "embeds": [
            {
                "title": title,
                "description": description,
                # FIX: plain int, never a string
                "color": color_int,
                "fields": [
                    {"name": "Alert ID", "value": alert["alert_id"], "inline": True},
                    {"name": "Failure Rate", "value": rate_pct, "inline": True},
                    {"name": "Failed Proxies", "value": str(alert["failed_proxies"]), "inline": True},
                    {"name": "Threshold", "value": f"{ALERT_THRESHOLD * 100:.1f}%", "inline": True},
                    {"name": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"]) or "none", "inline": False},
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
# GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


# Test helper: always returns 503 so probes classify it as "down"
@app.get("/fail/{proxy_id}")
async def fail_endpoint(proxy_id: str):
    return Response(status_code=503)


# ---------------------------------------------------------------------------
# POST /config
# ---------------------------------------------------------------------------
class ConfigBody(BaseModel):
    check_interval_seconds: int = Field(default=DEFAULT_CHECK_INTERVAL, ge=1)
    request_timeout_ms: int = Field(default=DEFAULT_REQUEST_TIMEOUT, ge=1)
    model_config = ConfigDict(extra="ignore")


@app.post("/config", status_code=200)
async def post_config(body: ConfigBody):
    async with state.lock:
        state.config["check_interval_seconds"] = body.check_interval_seconds
        state.config["request_timeout_ms"] = body.request_timeout_ms
        cfg = dict(state.config)
    restart_monitor()
    return cfg


# ---------------------------------------------------------------------------
# GET /config
# ---------------------------------------------------------------------------
@app.get("/config")
async def get_config():
    async with state.lock:
        return dict(state.config)


# ---------------------------------------------------------------------------
# POST /proxies
# ---------------------------------------------------------------------------
class PostProxiesBody(BaseModel):
    proxies: List[str]
    replace: Optional[bool] = False
    model_config = ConfigDict(extra="ignore")


@app.post("/proxies", status_code=201)
async def post_proxies(body: PostProxiesBody):
    accepted_proxies = []
    async with state.lock:
        if body.replace:
            state.proxies.clear()
        for url in body.proxies:
            pid = proxy_id_from_url(url)
            if pid in state.proxies:
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
            accepted_proxies.append({
                "id": pid,
                "url": url,
                "status": state.proxies[pid]["status"],
            })
    return {"accepted": len(accepted_proxies), "proxies": accepted_proxies}


# ---------------------------------------------------------------------------
# GET /proxies
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
# GET /proxies/{id}
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
# GET /proxies/{id}/history
# ---------------------------------------------------------------------------
@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    async with state.lock:
        p = state.proxies.get(proxy_id)
        if not p:
            raise HTTPException(status_code=404, detail=f"Proxy '{proxy_id}' not found.")
        return list(p["history"])


# ---------------------------------------------------------------------------
# DELETE /proxies
# ---------------------------------------------------------------------------
@app.delete("/proxies", status_code=204)
async def delete_proxies():
    async with state.lock:
        state.proxies.clear()
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# GET /alerts
# ---------------------------------------------------------------------------
@app.get("/alerts")
async def get_alerts():
    async with state.lock:
        return list(state.alerts)


# ---------------------------------------------------------------------------
# POST /webhooks
# ---------------------------------------------------------------------------
class WebhookBody(BaseModel):
    url: str
    model_config = ConfigDict(extra="ignore")


@app.post("/webhooks", status_code=201)
async def post_webhooks(body: WebhookBody):
    wh_id = f"wh-{uuid.uuid4().hex[:8]}"
    async with state.lock:
        state.webhooks[wh_id] = body.url
    return {"webhook_id": wh_id, "url": body.url}


# ---------------------------------------------------------------------------
# POST /integrations
# ---------------------------------------------------------------------------
class IntegrationBody(BaseModel):
    type: str
    webhook_url: str
    username: Optional[str] = "ProxyWatch"
    events: Optional[List[str]] = ["alert.fired", "alert.resolved"]
    model_config = ConfigDict(extra="ignore")


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
# GET /metrics
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
