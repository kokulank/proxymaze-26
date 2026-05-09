"""
ProxyMaze'26 — Real-time Proxy Monitoring HTTP API
Torch Labs Sri Lanka

FINAL FIX – Race condition resolved, retry backoff shortened.
"""
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Response
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
DEFAULT_CHECK_INTERVAL = 30
DEFAULT_REQUEST_TIMEOUT = 5000
PORT = int(os.environ.get("PORT", 7000))
# 🔧 Shortened retry delays to fit within 60 seconds (0 + 2 + 4 + 8 + 16 = 30s)
RETRY_DELAYS = [2, 4, 8, 16]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def unix_epoch_int() -> int:
    return int(time.time())

def proxy_id_from_url(url: str) -> str:
    return url.rstrip("/").split("/")[-1]


# ---------------------------------------------------------------------------
# In‑memory state
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

        self.delivered_events: Set[Tuple[str, str, str]] = set()
        self.inflight_events: Set[Tuple[str, str, str]] = set()

        self.background_tasks: set = set()
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
# Probe logic – strict 2xx = up
# ---------------------------------------------------------------------------
async def probe_proxy(proxy: Dict[str, Any], timeout_ms: int) -> str:
    url = proxy["url"]
    timeout_sec = timeout_ms / 1000.0
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_sec),
            follow_redirects=True,
            verify=False,
        ) as client:
            resp = await client.get(url)
        if 200 <= resp.status_code < 300:
            return "up"
        return "down"
    except Exception:
        return "down"


# ---------------------------------------------------------------------------
# Background monitor
# ---------------------------------------------------------------------------
async def run_monitor_cycle():
    async with state.lock:
        proxy_ids = list(state.proxies.keys())
        timeout_ms = state.config["request_timeout_ms"]
        snapshots = {pid: state.proxies[pid].copy() for pid in proxy_ids}

    if not snapshots:
        return

    # Probe outside the lock
    tasks = {
        pid: asyncio.create_task(probe_proxy(snapshots[pid], timeout_ms))
        for pid in snapshots
    }
    results = {}
    for pid, task in tasks.items():
        try:
            results[pid] = await task
        except Exception:
            results[pid] = "down"

    checked_at = utcnow_iso()

    # 🔧 Release the lock before calling alert handler
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

    # 🔧 Alert handler is called **without** holding the lock
    await _handle_alert_state(failure_rate)


async def _handle_alert_state(failure_rate: float):
    """Accesses state under its own lock when needed."""
    async with state.lock:
        if failure_rate >= ALERT_THRESHOLD:
            if state.active_alert_id is not None:
                for alert in state.alerts:
                    if alert["alert_id"] == state.active_alert_id:
                        alert["failed_proxy_ids"] = state._failed_proxy_ids()
                        alert["failed_proxies"] = len(alert["failed_proxy_ids"])
                        alert["failure_rate"] = round(failure_rate, 4)
                        break
                return

            alert_id = f"alert-{uuid.uuid4().hex[:8]}"
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

            # 📸 Snapshot webhooks & integrations *while still inside the lock*
            webhooks_snap = dict(state.webhooks)
            integrations_snap = list(state.integrations)

        else:
            if state.active_alert_id is not None:
                alert_id = state.active_alert_id
                resolved_at = utcnow_iso()

                resolved_alert = None
                for alert in state.alerts:
                    if alert["alert_id"] == alert_id:
                        alert["status"] = "resolved"
                        alert["resolved_at"] = resolved_at
                        resolved_alert = alert.copy()
                        break

                state.active_alert_id = None
                logger.info(f"Alert RESOLVED: {alert_id} failure_rate={failure_rate:.2%}")

                webhooks_snap = dict(state.webhooks)
                integrations_snap = list(state.integrations)
            else:
                # No active alert, nothing to deliver
                return

    # 🚀 Delivery tasks are created *outside* the lock to avoid blocking
    if 'alert_id' in locals():
        if failure_rate >= ALERT_THRESHOLD:
            t = asyncio.create_task(
                _deliver_alert_fired(alert_id, alert.copy(), webhooks_snap, integrations_snap)
            )
        else:
            t = asyncio.create_task(
                _deliver_alert_resolved(alert_id, resolved_at, resolved_alert, webhooks_snap, integrations_snap)
            )
        state.background_tasks.add(t)
        t.add_done_callback(state.background_tasks.discard)


async def monitor_loop():
    logger.info("Monitor loop started.")
    while True:
        try:
            await run_monitor_cycle()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Monitor cycle error: {e}")

        async with state.lock:
            interval = state.config["check_interval_seconds"]
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break


def restart_monitor():
    if state.monitor_task and not state.monitor_task.done():
        state.monitor_task.cancel()
    state.monitor_task = asyncio.create_task(monitor_loop())
    logger.info("Monitor task (re)started.")


# ---------------------------------------------------------------------------
# Webhook delivery with retry + dedup
# ---------------------------------------------------------------------------
async def _http_post_with_retry(url: str, payload: Dict[str, Any], delivery_key: Tuple[str, str, str]):
    if delivery_key in state.delivered_events:
        return
    if delivery_key in state.inflight_events:
        return

    state.inflight_events.add(delivery_key)
    headers = {"Content-Type": "application/json"}

    # Immediate first attempt (delay 0) + retries
    for attempt, delay in enumerate([0] + RETRY_DELAYS):
        if delay > 0:
            await asyncio.sleep(delay)
        if delivery_key in state.delivered_events:
            state.inflight_events.discard(delivery_key)
            return
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                resp = await client.post(url, json=payload, headers=headers)
            logger.info(f"Webhook POST {url} -> {resp.status_code} (attempt {attempt+1})")
            if 200 <= resp.status_code < 300:
                state.delivered_events.add(delivery_key)
                state.webhook_deliveries += 1
                state.inflight_events.discard(delivery_key)
                return
            if resp.status_code in (429, 500, 502, 503, 504):
                continue
            break
        except Exception as e:
            logger.warning(f"Webhook attempt {attempt+1} to {url} failed: {e}")

    state.inflight_events.discard(delivery_key)
    logger.error(f"Webhook delivery to {url} failed after all retries.")


async def _deliver_alert_fired(
    alert_id: str,
    alert: Dict[str, Any],
    webhooks: Dict[str, str],
    integrations: List[Dict],
):
    fired_payload = {
        "event": "alert.fired",
        "alert_id": alert_id,
        "status": "active",
        "fired_at": alert["fired_at"],
        "timestamp": alert["fired_at"],
        "failure_rate": alert["failure_rate"],
        "total_proxies": alert["total_proxies"],
        "failed_proxies": alert["failed_proxies"],
        "failed_proxy_ids": alert["failed_proxy_ids"],
        "threshold": ALERT_THRESHOLD,
        "message": alert["message"],
    }

    coros = []
    for wh_id, url in webhooks.items():
        key = (alert_id, "alert.fired", wh_id)
        coros.append(_http_post_with_retry(url, fired_payload, key))

    for integ in integrations:
        if "alert.fired" in integ.get("events", []):
            key = (alert_id, "alert.fired", integ["id"])
            if integ["type"] == "slack":
                payload = _build_slack_payload(alert, integ, fired=True)
            else:
                payload = _build_discord_payload(alert, integ, fired=True)
            coros.append(_http_post_with_retry(integ["webhook_url"], payload, key))

    if coros:
        await asyncio.gather(*coros, return_exceptions=True)


async def _deliver_alert_resolved(
    alert_id: str,
    resolved_at: str,
    alert: Optional[Dict],
    webhooks: Dict[str, str],
    integrations: List[Dict],
):
    resolved_payload = {
        "event": "alert.resolved",
        "alert_id": alert_id,
        "status": "resolved",
        "resolved_at": resolved_at,
        "timestamp": resolved_at,
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

    coros = []
    for wh_id, url in webhooks.items():
        key = (alert_id, "alert.resolved", wh_id)
        coros.append(_http_post_with_retry(url, resolved_payload, key))

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
            coros.append(_http_post_with_retry(integ["webhook_url"], payload, key))

    if coros:
        await asyncio.gather(*coros, return_exceptions=True)


# ---------------------------------------------------------------------------
# Slack & Discord payload builders (unchanged)
# ---------------------------------------------------------------------------
def _build_slack_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    color = "#FF0000" if fired else "#36A64F"
    rate_pct = f"{alert['failure_rate'] * 100:.1f}%"
    event_label = "FIRED 🔴" if fired else "RESOLVED 🟢"
    text = (
        f"*ProxyMaze Alert {event_label}*: failure rate {rate_pct} "
        f"(threshold {ALERT_THRESHOLD * 100:.1f}%)"
    )
    fields = [
        {"title": "Alert ID",       "value": alert["alert_id"],                                "short": True},
        {"title": "Failure Rate",   "value": rate_pct,                                         "short": True},
        {"title": "Failed Proxies", "value": str(alert["failed_proxies"]),                     "short": True},
        {"title": "Threshold",      "value": f"{ALERT_THRESHOLD * 100:.1f}%",                 "short": True},
        {"title": "Failed IDs",     "value": ", ".join(alert["failed_proxy_ids"]) or "none",  "short": False},
        {"title": "Fired At",       "value": alert.get("fired_at", ""),                       "short": False},
    ]
    if not fired and alert.get("resolved_at"):
        fields.append({"title": "Resolved At", "value": alert["resolved_at"], "short": False})

    return {
        "username": integ.get("username", "ProxyWatch"),
        "text": text,
        "attachments": [
            {
                "color": color,
                "fields": fields,
                "footer": "ProxyMaze'26 | Torch Labs",
                "ts": unix_epoch_int(),
            }
        ],
    }


def _build_discord_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    color_int = 16711680 if fired else 3580392
    title = "ProxyMaze Alert Fired 🔴" if fired else "ProxyMaze Alert Resolved 🟢"
    rate_pct = f"{alert['failure_rate'] * 100:.1f}%"
    description = (
        f"Proxy pool failure rate has {'exceeded' if fired else 'dropped below'} "
        f"the {ALERT_THRESHOLD * 100:.0f}% threshold."
    )
    fields = [
        {"name": "Alert ID",       "value": alert["alert_id"],                               "inline": True},
        {"name": "Failure Rate",   "value": rate_pct,                                        "inline": True},
        {"name": "Failed Proxies", "value": str(alert["failed_proxies"]),                    "inline": True},
        {"name": "Threshold",      "value": f"{ALERT_THRESHOLD * 100:.1f}%",                "inline": True},
        {"name": "Failed IDs",     "value": ", ".join(alert["failed_proxy_ids"]) or "none", "inline": False},
    ]
    if not fired and alert.get("resolved_at"):
        fields.append({"name": "Resolved At", "value": alert["resolved_at"], "inline": False})

    return {
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color_int,
                "fields": fields,
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
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/health/{proxy_id}")
async def health_proxy(proxy_id: str):
    return {"status": "ok"}

@app.get("/fail/{proxy_id}")
async def fail_endpoint(proxy_id: str):
    return Response(status_code=503)


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


@app.get("/config")
async def get_config():
    async with state.lock:
        return dict(state.config)


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


@app.get("/proxies/{proxy_id}")
async def get_proxy(proxy_id: str):
    async with state.lock:
        p = state.proxies.get(proxy_id)
        if not p:
            raise HTTPException(status_code=404, detail=f"Proxy '{proxy_id}' not found.")
        total_checks = p["total_checks"]
        up_count = p.get("up_count", 0)
        uptime = round(up_count / total_checks * 100, 2) if total_checks > 0 else 0.0
        return {
            "id": p["id"],
            "url": p["url"],
            "status": p["status"],
            "last_checked_at": p["last_checked_at"],
            "consecutive_failures": p["consecutive_failures"],
            "total_checks": total_checks,
            "uptime_percentage": uptime,
            "history": list(p["history"]),
        }


@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    async with state.lock:
        p = state.proxies.get(proxy_id)
        if not p:
            raise HTTPException(status_code=404, detail=f"Proxy '{proxy_id}' not found.")
        return list(p["history"])


@app.delete("/proxies", status_code=204)
async def delete_proxies():
    async with state.lock:
        state.proxies.clear()
    return Response(status_code=204)


@app.get("/alerts")
async def get_alerts():
    async with state.lock:
        return list(state.alerts)


@app.post("/webhooks", status_code=201)
async def post_webhooks(body: dict):
    url = body.get("url") or body.get("webhook_url") or body.get("target_url")
    if not url:
        raise HTTPException(status_code=400, detail="Missing url")
    wh_id = f"wh-{uuid.uuid4().hex[:8]}"
    async with state.lock:
        state.webhooks[wh_id] = url
    return {"id": wh_id, "webhook_id": wh_id, "url": url}


@app.get("/webhooks")
async def get_webhooks():
    async with state.lock:
        return [{"id": k, "webhook_id": k, "url": v} for k, v in state.webhooks.items()]


@app.post("/integrations", status_code=201)
async def post_integrations(body: dict):
    type_ = body.get("type")
    webhook_url = body.get("webhook_url") or body.get("url")
    if type_ not in ("slack", "discord"):
        raise HTTPException(status_code=400, detail="type must be 'slack' or 'discord'.")
    if not webhook_url:
        raise HTTPException(status_code=400, detail="Missing webhook_url")
    integ_id = f"integ-{uuid.uuid4().hex[:8]}"
    integ = {
        "id": integ_id,
        "type": type_,
        "webhook_url": webhook_url,
        "username": body.get("username", "ProxyWatch"),
        "events": body.get("events", ["alert.fired", "alert.resolved"]),
    }
    async with state.lock:
        state.integrations.append(integ)
    return {"id": integ_id, "integration_id": integ_id, "type": type_, "webhook_url": webhook_url}


@app.get("/integrations")
async def get_integrations():
    async with state.lock:
        return list(state.integrations)


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