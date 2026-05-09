"""
ProxyMaze'26  —  Complete implementation targeting 270 / 270
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("proxymaze")

# ─────────────────────────────────────────────────────────────
# Mutable global state  (single-process, in-memory)
# ─────────────────────────────────────────────────────────────
_config: dict = {
    "check_interval_seconds": 60,
    "request_timeout_ms": 5000,
}

# proxy_id → proxy record
_proxies: dict[str, dict] = {}

# alert_id → alert record
_alerts: dict[str, dict] = {}

# webhook_id → {webhook_id, url}
_webhooks: dict[str, dict] = {}

# integration_id → integration record
_integrations: dict[str, dict] = {}

_metrics: dict = {
    "total_checks": 0,
    "webhook_deliveries": 0,
}

_active_alert_id: Optional[str] = None   # at most one active alert
_alert_lock = asyncio.Lock()             # serialises evaluate_alert()
_check_lock = asyncio.Lock()             # prevents concurrent check cycles

THRESHOLD: float = 0.20

# ─────────────────────────────────────────────────────────────
# Tiny helpers
# ─────────────────────────────────────────────────────────────

def _now() -> str:
    """ISO-8601 UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch() -> int:
    return int(time.time())


def _proxy_id_from_url(url: str) -> str:
    """
    Extract proxy ID from URL.
    For URLs like http://mock-proxy.internal/px-001 → px-001
    For URLs like http://px-001.internal/ → px-001
    """
    stripped = url.rstrip("/")
    part = stripped.split("/")[-1]
    # If the last segment is empty or looks like a domain, use the hostname segment
    if not part or "." in part:
        # Try to get hostname
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            # Use hostname without port, or last path component
            hostname = parsed.hostname or ""
            path = parsed.path.rstrip("/")
            if path:
                part = path.split("/")[-1] or hostname
            else:
                part = hostname
        except Exception:
            pass
    return part


# ─────────────────────────────────────────────────────────────
# HTTP probe
# ─────────────────────────────────────────────────────────────

async def _probe(proxy: dict) -> bool:
    """
    Return True  → proxy is UP   (2xx within timeout, or 3xx/4xx reachable)
    Return False → proxy is DOWN (timeout / conn-err / 5xx)
    """
    url = proxy["url"]
    timeout_s = _config["request_timeout_ms"] / 1000.0
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=timeout_s,
                read=timeout_s,
                write=timeout_s,
                pool=timeout_s,
            ),
            follow_redirects=True,
            verify=False,
        ) as client:
            r = await client.get(url)
            if 200 <= r.status_code < 300:
                return True
            if r.status_code >= 500:
                return False
            # 3xx already followed; 4xx means the server is reachable → up
            return True
    except httpx.TimeoutException:
        log.debug("Probe timeout for %s", url)
        return False
    except httpx.ConnectError:
        log.debug("Probe connect error for %s", url)
        return False
    except Exception as exc:
        log.debug("Probe error for %s: %s", url, exc)
        return False


# ─────────────────────────────────────────────────────────────
# Webhook delivery  (retry on 5xx / network errors, exactly-once success)
# ─────────────────────────────────────────────────────────────

async def _deliver(url: str, payload: dict) -> bool:
    """
    Single delivery attempt.
    Returns True iff delivery was accepted (any non-5xx response).
    Returns False on 5xx or network error (should retry).
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            verify=False,
        ) as client:
            r = await client.post(
                url, json=payload,
                headers={"Content-Type": "application/json"},
            )
            if r.status_code in (500, 502, 503, 504):
                log.warning("Receiver %s → %d  (will retry)", url, r.status_code)
                return False
            log.info("Delivered to %s  →  %d", url, r.status_code)
            return True
    except (httpx.TimeoutException, httpx.ConnectError, httpx.NetworkError) as exc:
        log.warning("Network error delivering to %s: %s", url, exc)
        return False
    except Exception as exc:
        log.warning("Unexpected error delivering to %s: %s", url, exc)
        return False


async def _deliver_with_retry(url: str, payload: dict) -> None:
    """
    Retry with exponential back-off until the receiver accepts.
    Counts exactly one successful delivery in _metrics.
    """
    delay = 1
    for attempt in range(30):          # up to ~17 minutes of retrying
        if await _deliver(url, payload):
            _metrics["webhook_deliveries"] += 1
            return
        log.info("Retry attempt %d for %s, waiting %ds", attempt + 1, url, min(delay, 60))
        await asyncio.sleep(min(delay, 60))
        delay *= 2
    log.error("Gave up delivering to %s after 30 attempts", url)


def _schedule_webhook(payload: dict) -> None:
    """Fire-and-forget delivery to every registered webhook receiver."""
    for wh in list(_webhooks.values()):
        asyncio.create_task(_deliver_with_retry(wh["url"], payload))


def _schedule_integrations(alert: dict, event: str) -> None:
    """Fire-and-forget delivery to every registered integration."""
    for intg in list(_integrations.values()):
        if event not in intg.get("events", []):
            continue
        itype = intg["type"]
        if itype == "slack":
            payload = _slack_payload(intg, alert, event)
        elif itype == "discord":
            payload = _discord_payload(intg, alert, event)
        else:
            continue
        asyncio.create_task(_deliver_with_retry(intg["webhook_url"], payload))


# ─────────────────────────────────────────────────────────────
# Slack Block Kit payload builder
# ─────────────────────────────────────────────────────────────

def _slack_payload(intg: dict, alert: dict, event: str) -> dict:
    fired_at = alert.get("fired_at", _now())
    failed_ids = ", ".join(alert.get("failed_proxy_ids", [])) or "none"
    failure_rate = alert.get("failure_rate", 0)
    threshold = alert.get("threshold", THRESHOLD)
    alert_id = alert.get("alert_id", "")
    failed_proxies = alert.get("failed_proxies", 0)

    if event == "alert.fired":
        header_text = ":red_circle: Alert Fired — Proxy Pool Threshold Breached"
        summary = f"Failure rate *{failure_rate}* has exceeded threshold *{threshold}*."
        color = "#FF0000"
        ts_str = fired_at
    else:  # alert.resolved
        header_text = ":large_green_circle: Alert Resolved — Proxy Pool Recovered"
        resolved_at = alert.get("resolved_at", _now())
        summary = f"Pool recovered. Failure rate dropped below threshold *{threshold}*. Resolved: {resolved_at}."
        color = "#36A64F"
        ts_str = resolved_at if event == "alert.resolved" else fired_at

    try:
        ts = int(
            datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
    except Exception:
        ts = _epoch()

    # Block Kit format
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text, "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Alert ID:*\n{alert_id}"},
                {"type": "mrkdwn", "text": f"*Failure Rate:*\n{failure_rate}"},
                {"type": "mrkdwn", "text": f"*Failed Proxies:*\n{failed_proxies}"},
                {"type": "mrkdwn", "text": f"*Threshold:*\n{threshold}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Failed IDs:*\n{failed_ids}"},
                {"type": "mrkdwn", "text": f"*Fired At:*\n{fired_at}"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": "ProxyMaze\u201926 \u2014 Torch Labs"},
            ],
        },
    ]

    # Also include legacy attachments for compatibility
    return {
        "username": intg.get("username", "ProxyWatch"),
        "text": header_text,
        "blocks": blocks,
        "attachments": [
            {
                "color": color,
                "fields": [
                    {"title": "Alert ID",       "value": alert_id,           "short": True},
                    {"title": "Failure Rate",   "value": str(failure_rate),  "short": True},
                    {"title": "Failed Proxies", "value": str(failed_proxies),"short": True},
                    {"title": "Threshold",      "value": str(threshold),     "short": True},
                    {"title": "Failed IDs",     "value": failed_ids,         "short": False},
                    {"title": "Fired At",       "value": fired_at,           "short": True},
                ],
                "footer": "ProxyMaze\u201926 \u2014 Torch Labs",
                "ts": ts,
            }
        ],
    }


# ─────────────────────────────────────────────────────────────
# Discord Embeds payload builder
# ─────────────────────────────────────────────────────────────

def _discord_payload(intg: dict, alert: dict, event: str) -> dict:
    fired_at = alert.get("fired_at", _now())
    failed_ids = ", ".join(alert.get("failed_proxy_ids", [])) or "none"
    failure_rate = alert.get("failure_rate", 0)
    threshold = alert.get("threshold", THRESHOLD)
    alert_id = alert.get("alert_id", "")
    failed_proxies = alert.get("failed_proxies", 0)

    if event == "alert.fired":
        color = 16711680       # #FF0000 red — integer
        title = "\U0001f6a8 Alert Fired \u2014 Proxy Pool Threshold Breached"
        desc = (
            f"Failure rate **{failure_rate}** has exceeded "
            f"threshold **{threshold}**."
        )
        timestamp_str = fired_at
        fields = [
            {"name": "Alert ID",       "value": str(alert_id),        "inline": True},
            {"name": "Failure Rate",   "value": str(failure_rate),    "inline": True},
            {"name": "Failed Proxies", "value": str(failed_proxies),  "inline": True},
            {"name": "Threshold",      "value": str(threshold),       "inline": True},
            {"name": "Failed IDs",     "value": failed_ids,           "inline": False},
            {"name": "Fired At",       "value": fired_at,             "inline": True},
        ]
    else:
        color = 3329330        # #32CD32 green — integer
        resolved_at = alert.get("resolved_at", _now())
        title = "\u2705 Alert Resolved \u2014 Proxy Pool Recovered"
        desc = (
            f"Pool has recovered. Failure rate dropped below "
            f"threshold **{threshold}**. Resolved: {resolved_at}."
        )
        timestamp_str = resolved_at
        fields = [
            {"name": "Alert ID",       "value": str(alert_id),        "inline": True},
            {"name": "Failure Rate",   "value": str(failure_rate),    "inline": True},
            {"name": "Failed Proxies", "value": str(failed_proxies),  "inline": True},
            {"name": "Threshold",      "value": str(threshold),       "inline": True},
            {"name": "Failed IDs",     "value": failed_ids,           "inline": False},
            {"name": "Fired At",       "value": fired_at,             "inline": True},
        ]

    # Discord uses ISO 8601 timestamp for embed timestamp
    try:
        ts_iso = (
            datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .isoformat()
        )
    except Exception:
        ts_iso = _now()

    return {
        "username": intg.get("username", "ProxyWatch"),
        "embeds": [{
            "title":       title,
            "description": desc,
            "color":       color,       # integer 0–16777215
            "fields":      fields,
            "timestamp":   ts_iso,      # ISO 8601 string for Discord
            "footer":      {"text": "ProxyMaze\u201926 \u2014 Torch Labs"},
        }],
    }


# ─────────────────────────────────────────────────────────────
# Alert state machine
# ─────────────────────────────────────────────────────────────

async def _fire_alert(total: int, down: int, ids: list[str], rate: float) -> None:
    global _active_alert_id
    alert_id = f"alert-{uuid.uuid4().hex[:8]}"
    fired_at = _now()
    rec = {
        "alert_id":         alert_id,
        "status":           "active",
        "failure_rate":     round(rate, 4),
        "total_proxies":    total,
        "failed_proxies":   down,
        "failed_proxy_ids": list(ids),
        "threshold":        THRESHOLD,
        "fired_at":         fired_at,
        "resolved_at":      None,
        "message":          "Proxy pool failure rate exceeded threshold",
    }
    _alerts[alert_id] = rec
    _active_alert_id = alert_id
    log.info("ALERT FIRED  %s  rate=%.4f  down=%d/%d", alert_id, rate, down, total)

    wh_payload = {
        "event":            "alert.fired",
        "alert_id":         alert_id,
        "fired_at":         fired_at,
        "failure_rate":     rec["failure_rate"],
        "total_proxies":    total,
        "failed_proxies":   down,
        "failed_proxy_ids": list(ids),
        "threshold":        THRESHOLD,
        "message":          "Proxy pool failure rate exceeded threshold",
    }
    _schedule_webhook(wh_payload)
    _schedule_integrations(rec, "alert.fired")


async def _resolve_alert() -> None:
    global _active_alert_id
    alert_id = _active_alert_id
    if not alert_id or alert_id not in _alerts:
        _active_alert_id = None
        return
    rec = _alerts[alert_id]
    resolved_at = _now()
    rec["status"] = "resolved"
    rec["resolved_at"] = resolved_at
    _active_alert_id = None
    log.info("ALERT RESOLVED  %s", alert_id)

    wh_payload = {
        "event":        "alert.resolved",
        "alert_id":     alert_id,
        "resolved_at":  resolved_at,
        # include extra fields for completeness
        "failure_rate":     rec["failure_rate"],
        "total_proxies":    rec["total_proxies"],
        "failed_proxies":   rec["failed_proxies"],
        "failed_proxy_ids": rec["failed_proxy_ids"],
        "threshold":        rec["threshold"],
        "fired_at":         rec["fired_at"],
        "message":          "Proxy pool failure rate dropped below threshold",
    }
    _schedule_webhook(wh_payload)
    _schedule_integrations(rec, "alert.resolved")


async def _evaluate() -> None:
    """
    Called after every check cycle.
    Serialised by _alert_lock → exactly one active alert, no duplicates.
    """
    global _active_alert_id

    async with _alert_lock:
        total = len(_proxies)
        if total == 0:
            return

        down_ids = [pid for pid, p in _proxies.items() if p["status"] == "down"]
        down = len(down_ids)
        rate = down / total

        log.info(
            "Evaluate  total=%d  down=%d  rate=%.4f  active=%s",
            total, down, rate, _active_alert_id,
        )

        if rate >= THRESHOLD:
            if _active_alert_id is None:
                # No active alert → fire a new one
                await _fire_alert(total, down, down_ids, rate)
            else:
                # Breach continues → keep existing alert, update live data only
                # DO NOT fire a new alert or duplicate webhooks
                rec = _alerts[_active_alert_id]
                rec["failed_proxy_ids"] = list(down_ids)
                rec["failed_proxies"]   = down
                rec["failure_rate"]     = round(rate, 4)
                rec["total_proxies"]    = total
        else:
            if _active_alert_id is not None:
                await _resolve_alert()


# ─────────────────────────────────────────────────────────────
# Check cycle  (protected by _check_lock to prevent concurrent runs)
# ─────────────────────────────────────────────────────────────

async def _run_checks() -> None:
    """Probe every proxy in the current pool concurrently, then evaluate."""
    if not _proxies:
        return

    # Prevent concurrent check cycles from running simultaneously
    if _check_lock.locked():
        log.info("Check cycle already in progress, skipping")
        return

    async with _check_lock:
        checked_at = _now()
        snapshot = list(_proxies.keys())       # stable list for this cycle

        log.info("Running check cycle for %d proxies", len(snapshot))

        # Launch all probes concurrently
        tasks: dict[str, asyncio.Task] = {}
        for pid in snapshot:
            if pid in _proxies:
                tasks[pid] = asyncio.create_task(_probe(_proxies[pid]))

        for pid, task in tasks.items():
            if pid not in _proxies:
                task.cancel()
                continue
            try:
                is_up: bool = await task
            except Exception:
                is_up = False

            if pid not in _proxies:
                continue

            p = _proxies[pid]
            status = "up" if is_up else "down"
            p["status"]          = status
            p["last_checked_at"] = checked_at

            if status == "down":
                p["consecutive_failures"] += 1
            else:
                p["consecutive_failures"] = 0

            p["total_checks"] += 1
            p["up_checks"]    += 1 if is_up else 0
            p["uptime_percentage"] = round(p["up_checks"] / p["total_checks"] * 100, 1)
            p["history"].append({"checked_at": checked_at, "status": status})

            _metrics["total_checks"] += 1

        await _evaluate()


# ─────────────────────────────────────────────────────────────
# Background monitoring loop
# ─────────────────────────────────────────────────────────────

async def _monitor_loop() -> None:
    log.info("Background monitoring loop started")
    while True:
        try:
            await asyncio.sleep(_config["check_interval_seconds"])
            log.info("Scheduled check cycle  (interval=%ds)", _config["check_interval_seconds"])
            await _run_checks()
        except asyncio.CancelledError:
            log.info("Monitor loop cancelled — shutting down")
            break
        except Exception as exc:
            log.exception("Unexpected error in monitor loop: %s", exc)
            await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────
# FastAPI application
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def _lifespan(app: FastAPI):
    task = asyncio.create_task(_monitor_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="ProxyMaze'26", lifespan=_lifespan)


# ── Ch01  GET /health ────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Ch02  POST /config ───────────────────────────────────────

class _ConfigIn(BaseModel):
    check_interval_seconds: int
    request_timeout_ms: int
    model_config = {"extra": "allow"}


@app.post("/config")
async def post_config(body: _ConfigIn):
    _config["check_interval_seconds"] = body.check_interval_seconds
    _config["request_timeout_ms"]     = body.request_timeout_ms
    log.info("Config updated: interval=%ds  timeout=%dms",
             body.check_interval_seconds, body.request_timeout_ms)
    return {
        "check_interval_seconds": body.check_interval_seconds,
        "request_timeout_ms":     body.request_timeout_ms,
    }


# ── Ch03  GET /config ────────────────────────────────────────

@app.get("/config")
async def get_config():
    return {
        "check_interval_seconds": _config["check_interval_seconds"],
        "request_timeout_ms":     _config["request_timeout_ms"],
    }


# ── Ch04  POST /proxies ──────────────────────────────────────

class _ProxiesIn(BaseModel):
    proxies: list[str]
    replace: bool = False
    model_config = {"extra": "allow"}


@app.post("/proxies", status_code=201)
async def post_proxies(body: _ProxiesIn):
    if body.replace:
        _proxies.clear()

    accepted = []
    for url in body.proxies:
        pid = _proxy_id_from_url(url)
        if pid not in _proxies:
            _proxies[pid] = {
                "id":                   pid,
                "url":                  url,
                "status":               "pending",
                "last_checked_at":      None,
                "consecutive_failures": 0,
                "total_checks":         0,
                "up_checks":            0,
                "uptime_percentage":    0.0,
                "history":              [],
            }
        accepted.append(_proxies[pid])

    # Trigger an immediate check cycle without blocking the response
    asyncio.create_task(_run_checks())

    return {
        "accepted": len(accepted),
        "proxies": [
            {"id": p["id"], "url": p["url"], "status": p["status"]}
            for p in accepted
        ],
    }


# ── Ch05  GET /proxies ───────────────────────────────────────

@app.get("/proxies")
async def get_proxies():
    total = len(_proxies)
    down  = sum(1 for p in _proxies.values() if p["status"] == "down")
    up    = sum(1 for p in _proxies.values() if p["status"] == "up")
    rate  = round(down / total, 4) if total else 0.0
    return {
        "total":        total,
        "up":           up,
        "down":         down,
        "failure_rate": rate,
        "proxies": [
            {
                "id":                   p["id"],
                "url":                  p["url"],
                "status":               p["status"],
                "last_checked_at":      p["last_checked_at"],
                "consecutive_failures": p["consecutive_failures"],
            }
            for p in _proxies.values()
        ],
    }


# ── Ch07  GET /proxies/{id}/history  (declared BEFORE /{id}) ──

@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    if proxy_id not in _proxies:
        raise HTTPException(404, "Proxy not found")
    return _proxies[proxy_id]["history"]


# ── Ch06  GET /proxies/{id} ──────────────────────────────────

@app.get("/proxies/{proxy_id}")
async def get_proxy(proxy_id: str):
    if proxy_id not in _proxies:
        raise HTTPException(404, "Proxy not found")
    p = _proxies[proxy_id]
    return {
        "id":                   p["id"],
        "url":                  p["url"],
        "status":               p["status"],
        "last_checked_at":      p["last_checked_at"],
        "consecutive_failures": p["consecutive_failures"],
        "total_checks":         p["total_checks"],
        "uptime_percentage":    p["uptime_percentage"],
        "history":              p["history"],
    }


# ── Ch08  DELETE /proxies ────────────────────────────────────

@app.delete("/proxies", status_code=204)
async def delete_proxies():
    _proxies.clear()
    # NOTE: alerts are intentionally NOT cleared (spec requirement)


# ── Ch09  GET /alerts ────────────────────────────────────────

@app.get("/alerts")
async def get_alerts():
    return list(_alerts.values())


# ── Ch10  POST /webhooks ─────────────────────────────────────

class _WebhookIn(BaseModel):
    url: str
    model_config = {"extra": "allow"}


@app.post("/webhooks", status_code=201)
async def post_webhook(body: _WebhookIn):
    wid = f"wh-{uuid.uuid4().hex[:8]}"
    _webhooks[wid] = {"webhook_id": wid, "url": body.url}
    log.info("Webhook registered  %s → %s", wid, body.url)
    return {"webhook_id": wid, "url": body.url}


# ── Ch11  POST /integrations ─────────────────────────────────

class _IntegrationIn(BaseModel):
    type: str
    webhook_url: str
    username: str = "ProxyWatch"
    events: list[str] = ["alert.fired", "alert.resolved"]
    model_config = {"extra": "allow"}


@app.post("/integrations", status_code=201)
async def post_integration(body: _IntegrationIn):
    iid = f"intg-{uuid.uuid4().hex[:8]}"
    _integrations[iid] = {
        "integration_id": iid,
        "type":           body.type,
        "webhook_url":    body.webhook_url,
        "username":       body.username or "ProxyWatch",
        "events":         body.events or ["alert.fired", "alert.resolved"],
    }
    log.info("Integration registered  %s  type=%s", iid, body.type)
    return {"integration_id": iid, "type": body.type, "webhook_url": body.webhook_url}


# ── Ch12  GET /metrics ───────────────────────────────────────

@app.get("/metrics")
async def get_metrics():
    active = sum(1 for a in _alerts.values() if a["status"] == "active")
    return {
        "total_checks":       _metrics["total_checks"],
        "current_pool_size":  len(_proxies),
        "active_alerts":      active,
        "total_alerts":       len(_alerts),
        "webhook_deliveries": _metrics["webhook_deliveries"],
    }


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)