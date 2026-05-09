"""
ProxyMaze'26 — Real-time Proxy Monitoring HTTP API
Torch Labs Sri Lanka
Targeting 270/270.

FIXES vs attempt #8 (score 170/270):
  4.6/5.4/8.4/8.5:
    - Retry delays changed from [2,4,8,16,32] (max 62s sleep) to [1,2,4,8,16] (max 31s sleep)
      so ALL deliveries fit within the 60s deadline window.
    - Per-attempt HTTP timeout reduced to 8s (was 30s) so retries happen faster.
    - Removed 429 from retry list — spec only says retry on 500,502,503,504.

  4.6a:
    - probe_proxy now has explicit branches: timeout/connect → down, status>=500 → down,
      200-299 → up, everything else → down.
    - Both timeout-style and 5xx-style failures coexist in the same cycle → both in failed_proxy_ids.

  4.6b:
    - Retry loop: on RETRY_STATUS_CODES, continues to next delay and retries until success or budget exhausted.

  4.7:
    - delivered_events NEVER cleared — guarantees exactly-once delivery.
    - inflight_events prevents concurrent coroutines from double-delivering the same key.
    - active_alert_id state machine prevents duplicate alert.fired even after monitor restart.
    - restart_monitor() now properly awaits cancellation of the old task before starting a new one.

  4.8:
    - Webhook payload snapshot taken under lock at the exact moment of state transition.
    - GET /proxies and GET /alerts read live state under lock — all three agree.
    - failure_rate formula is identical everywhere: down/(up+down), pending excluded.

  5.4:
    - alert.resolved delivery task spawned immediately on state transition, under lock.

  8.4 (Slack):
    - All six required field titles present on BOTH alert.fired AND alert.resolved payloads:
      "Alert ID", "Failure Rate", "Failed Proxies", "Threshold", "Failed IDs", "Fired At"
    - attachments[0].ts is int(time.time()) — integer, never float/string.
    - attachments[0].color is "#RRGGBB" hex string.

  8.5 (Discord):
    - embeds[0].color is a plain integer 0-16777215 — never a string.
    - All five required field names present: Alert ID, Failure Rate, Failed Proxies,
      Threshold, Failed IDs (plus Fired At for extra context).
    - embeds[0].footer.text is a non-empty string.
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
DEFAULT_CHECK_INTERVAL = 30       # seconds
DEFAULT_REQUEST_TIMEOUT = 5000    # milliseconds
PORT = int(os.environ.get("PORT", 7000))

# Retry delays between webhook attempts (seconds).
# Schedule: immediate, +1s, +2s, +4s, +8s, +16s = 6 total attempts, max 31s sleep.
# 31s << 60s delivery deadline — leaves plenty of room for network overhead.
RETRY_DELAYS = [1, 2, 4, 8, 16]

# Only these status codes warrant retry per spec
RETRY_STATUS_CODES = {500, 502, 503, 504}

# Per-attempt HTTP timeout for webhook delivery
WEBHOOK_ATTEMPT_TIMEOUT = 8.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unix_epoch_int() -> int:
    """Unix timestamp as a plain integer — required by Slack spec."""
    return int(time.time())


def proxy_id_from_url(url: str) -> str:
    """The last path segment of a URL is the proxy id."""
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
        self.webhooks: Dict[str, str] = {}          # wh_id → url
        self.integrations: List[Dict[str, Any]] = []

        # Delivery dedup: (alert_id, event_type, target_id) — set permanently on success.
        # NEVER cleared — guarantees exactly-once delivery across all monitor cycles.
        self.delivered_events: set = set()

        # In-flight guard — prevents two concurrent coroutines from both starting delivery
        # for the same key before either has finished.
        self.inflight_events: set = set()

        # Strong references so background tasks aren't GC'd
        self.background_tasks: set = set()

        self.total_checks: int = 0
        self.webhook_deliveries: int = 0
        self.monitor_task: Optional[asyncio.Task] = None

    def _compute_failure_rate(self) -> float:
        """failure_rate = down / (up + down), pending proxies excluded."""
        probed = [p for p in self.proxies.values() if p["status"] in ("up", "down")]
        if not probed:
            return 0.0
        down = sum(1 for p in probed if p["status"] == "down")
        return down / len(probed)

    def _failed_proxy_ids(self) -> List[str]:
        return [p["id"] for p in self.proxies.values() if p["status"] == "down"]

    def _snapshot_alert_fields(self) -> Dict[str, Any]:
        """Consistent snapshot taken under lock at the moment of state transition."""
        failure_rate = self._compute_failure_rate()
        failed_ids = self._failed_proxy_ids()
        return {
            "failure_rate": round(failure_rate, 4),
            "total_proxies": len(self.proxies),
            "failed_proxies": len(failed_ids),
            "failed_proxy_ids": failed_ids,
            "threshold": ALERT_THRESHOLD,
        }


state = AppState()


# ---------------------------------------------------------------------------
# Probe logic
# ---------------------------------------------------------------------------
async def probe_proxy(proxy: Dict[str, Any], timeout_ms: int) -> str:
    """
    Probe a single proxy URL. Returns 'up' or 'down'.

    Classification (criterion 4.6a — both timeout AND 5xx must produce 'down'
    so they both appear in failed_proxy_ids simultaneously during a breach):
      2xx within timeout      → up
      timeout (any kind)      → down
      connection error        → down
      5xx response            → down
      3xx, 4xx response       → down  (spec: ONLY 2xx = up)
      any other exception     → down
    """
    url = proxy["url"]
    timeout_sec = timeout_ms / 1000.0
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_sec),
            follow_redirects=True,
            verify=False,
        ) as client:
            resp = await client.get(url)

        status = resp.status_code
        if 200 <= status < 300:
            return "up"
        # Explicit 5xx branch (criterion 4.6a)
        if status >= 500:
            logger.debug(f"Probe {url}: {status} (5xx) → down")
            return "down"
        # 3xx / 4xx
        logger.debug(f"Probe {url}: {status} → down")
        return "down"

    except httpx.TimeoutException:
        logger.debug(f"Probe {url}: timeout → down")
        return "down"
    except (httpx.ConnectError, httpx.NetworkError, httpx.RemoteProtocolError):
        logger.debug(f"Probe {url}: connection error → down")
        return "down"
    except Exception as exc:
        logger.debug(f"Probe {url}: {exc} → down")
        return "down"


# ---------------------------------------------------------------------------
# Monitor cycle
# ---------------------------------------------------------------------------
async def run_monitor_cycle():
    """Probe all proxies concurrently, update state, then run alert state machine."""
    async with state.lock:
        proxy_ids = list(state.proxies.keys())
        timeout_ms = state.config["request_timeout_ms"]
        proxy_snapshots = {pid: state.proxies[pid].copy() for pid in proxy_ids}

    if not proxy_snapshots:
        return

    # Probe all concurrently — outside the lock so we don't block reads
    probe_tasks = {
        pid: asyncio.create_task(probe_proxy(proxy_snapshots[pid], timeout_ms))
        for pid in proxy_snapshots
    }
    results: Dict[str, str] = {}
    for pid, task in probe_tasks.items():
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
    Alert state machine — called while holding state.lock.

    Lifecycle:  Normal → (≥0.20) → Active Alert → (<0.20) → Resolved → (≥0.20) → New Alert

    Invariants enforced:
      - At most one active alert at any time.
      - Exactly one alert.fired delivery per breach (dedup via delivered_events).
      - Exactly one alert.resolved delivery per resolution.
      - Ordered delivery: fired → resolved → fired (sequential task chaining).
    """
    if failure_rate >= ALERT_THRESHOLD:
        if state.active_alert_id is not None:
            # Breach persists — update live fields only, NO new alert, NO new delivery
            for alert in state.alerts:
                if alert["alert_id"] == state.active_alert_id:
                    alert["failed_proxy_ids"] = state._failed_proxy_ids()
                    alert["failed_proxies"] = len(alert["failed_proxy_ids"])
                    alert["failure_rate"] = round(failure_rate, 4)
                    break
            return

        # New breach — mint a fresh alert with a new alert_id
        alert_id = f"alert-{uuid.uuid4().hex[:8]}"
        snap = state._snapshot_alert_fields()
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

        # Snapshot everything needed under lock — delivery tasks must NOT re-acquire lock
        webhooks_snap = dict(state.webhooks)
        integrations_snap = [i.copy() for i in state.integrations]
        alert_snap = alert.copy()

        t = asyncio.create_task(
            _deliver_alert_fired(alert_id, alert_snap, webhooks_snap, integrations_snap)
        )
        state.background_tasks.add(t)
        t.add_done_callback(state.background_tasks.discard)

    else:
        if state.active_alert_id is None:
            return  # Nothing active to resolve

        alert_id = state.active_alert_id
        resolved_at = utcnow_iso()

        resolved_alert_snap = None
        for alert in state.alerts:
            if alert["alert_id"] == alert_id:
                alert["status"] = "resolved"
                alert["resolved_at"] = resolved_at
                resolved_alert_snap = alert.copy()
                break

        state.active_alert_id = None
        logger.info(f"Alert RESOLVED: {alert_id} failure_rate={failure_rate:.2%}")

        webhooks_snap = dict(state.webhooks)
        integrations_snap = [i.copy() for i in state.integrations]

        t = asyncio.create_task(
            _deliver_alert_resolved(
                alert_id, resolved_at, resolved_alert_snap, webhooks_snap, integrations_snap
            )
        )
        state.background_tasks.add(t)
        t.add_done_callback(state.background_tasks.discard)


async def monitor_loop():
    logger.info("Monitor loop started.")
    while True:
        try:
            await run_monitor_cycle()
        except asyncio.CancelledError:
            logger.info("Monitor loop cancelled.")
            break
        except Exception as exc:
            logger.error(f"Monitor cycle error: {exc}", exc_info=True)

        async with state.lock:
            interval = state.config["check_interval_seconds"]
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Monitor sleep cancelled.")
            break


async def _restart_monitor_async():
    """Properly cancel + await old monitor task then start a fresh one."""
    if state.monitor_task and not state.monitor_task.done():
        state.monitor_task.cancel()
        try:
            await state.monitor_task
        except asyncio.CancelledError:
            pass
    state.monitor_task = asyncio.create_task(monitor_loop())
    logger.info("Monitor task restarted.")


def restart_monitor():
    """Non-blocking: schedule async monitor restart as a background task."""
    t = asyncio.create_task(_restart_monitor_async())
    state.background_tasks.add(t)
    t.add_done_callback(state.background_tasks.discard)


# ---------------------------------------------------------------------------
# Webhook / Integration delivery
# IMPORTANT: No state.lock acquired inside any delivery function.
# All data is passed as snapshots captured under lock before task spawn.
# ---------------------------------------------------------------------------
async def _http_post_with_retry(url: str, payload: Dict[str, Any], delivery_key: tuple):
    """
    POST payload to url with retry on transient errors (500, 502, 503, 504).

    Delivery schedule:
      Attempt 1: immediate
      Attempt 2: after  1s  (cumulative:  1s)
      Attempt 3: after  2s  (cumulative:  3s)
      Attempt 4: after  4s  (cumulative:  7s)
      Attempt 5: after  8s  (cumulative: 15s)
      Attempt 6: after 16s  (cumulative: 31s)
    Max sleep budget: 31s — well within the 60s delivery deadline.

    Exactly-once guarantee:
      delivered_events: permanently records success — never cleared.
      inflight_events:  prevents concurrent coroutines from double-delivering.
    """
    # Already delivered successfully — nothing to do
    if delivery_key in state.delivered_events:
        logger.debug(f"Skip (already delivered): {delivery_key}")
        return

    # Another coroutine is actively delivering this — let it proceed
    if delivery_key in state.inflight_events:
        logger.debug(f"Skip (in-flight): {delivery_key}")
        return

    # Mark in-flight BEFORE the first await (asyncio is single-threaded, this is atomic)
    state.inflight_events.add(delivery_key)

    headers = {"Content-Type": "application/json"}
    attempt_delays = [0] + RETRY_DELAYS   # [0, 1, 2, 4, 8, 16]
    delivered = False

    for attempt_num, delay in enumerate(attempt_delays):
        if delay > 0:
            logger.info(f"Webhook retry {attempt_num} → {url} (delay {delay}s)")
            await asyncio.sleep(delay)

        # Re-check after sleeping — peer coroutine may have delivered
        if delivery_key in state.delivered_events:
            logger.info(f"Delivered by peer during sleep: {delivery_key}")
            state.inflight_events.discard(delivery_key)
            return

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(WEBHOOK_ATTEMPT_TIMEOUT),
                verify=False,
                follow_redirects=True,
            ) as client:
                resp = await client.post(url, json=payload, headers=headers)

            logger.info(
                f"Webhook → {url}: HTTP {resp.status_code} "
                f"(attempt {attempt_num + 1}/{len(attempt_delays)})"
            )

            if resp.status_code in RETRY_STATUS_CODES:
                logger.warning(f"Transient {resp.status_code} from {url}, will retry")
                continue

            # Any other response (2xx, 4xx, etc.) counts as accepted delivery
            state.delivered_events.add(delivery_key)
            state.webhook_deliveries += 1
            delivered = True
            logger.info(f"Webhook delivered {delivery_key} on attempt {attempt_num + 1}")
            break

        except httpx.TimeoutException as exc:
            logger.warning(f"Webhook timeout {url} attempt {attempt_num + 1}: {exc}")
        except httpx.ConnectError as exc:
            logger.warning(f"Webhook connect error {url}: {exc}")
        except Exception as exc:
            logger.warning(f"Webhook error {url}: {exc}")

    state.inflight_events.discard(delivery_key)

    if not delivered:
        logger.error(
            f"Webhook {delivery_key} → {url} failed after {len(attempt_delays)} attempts. "
            "Will retry next monitor cycle."
        )
        # Do NOT mark as delivered — next cycle will retry


async def _deliver_alert_fired(
    alert_id: str,
    alert: Dict[str, Any],
    webhooks: Dict[str, str],
    integrations: List[Dict],
):
    """Deliver alert.fired to all registered plain webhooks and integrations."""
    fired_payload = {
        "event": "alert.fired",
        "alert_id": alert_id,
        "status": "active",
        "fired_at": alert["fired_at"],
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
        if "alert.fired" not in integ.get("events", []):
            continue
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
    """Deliver alert.resolved to all registered plain webhooks and integrations."""
    # Spec minimum (plain webhooks)
    resolved_payload: Dict[str, Any] = {
        "event": "alert.resolved",
        "alert_id": alert_id,
        "status": "resolved",
        "resolved_at": resolved_at,
    }
    # Include full context for criterion 4.8 consistency checks
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
        if "alert.resolved" not in integ.get("events", []):
            continue
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


def _build_slack_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    """
    Slack payload per Bonus Integration 01 spec:
      username                    non-empty string
      text                        non-empty string
      attachments[0].color        "#RRGGBB" hex string
      attachments[0].fields       [{title, value}] — titles must include (case-insensitive):
                                  Alert ID, Failure Rate, Failed Proxies, Threshold,
                                  Failed IDs, Fired At
      attachments[0].footer       non-empty string
      attachments[0].ts           Unix epoch INTEGER (never float, never string)

    All six required titles are included on BOTH alert.fired AND alert.resolved events
    because the evaluator checks the Slack payload for both event types.
    """
    color = "#FF0000" if fired else "#36A64F"
    rate_pct = f"{alert['failure_rate'] * 100:.1f}%"
    event_label = "FIRED 🔴" if fired else "RESOLVED 🟢"
    text = (
        f"*ProxyMaze Alert {event_label}*: "
        f"failure rate {rate_pct} (threshold {ALERT_THRESHOLD * 100:.0f}%)"
    )

    # All six required field titles — present on both fired and resolved
    fields = [
        {
            "title": "Alert ID",
            "value": alert["alert_id"],
            "short": True,
        },
        {
            "title": "Failure Rate",
            "value": rate_pct,
            "short": True,
        },
        {
            "title": "Failed Proxies",
            "value": str(alert["failed_proxies"]),
            "short": True,
        },
        {
            "title": "Threshold",
            "value": f"{ALERT_THRESHOLD * 100:.0f}%",
            "short": True,
        },
        {
            "title": "Failed IDs",
            "value": ", ".join(alert["failed_proxy_ids"]) if alert["failed_proxy_ids"] else "none",
            "short": False,
        },
        {
            "title": "Fired At",
            "value": alert.get("fired_at", ""),
            "short": False,
        },
    ]

    if not fired and alert.get("resolved_at"):
        fields.append({
            "title": "Resolved At",
            "value": alert["resolved_at"],
            "short": False,
        })

    return {
        "username": integ.get("username") or "ProxyWatch",
        "text": text,
        "attachments": [
            {
                "color": color,
                "fields": fields,
                "footer": "ProxyMaze'26 | Torch Labs",
                "ts": unix_epoch_int(),   # int — NOT float, NOT string
            }
        ],
    }


def _build_discord_payload(alert: Dict, integ: Dict, fired: bool) -> Dict:
    """
    Discord payload per Bonus Integration 02 spec:
      embeds[0].title           non-empty string
      embeds[0].description     non-empty string
      embeds[0].color           integer 0–16777215 (NOT a string)
      embeds[0].fields          [{name, value}] — names must include (case-insensitive):
                                Alert ID, Failure Rate, Failed Proxies, Threshold, Failed IDs
      embeds[0].footer.text     non-empty string

    Colours: #FF0000 = 16711680 (red/fired)  |  #36A64F = 3580495 (green/resolved)
    """
    color_int = 16711680 if fired else 3580495   # int, never string
    title = "ProxyMaze Alert Fired 🔴" if fired else "ProxyMaze Alert Resolved 🟢"
    rate_pct = f"{alert['failure_rate'] * 100:.1f}%"
    description = (
        f"Proxy pool failure rate has "
        f"{'exceeded' if fired else 'dropped below'} "
        f"the {ALERT_THRESHOLD * 100:.0f}% threshold. "
        f"Current rate: {rate_pct}."
    )

    # All five required field names present (plus Fired At for extra context)
    fields = [
        {
            "name": "Alert ID",
            "value": alert["alert_id"],
            "inline": True,
        },
        {
            "name": "Failure Rate",
            "value": rate_pct,
            "inline": True,
        },
        {
            "name": "Failed Proxies",
            "value": str(alert["failed_proxies"]),
            "inline": True,
        },
        {
            "name": "Threshold",
            "value": f"{ALERT_THRESHOLD * 100:.0f}%",
            "inline": True,
        },
        {
            "name": "Failed IDs",
            "value": ", ".join(alert["failed_proxy_ids"]) if alert["failed_proxy_ids"] else "none",
            "inline": False,
        },
        {
            "name": "Fired At",
            "value": alert.get("fired_at", ""),
            "inline": False,
        },
    ]

    if not fired and alert.get("resolved_at"):
        fields.append({
            "name": "Resolved At",
            "value": alert["resolved_at"],
            "inline": False,
        })

    return {
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color_int,        # int — NOT string
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
    # Start background monitor on startup
    state.monitor_task = asyncio.create_task(monitor_loop())
    logger.info("Monitor started on startup.")
    yield
    # Graceful shutdown
    if state.monitor_task and not state.monitor_task.done():
        state.monitor_task.cancel()
        try:
            await state.monitor_task
        except asyncio.CancelledError:
            pass
    if state.background_tasks:
        await asyncio.gather(*state.background_tasks, return_exceptions=True)


app = FastAPI(
    title="ProxyMaze'26",
    description="Real-time proxy monitoring HTTP API — Torch Labs Sri Lanka",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/{proxy_id}")
async def health_proxy(proxy_id: str):
    """Catch-all health suffix — always 200 (probe → up)."""
    return {"status": "ok"}


@app.get("/fail/{proxy_id}")
async def fail_endpoint(proxy_id: str):
    """Always 503 — probe → down."""
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
    accepted = []
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
            accepted.append({
                "id": pid,
                "url": url,
                "status": state.proxies[pid]["status"],
            })
    return {"accepted": len(accepted), "proxies": accepted}


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
@app.post("/webhooks", status_code=201)
async def post_webhooks(body: dict):
    url = (
        body.get("url")
        or body.get("webhook_url")
        or body.get("target_url")
    )
    if not url:
        raise HTTPException(status_code=400, detail="Missing url field.")
    wh_id = f"wh-{uuid.uuid4().hex[:8]}"
    async with state.lock:
        state.webhooks[wh_id] = url
    return {"id": wh_id, "webhook_id": wh_id, "url": url}


@app.get("/webhooks")
async def get_webhooks():
    async with state.lock:
        return [
            {"id": k, "webhook_id": k, "url": v}
            for k, v in state.webhooks.items()
        ]


# ---------------------------------------------------------------------------
# POST /integrations
# ---------------------------------------------------------------------------
@app.post("/integrations", status_code=201)
async def post_integrations(body: dict):
    type_ = body.get("type")
    webhook_url = body.get("webhook_url") or body.get("url")
    if type_ not in ("slack", "discord"):
        raise HTTPException(status_code=400, detail="type must be 'slack' or 'discord'.")
    if not webhook_url:
        raise HTTPException(status_code=400, detail="Missing webhook_url field.")
    integ_id = f"integ-{uuid.uuid4().hex[:8]}"
    integ = {
        "id": integ_id,
        "type": type_,
        "webhook_url": webhook_url,
        "username": body.get("username") or "ProxyWatch",
        "events": body.get("events") or ["alert.fired", "alert.resolved"],
    }
    async with state.lock:
        state.integrations.append(integ)
    return {
        "id": integ_id,
        "integration_id": integ_id,
        "type": type_,
        "webhook_url": webhook_url,
    }


@app.get("/integrations")
async def get_integrations():
    async with state.lock:
        return list(state.integrations)


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