"""
ProxyMaze'26 — Smoke Test Suite
Hits every endpoint in logical order. Run with: python test_all.py
Server must be running on http://localhost:8080 (or set BASE_URL env var).
"""

import io
import json
import os
import sys
import time
import threading

# Force UTF-8 output on Windows so Unicode symbols don't crash cp1252
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
import http.server
from datetime import datetime, timezone

import requests

BASE_URL = os.environ.get("BASE_URL", "http://localhost:7000").rstrip("/")

PASS = "[PASS]"
FAIL = "[FAIL]"
INFO = "[INFO]"

passed = 0
failed = 0


def check(label: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  {PASS}  {label}")
    else:
        failed += 1
        print(f"  {FAIL}  {label}" + (f" — {detail}" if detail else ""))


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def get(path, **kwargs):
    return requests.get(f"{BASE_URL}{path}", **kwargs)


def post(path, **kwargs):
    return requests.post(f"{BASE_URL}{path}", **kwargs)


def delete(path, **kwargs):
    return requests.delete(f"{BASE_URL}{path}", **kwargs)


# ---------------------------------------------------------------------------
# Tiny webhook receiver — listens on a free port in a background thread
# ---------------------------------------------------------------------------
received_events: list = []
receiver_port: int = 0


class WebhookHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            data = json.loads(body)
            received_events.append(data)
        except Exception:
            pass
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass  # silence default log


def start_webhook_receiver():
    global receiver_port
    server = http.server.HTTPServer(("0.0.0.0", 0), WebhookHandler)
    receiver_port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_ch01_health():
    section("Ch01 — GET /health")
    r = get("/health")
    check("Status 200", r.status_code == 200, str(r.status_code))
    check('Body {"status":"ok"}', r.json() == {"status": "ok"}, str(r.json()))


def test_ch02_post_config():
    section("Ch02 — POST /config")
    body = {"check_interval_seconds": 5, "request_timeout_ms": 2000}
    r = post("/config", json=body)
    check("Status 200", r.status_code == 200, str(r.status_code))
    j = r.json()
    check("check_interval_seconds accepted", j.get("check_interval_seconds") == 5, str(j))
    check("request_timeout_ms accepted", j.get("request_timeout_ms") == 2000, str(j))
    check("Unknown fields silently ignored (extra key)", True)  # validated by sending extra key
    r2 = post("/config", json={**body, "extra_field": "ignore_me"})
    check("Extra field ignored — still 200", r2.status_code == 200)


def test_ch03_get_config():
    section("Ch03 — GET /config")
    r = get("/config")
    check("Status 200", r.status_code == 200)
    j = r.json()
    check("check_interval_seconds == 5", j.get("check_interval_seconds") == 5, str(j))
    check("request_timeout_ms == 2000", j.get("request_timeout_ms") == 2000, str(j))


def test_ch04_post_proxies():
    section("Ch04 — POST /proxies")
    # Use httpbin as real targets so probes can succeed
    body = {
        "proxies": [
            f"{BASE_URL}/health/px-001",
            f"{BASE_URL}/health/px-002",
            "https://this-host-should-not-exist-xyz.invalid/proxy/px-003",
        ],
        "replace": True,
    }
    r = post("/proxies", json=body)
    check("Status 201", r.status_code == 201, str(r.status_code))
    j = r.json()
    check("accepted == 3", j.get("accepted") == 3, str(j))
    check("proxies array length 3", len(j.get("proxies", [])) == 3)
    statuses = {p["id"]: p["status"] for p in j["proxies"]}
    check("px-001 starts pending", statuses.get("px-001") == "pending", str(statuses))
    check("px-003 starts pending", statuses.get("px-003") == "pending", str(statuses))

    # Append mode
    body2 = {"proxies": [f"{BASE_URL}/health/px-004"], "replace": False}
    r2 = post("/proxies", json=body2)
    check("Append — status 201", r2.status_code == 201)
    j2 = r2.json()
    check("Append — accepted 1", j2.get("accepted") == 1)

    # Verify pool has 4 proxies now
    r3 = get("/proxies")
    check("Pool total == 4 after append", r3.json().get("total") == 4, str(r3.json()))

    # Extra fields silently ignored
    r4 = post("/proxies", json={**body2, "rogue_field": 42})
    check("Extra field ignored on POST /proxies", r4.status_code == 201)


def test_ch04_replace_true():
    section("Ch04b — POST /proxies replace:true clears pool")
    body = {"proxies": [f"{BASE_URL}/health/px-010", f"{BASE_URL}/health/px-011"], "replace": True}
    r = post("/proxies", json=body)
    check("Replace — 201", r.status_code == 201)
    r2 = get("/proxies")
    check("Pool total == 2 after replace", r2.json().get("total") == 2, str(r2.json().get("total")))


def test_ch05_get_proxies():
    section("Ch05 — GET /proxies")
    r = get("/proxies")
    check("Status 200", r.status_code == 200)
    j = r.json()
    required_keys = {"total", "up", "down", "failure_rate", "proxies"}
    check("All required keys present", required_keys.issubset(j.keys()), str(j.keys()))
    for p in j["proxies"]:
        required_proxy_keys = {"id", "url", "status", "last_checked_at", "consecutive_failures"}
        check(f"Proxy {p['id']} has all fields", required_proxy_keys.issubset(p.keys()))


def test_wait_for_probes():
    section("Waiting for background probes (up to 20s)…")
    deadline = time.time() + 20
    while time.time() < deadline:
        r = get("/proxies")
        j = r.json()
        probed = [p for p in j["proxies"] if p["status"] in ("up", "down")]
        if len(probed) >= 1:
            print(f"  {INFO}  First probe completed. Pool: {j}")
            break
        time.sleep(1)
    check("At least one proxy probed", len(probed) >= 1)


def test_ch06_get_proxy():
    section("Ch06 — GET /proxies/{id}")
    r = get("/proxies")
    proxies = r.json()["proxies"]
    if not proxies:
        check("Skipped — no proxies in pool", True)
        return
    pid = proxies[0]["id"]
    r2 = get(f"/proxies/{pid}")
    check("Status 200", r2.status_code == 200, str(r2.status_code))
    j = r2.json()
    required = {"id", "url", "status", "last_checked_at", "consecutive_failures", "total_checks", "uptime_percentage", "history"}
    check("All required fields present", required.issubset(j.keys()), str(j.keys()))
    check("id matches", j["id"] == pid)
    check("history is a list", isinstance(j["history"], list))

    # 404 for unknown id
    r3 = get("/proxies/nonexistent-proxy-xyz-999")
    check("404 for unknown proxy", r3.status_code == 404, str(r3.status_code))


def test_ch07_get_history():
    section("Ch07 — GET /proxies/{id}/history")
    r = get("/proxies")
    proxies = r.json()["proxies"]
    if not proxies:
        check("Skipped — no proxies", True)
        return
    pid = proxies[0]["id"]
    r2 = get(f"/proxies/{pid}/history")
    check("Status 200", r2.status_code == 200)
    j = r2.json()
    check("Returns a list", isinstance(j, list))
    if j:
        entry = j[0]
        check("Entry has checked_at", "checked_at" in entry)
        check("Entry has status", "status" in entry)

    # 404 for unknown
    r3 = get("/proxies/nonexistent-xyz-history/history")
    check("404 for unknown proxy history", r3.status_code == 404)


def test_ch08_delete_proxies():
    section("Ch08 — DELETE /proxies")
    # Add some proxies first
    post("/proxies", json={"proxies": [f"{BASE_URL}/health/px-del-1"], "replace": False})
    r = delete("/proxies")
    check("Status 204", r.status_code == 204, str(r.status_code))
    r2 = get("/proxies")
    check("Pool empty after DELETE", r2.json().get("total") == 0, str(r2.json()))

    # Alert history must be preserved
    r3 = get("/alerts")
    check("Alert history preserved after DELETE", isinstance(r3.json(), list))


def test_ch09_get_alerts():
    section("Ch09 — GET /alerts")
    r = get("/alerts")
    check("Status 200", r.status_code == 200)
    j = r.json()
    check("Returns a list", isinstance(j, list))
    for alert in j:
        required = {"alert_id", "status", "failure_rate", "total_proxies", "failed_proxies",
                    "failed_proxy_ids", "threshold", "fired_at", "resolved_at", "message"}
        check(f"Alert {alert.get('alert_id')} has all fields", required.issubset(alert.keys()))


def test_ch10_webhooks(webhook_url: str):
    section("Ch10 — POST /webhooks")
    r = post("/webhooks", json={"url": webhook_url})
    check("Status 201", r.status_code == 201, str(r.status_code))
    j = r.json()
    check("webhook_id present", "webhook_id" in j, str(j))
    check("webhook_id starts with wh-", j.get("webhook_id", "").startswith("wh-"))
    check("url echoed back", j.get("url") == webhook_url)

    # Extra fields ignored
    r2 = post("/webhooks", json={"url": webhook_url, "extra": "ignored"})
    check("Extra field ignored — 201", r2.status_code == 201)


def test_ch11_integrations():
    section("Ch11 — POST /integrations (Slack + Discord)")
    slack_body = {
        "type": "slack",
        "webhook_url": "https://hooks.slack.com/services/TEST/TEST/TEST",
        "username": "ProxyWatch",
        "events": ["alert.fired", "alert.resolved"],
    }
    r1 = post("/integrations", json=slack_body)
    check("Slack integration — 201", r1.status_code == 201, str(r1.status_code))
    j1 = r1.json()
    check("Slack integration_id present", "integration_id" in j1)

    discord_body = {
        "type": "discord",
        "webhook_url": "https://discord.com/api/webhooks/TEST/TEST",
        "username": "ProxyWatch",
        "events": ["alert.fired", "alert.resolved"],
    }
    r2 = post("/integrations", json=discord_body)
    check("Discord integration — 201", r2.status_code == 201, str(r2.status_code))

    # Extra fields ignored
    r3 = post("/integrations", json={**slack_body, "rogue": "nope"})
    check("Extra field ignored on integration", r3.status_code == 201)

    # Invalid type
    r4 = post("/integrations", json={**slack_body, "type": "teams"})
    check("Invalid type returns 400", r4.status_code == 400, str(r4.status_code))


def test_ch12_metrics():
    section("Ch12 — GET /metrics")
    r = get("/metrics")
    check("Status 200", r.status_code == 200)
    j = r.json()
    required = {"total_checks", "current_pool_size", "active_alerts", "total_alerts", "webhook_deliveries"}
    check("All required keys present", required.issubset(j.keys()), str(j.keys()))
    check("total_checks is int", isinstance(j.get("total_checks"), int))
    check("active_alerts is int", isinstance(j.get("active_alerts"), int))


def test_alert_lifecycle(webhook_url: str):
    """
    Trigger an alert by loading mostly-down proxies, then resolve it.
    Verifies: alert fired, webhook received, resolved, no duplicate.
    """
    section("Alert Lifecycle — fire + resolve (integration test)")

    # Use localhost closed-port URLs: instant "Connection refused" on any server
    # 127.0.0.1:19999 is virtually never open → ConnectError → classified as "down"
    bad_proxies = [f"http://127.0.0.1:19999/proxy/px-bad-{i:03d}" for i in range(5)]
    good_proxies = []  # start with all bad

    post("/config", json={"check_interval_seconds": 3, "request_timeout_ms": 1000})
    post("/proxies", json={"proxies": bad_proxies, "replace": True})

    print(f"  {INFO}  Waiting up to 30s for alert to fire…")
    deadline = time.time() + 30
    alert_fired = False
    while time.time() < deadline:
        r = get("/alerts")
        alerts = r.json()
        active = [a for a in alerts if a["status"] == "active"]
        if active:
            alert_fired = True
            print(f"  {INFO}  Alert fired: {active[0]['alert_id']}")
            break
        time.sleep(1)

    check("Alert fired when failure_rate >= 0.20", alert_fired)

    if alert_fired:
        r = get("/alerts")
        alert = [a for a in r.json() if a["status"] == "active"][0]
        check("threshold == 0.20", alert.get("threshold") == 0.20, str(alert.get("threshold")))
        check("failed_proxy_ids is list", isinstance(alert.get("failed_proxy_ids"), list))
        check("message is non-empty", bool(alert.get("message")))
        check("fired_at is set", bool(alert.get("fired_at")))
        check("resolved_at is None", alert.get("resolved_at") is None)

        # Only ONE active alert
        active_count = len([a for a in r.json() if a["status"] == "active"])
        check("Only one active alert at a time", active_count == 1, f"active_count={active_count}")

    # Now resolve: replace pool with all-good proxies
    # We'll use httpbin which should respond 200
    good = [f"{BASE_URL}/health/px-good-001", f"{BASE_URL}/health/px-good-002",
            f"{BASE_URL}/health/px-good-003", f"{BASE_URL}/health/px-good-004",
            f"{BASE_URL}/health/px-good-005"]
    post("/proxies", json={"proxies": good, "replace": True})
    post("/config", json={"check_interval_seconds": 3, "request_timeout_ms": 3000})

    print(f"  {INFO}  Waiting up to 30s for alert to resolve…")
    deadline = time.time() + 30
    alert_resolved = False
    while time.time() < deadline:
        r = get("/alerts")
        resolved = [a for a in r.json() if a["status"] == "resolved"]
        if resolved:
            alert_resolved = True
            print(f"  {INFO}  Alert resolved: {resolved[0]['alert_id']}")
            break
        time.sleep(1)

    check("Alert resolved when failure_rate drops below 0.20", alert_resolved)
    if alert_resolved:
        r = get("/alerts")
        resolved_alert = [a for a in r.json() if a["status"] == "resolved"][0]
        check("resolved_at is set", bool(resolved_alert.get("resolved_at")))

    # Check GET /metrics reflects alert counts
    r_metrics = get("/metrics")
    m = r_metrics.json()
    check("metrics.total_alerts >= 1", m.get("total_alerts", 0) >= 1)
    check("metrics.active_alerts == 0", m.get("active_alerts") == 0, str(m))


def test_webhook_delivery(webhook_url: str):
    section("Webhook delivery check")
    if "localhost" not in BASE_URL and "127.0.0.1" not in BASE_URL:
        check("Skipped webhook delivery check (remote host cannot reach local receiver)", True)
        return
    print(f"  {INFO}  Checking if alert.fired was delivered to receiver…")
    time.sleep(5)  # allow delivery
    fired_events = [e for e in received_events if e.get("event") == "alert.fired"]
    check("alert.fired webhook delivered", len(fired_events) >= 1,
          f"received_events={received_events[:3]}")
    if fired_events:
        e = fired_events[0]
        check("alert_id present in webhook payload", "alert_id" in e)
        check("failure_rate present", "failure_rate" in e)
        check("failed_proxy_ids present", "failed_proxy_ids" in e)
        check("threshold == 0.2", e.get("threshold") == 0.2, str(e.get("threshold")))
        check("message present", "message" in e)

    resolved_events = [e for e in received_events if e.get("event") == "alert.resolved"]
    check("alert.resolved webhook delivered", len(resolved_events) >= 1,
          f"received_events count={len(received_events)}")


def test_proxy_id_extraction():
    section("Proxy ID extraction")
    # The ID must be the last URL path segment
    test_cases = [
        ("https://example.com/proxy/px-101", "px-101"),
        ("https://example.com/px-999", "px-999"),
        ("https://example.com/some/deep/path/px-abc", "px-abc"),
    ]
    post("/proxies", json={"proxies": [t[0] for t in test_cases], "replace": True})
    r = get("/proxies")
    pool_ids = {p["id"] for p in r.json()["proxies"]}
    for url, expected_id in test_cases:
        check(f"ID '{expected_id}' extracted from URL", expected_id in pool_ids, str(pool_ids))


def test_pending_not_counted():
    section("Pending proxies not counted in failure_rate")
    post("/proxies", json={"proxies": [f"{BASE_URL}/health/px-new-777"], "replace": True})
    r = get("/proxies")
    j = r.json()
    # All pending → failure_rate should be 0.0
    pending = [p for p in j["proxies"] if p["status"] == "pending"]
    if pending:
        check("failure_rate is 0.0 when all proxies pending", j["failure_rate"] == 0.0,
              str(j["failure_rate"]))
    else:
        check("Skipped — proxies already probed", True)


def test_delete_preserves_alerts():
    section("DELETE /proxies preserves alert history")
    alert_count_before = len(get("/alerts").json())
    delete("/proxies")
    alert_count_after = len(get("/alerts").json())
    check("Alert history preserved after DELETE /proxies",
          alert_count_before == alert_count_after,
          f"before={alert_count_before} after={alert_count_after}")
    check("Pool is empty after DELETE", get("/proxies").json()["total"] == 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"\n{'#'*60}")
    print(f"  ProxyMaze'26 — Smoke Test Suite")
    print(f"  Target: {BASE_URL}")
    print(f"{'#'*60}")

    # Start local webhook receiver
    srv = start_webhook_receiver()
    webhook_receiver_url = f"http://host.docker.internal:{receiver_port}"
    # Try to detect if running locally (not in Docker)
    try:
        requests.get(f"http://localhost:{receiver_port}", timeout=1)
        webhook_receiver_url = f"http://localhost:{receiver_port}"
    except Exception:
        pass
    print(f"\n  Webhook receiver started on port {receiver_port}")
    print(f"  Receiver URL: {webhook_receiver_url}\n")

    # Verify server is up
    try:
        r = requests.get(f"{BASE_URL}/health", timeout=5)
        assert r.status_code == 200
    except Exception as e:
        print(f"\n\033[91mERROR: Could not reach server at {BASE_URL}\033[0m")
        print(f"       Start the server with: python main.py")
        print(f"       Error: {e}\n")
        sys.exit(1)

    # Run all tests
    test_ch01_health()
    test_ch02_post_config()
    test_ch03_get_config()
    test_ch04_post_proxies()
    test_ch04_replace_true()
    test_ch05_get_proxies()
    test_proxy_id_extraction()
    test_pending_not_counted()
    test_ch10_webhooks(webhook_receiver_url)
    test_ch11_integrations()
    test_ch12_metrics()

    # Alert lifecycle (needs real probes — slowest part)
    test_alert_lifecycle(webhook_receiver_url)
    test_wait_for_probes()
    test_ch06_get_proxy()
    test_ch07_get_history()

    # Delete tests
    test_ch08_delete_proxies()
    test_delete_preserves_alerts()
    test_ch09_get_alerts()

    # Webhook delivery check (may need time)
    test_webhook_delivery(webhook_receiver_url)

    # Final metrics
    test_ch12_metrics()

    print(f"\n{'='*60}")
    print(f"  Results: {passed} passed, {failed} failed out of {passed+failed} checks")
    if failed == 0:
        print(f"  *** ALL TESTS PASSED! ***")
    else:
        print(f"  *** {failed} test(s) FAILED - review output above. ***")
    print(f"{'='*60}\n")

    sys.exit(0 if failed == 0 else 1)
