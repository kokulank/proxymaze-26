"""Quick test: does our Render server actually deliver webhooks to external URLs?"""
import requests, time

BASE = "https://proxymaze-26.onrender.com"

# Register a webhook pointing to httpbin (will return 200)
r = requests.post(f"{BASE}/webhooks", json={"url": "https://httpbin.org/post"})
print(f"Register webhook: {r.status_code} {r.json()}")

# Replace pool with bad proxies to trigger breach
bad = [f"http://127.0.0.1:19999/px-bad-{i}" for i in range(5)]
r = requests.post(f"{BASE}/proxies", json={"proxies": bad, "replace": True})
print(f"POST bad proxies: {r.status_code}")

# Set fast check interval
requests.post(f"{BASE}/config", json={"check_interval_seconds": 3, "request_timeout_ms": 1000})

print("Waiting 20s for alert + delivery...")
time.sleep(20)

# Check alerts
r = requests.get(f"{BASE}/alerts")
alerts = r.json()
active = [a for a in alerts if a["status"] == "active"]
print(f"Total alerts: {len(alerts)}, Active: {len(active)}")
for a in alerts[-3:]:
    aid = a["alert_id"]
    st = a["status"]
    print(f"  {aid}: status={st}")

# Check metrics — webhook_deliveries should be > 0
r = requests.get(f"{BASE}/metrics")
m = r.json()
print(f"Metrics: {m}")
print(f"  webhook_deliveries = {m.get('webhook_deliveries', 'MISSING')}")

if m.get("webhook_deliveries", 0) > 0:
    print("\n*** OUTBOUND DELIVERY WORKS! ***")
else:
    print("\n*** DELIVERY FAILED — webhook_deliveries is 0 ***")
