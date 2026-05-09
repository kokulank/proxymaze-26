# ProxyMaze'26
**Real-Time Proxy Intelligence API**

*Built for: Torch Labs Sri Lanka 2026 Engineering Challenge*
*Stack: Python 3.11+, FastAPI, uvicorn, httpx*

---

## Overview

**Live Deployment:** [https://proxymaze-26.onrender.com](https://proxymaze-26.onrender.com)

ProxyMaze'26 is a production-quality HTTP API that continuously monitors a pool of proxy URLs via real HTTP probes. It tracks the up, down, and pending status of each proxy and automatically fires threshold-based alerts (when the failure rate reaches >= 20%). The service handles reliable webhook delivery with exponential retry, and natively supports Slack and Discord integrations for real-time alerting.

## Requirements

- Python 3.11 or higher
- pip

## Installation & Running

```bash
pip install -r requirements.txt
python main.py
```

The server starts on `http://localhost:8080` by default. You can override the port using the `PORT` environment variable:

```bash
# Linux/Mac
PORT=9090 python main.py

# Windows PowerShell
$env:PORT=9090; python main.py
```

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Service health check |
| POST | `/config` | Set monitoring interval and timeout |
| GET | `/config` | Get current config |
| POST | `/proxies` | Add proxies to monitoring pool |
| GET | `/proxies` | Get pool status and all proxy states |
| GET | `/proxies/{id}` | Get single proxy details and history |
| GET | `/proxies/{id}/history` | Get probe history for a proxy |
| DELETE | `/proxies` | Clear the proxy pool |
| GET | `/alerts` | Get all alerts (active and resolved) |
| POST | `/webhooks` | Register a webhook receiver URL |
| POST | `/integrations` | Register Slack or Discord integration |
| GET | `/metrics` | Get operational metrics |
| GET | `/docs` | Interactive Swagger UI (auto-generated) |

## Key Behaviours

- **Proxy IDs** extracted from last URL path segment
- **Pending proxies** excluded from failure rate calculation
- **Alert threshold:** 0.20 (fires when failure_rate >= 0.20, resolves when < 0.20)
- **At most one active alert** at any time
- **Webhook delivery retried** on 500/502/503/504 with backoff
- **Exactly one successful delivery** per state transition (no duplicates)
- **All timestamps** ISO 8601 UTC
- **Unknown JSON fields** silently ignored on all endpoints

## Bonus Integrations

- **Slack:** `POST /integrations` with type `"slack"` — delivers formatted Slack payload with attachments, color, fields, and integer `ts`
- **Discord:** `POST /integrations` with type `"discord"` — delivers Discord embed payload with integer `color`

## Running Tests

### Local Testing
```bash
# Start the server first (in one terminal)
python main.py

# In a second terminal:
python test_all.py
```

### Testing the Live Deployment
You can point the test suite directly at the live Render API using the `BASE_URL` environment variable:

```powershell
# Windows PowerShell
$env:BASE_URL="https://proxymaze-26.onrender.com"
python test_all.py
```

```bash
# Linux/Mac
BASE_URL="https://proxymaze-26.onrender.com" python test_all.py
```

*Expected output: 87 passed, 0 failed*

## Deploying to Render

- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Environment Variable:** `PORT` (Render sets this automatically)

## Project Structure

```text
Backend/
├── main.py              # Main FastAPI application and logic
├── requirements.txt     # Python dependencies
├── test_all.py          # Comprehensive test suite
├── .gitignore           # Git ignore file
└── README.md            # Project documentation
```
