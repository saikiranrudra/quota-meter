# Quota Meter

A high-performance, multi-tenant API quota management system built with **Flask**, **Redis**, and **PostgreSQL**. It enforces per-organisation, per-feature usage limits with atomic check-and-deduct operations powered by Redis Lua scripts, supporting sustained throughput of ~2,000 ops/sec and peak loads of 10,000+ ops/sec across a horizontally-scaled fleet.

---

## Table of Contents

- [Brief Overview](#brief-overview)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [How It Works](#how-it-works)
- [API Reference](#api-reference)
- [Running Tests](#running-tests)

---

## Brief Overview

Quota Meter solves the problem of **tracking and enforcing resource quotas** for multi-tenant SaaS APIs. Each customer organisation is assigned per-feature monthly limits (e.g., *container-tracking: 150 requests/month*), and the system atomically deducts units on every API call — rejecting requests that would exceed the quota.

### Key Capabilities

| Capability | Description |
|---|---|
| **Multi-Tenant Isolation** | Quotas are tracked independently per `org_id` + `feature` pair |
| **Atomic Check-and-Deduct** | A single Redis Lua script checks remaining quota and deducts in one atomic operation — no race conditions |
| **Automatic Monthly Reset** | Quota counters roll over to a new calendar month automatically, handled inside the Lua script itself |
| **Batch Operations** | A single request can consume multiple units (e.g., tracking 100 containers = 100 units) |
| **Idempotency** | Client retries with an `Idempotency-Key` header are safe — the system replays the cached result without double-deducting |
| **Refund on Failure** | If downstream logic fails after a deduction, units are automatically refunded |
| **Usage Reporting** | A read-only endpoint returns current usage, limit, remaining balance, and next reset timestamp |
| **Horizontally Scalable** | Flask instances are fully stateless; all quota state lives in Redis, allowing linear scaling behind a load balancer |

### Tech Stack

- **Python 3.11** with **Flask** web framework
- **Redis 7** — real-time quota state (Lua scripts for atomicity)
- **PostgreSQL 15** — persistent storage for quota configurations (default limits per org/feature)
- **Gunicorn + gevent** — async worker model for high concurrency
- **Docker Compose** — single-command deployment of the entire stack

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          CLIENT REQUEST                             │
│                   (X-Org-Id + Idempotency-Key headers)              │
└────────────────────────────┬─────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     LOAD BALANCER (nginx / ALB)                     │
│                  (for horizontal scaling; optional)                  │
└──────┬─────────────┬──────────────┬─────────────┬───────────────────┘
       │             │              │             │
       ▼             ▼              ▼             ▼
┌────────────┐┌────────────┐┌────────────┐┌────────────┐
│  Flask #1  ││  Flask #2  ││  Flask #3  ││  Flask #N  │  ← Stateless
│  Gunicorn  ││  Gunicorn  ││  Gunicorn  ││  Gunicorn  │    Workers
│  + gevent  ││  + gevent  ││  + gevent  ││  + gevent  │
└─────┬──────┘└─────┬──────┘└─────┬──────┘└─────┬──────┘
      │             │             │             │
      │    ┌────────┴─────────────┴─────────┐   │
      │    │    @quota_required decorator    │   │
      │    │  ┌───────────────────────────┐  │   │
      │    │  │  1. Resolve org_id        │  │   │
      │    │  │  2. Check idempotency     │  │   │
      │    │  │  3. Call QuotaEngine      │  │   │
      │    │  │  4. Allow / Reject (429)  │  │   │
      │    │  │  5. Refund on failure     │  │   │
      │    │  └───────────────────────────┘  │   │
      │    └────────┬─────────────┬─────────┘   │
      │             │             │             │
      └─────────────┴──────┬──────┴─────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
┌──────────────────────┐   ┌──────────────────────┐
│     REDIS 7          │   │   POSTGRESQL 15       │
│  (Hot Path)          │   │  (Cold Path)          │
│                      │   │                       │
│ ┌──────────────────┐ │   │ ┌───────────────────┐ │
│ │  Quota Hashes    │ │   │ │   quotas table    │ │
│ │  quota:{org}:{f} │ │   │ │                   │ │
│ │  ├─ limit        │ │   │ │  id (UUID PK)     │ │
│ │  ├─ used         │ │   │ │  org_id           │ │
│ │  └─ period       │ │   │ │  feature          │ │
│ └──────────────────┘ │   │ │  default_limit    │ │
│                      │   │ └───────────────────┘ │
│ ┌──────────────────┐ │   │                       │
│ │  Default Limits  │ │   │  Source of truth for   │
│ │  default_limit:  │ │   │  provisioned limits;   │
│ │   {org}:{feat}   │ │   │  synced to Redis on    │
│ └──────────────────┘ │   │  startup via           │
│                      │   │  /warm-cache            │
│ ┌──────────────────┐ │   │                       │
│ │  Idempotency     │ │   └───────────────────────┘
│ │  idem:{org}:     │ │
│ │   {feat}:{key}   │ │
│ │  TTL = 24h       │ │
│ └──────────────────┘ │
│                      │
│  Lua Scripts:        │
│  • CHECK_AND_DEDUCT  │
│  • REFUND            │
│  • GET_USAGE         │
└──────────────────────┘
```

### Data Flow Summary

```
Request ──► @quota_required ──► QuotaEngine.check_and_deduct()
                                        │
                                        ▼
                                Redis Lua Script
                                (atomic: check remaining → deduct → return)
                                        │
                              ┌─────────┴──────────┐
                              │                    │
                         allowed=1            allowed=0
                              │                    │
                         Run endpoint         Return 429
                              │                    (Quota Exceeded)
                         ┌────┴────┐
                         │         │
                      Success    Failure
                         │         │
                      Return    engine.refund()
                      response  + re-raise
```

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (v20+)
- [Docker Compose](https://docs.docker.com/compose/install/) (v2+)

### Spin Up the Application

The entire stack — Flask app, PostgreSQL, and Redis — launches with a **single command**:

```bash
docker compose up --build
```

This will:

1. **Build** the Flask application Docker image (Python 3.11-slim)
2. **Start PostgreSQL** (port `5432`) and wait for it to be healthy
3. **Start Redis** (port `6379`) and wait for it to be healthy
4. **Run database migrations** (`flask db upgrade`)
5. **Seed the database** with sample org/feature quotas (MSC, MAERSK, COSC, ONE)
6. **Start Gunicorn** with 5 gevent workers (1000 connections each) on port `5000`
7. **Warm the Redis cache** by syncing all default limits from PostgreSQL → Redis via `POST /warm-cache`

The app is accessible at **`http://localhost:8080`** once you see:

```
✅ Flask is ready!
✅ Cache warmed: {"status":"ok","synced":8}
```

### Stopping the Application

```bash
docker compose down
```

To also wipe all persisted data (PostgreSQL + Redis volumes):

```bash
docker compose down -v
```

---

## How It Works

### 1. Quota Configuration (PostgreSQL → Redis)

Default limits are stored in the `quotas` PostgreSQL table as `(org_id, feature, default_limit)` tuples. On application startup, the `entrypoint.sh` script:

1. Waits for PostgreSQL and Redis to become reachable
2. Runs Alembic migrations to ensure the schema is up to date
3. Seeds the database with initial quota configurations (`seed.py`)
4. Starts Gunicorn, then calls `POST /warm-cache` to paginate through all `Quota` rows and write each limit into a Redis key (`default_limit:{org_id}:{feature}`)

This means **Redis is the runtime source of truth** for quota limits, while PostgreSQL is the durable configuration store.

### 2. Quota Enforcement (The Hot Path)

Every protected endpoint is decorated with `@quota_required`:

```python
@api.route("/containers", methods=["POST"])
@quota_required(
    feature="container-tracking",
    amount=lambda: len(request.get_json(force=True).get("containers", [])),
)
def track_containers():
    ...
```

When a request arrives, the decorator:

1. **Resolves the org** from the `X-Org-Id` header
2. **Checks idempotency** — if an `Idempotency-Key` header is present and was seen before, replays the cached result without re-deducting
3. **Calls `QuotaEngine.check_and_deduct()`**, which executes the `CHECK_AND_DEDUCT` Lua script in Redis:
   - Loads the hash `quota:{org_id}:{feature}` (or initialises it from the cached default limit)
   - Rolls over the counter if the stored period doesn't match the current calendar month
   - Checks if `remaining >= amount`
   - If yes: atomically increments `used` and returns `allowed=1`
   - If no: returns `allowed=0` without partial deduction (all-or-nothing policy)
4. **On `allowed=0`**: returns HTTP 429 with remaining/limit/period details
5. **On `allowed=1`**: runs the actual endpoint logic
6. **On downstream failure**: automatically refunds the deducted units via the `REFUND` Lua script

### 3. Atomicity via Redis Lua Scripts

All quota mutations happen inside three Lua scripts (`scripts.py`) that Redis executes atomically:

| Script | Purpose |
|---|---|
| `CHECK_AND_DEDUCT` | Check remaining quota, handle period rollover, and deduct — all in one atomic operation |
| `REFUND` | Return units after a downstream failure; floors at 0 to prevent double-refund exploits |
| `GET_USAGE` | Read-only usage lookup with virtual rollover (reports 0 for stale periods without mutating) |

Because Redis is single-threaded for script execution, these scripts guarantee **strict concurrent correctness** — no distributed locks, no race conditions, and only a single network round trip per operation.

### 4. Automatic Monthly Reset

Period rollover is handled **inside the `CHECK_AND_DEDUCT` Lua script** itself. Each quota hash stores a `period` field (e.g., `"2026-06"`). When a new request arrives with a different current period:

```lua
if period ~= current_period then
    used = 0
    period = current_period
    redis.call('HSET', key, 'used', used, 'period', period)
end
```

This makes reset-and-deduct a single atomic operation, so two requests racing across a month boundary cannot both see "needs reset" and double-reset.

### 5. Idempotency

Clients can send an `Idempotency-Key` header to safely retry failed requests:

- On the first call: the result (`allowed:remaining:used:limit:period`) is cached in Redis with a 24-hour TTL
- On subsequent calls with the same key: the cached result is replayed verbatim — no re-deduction, no re-execution of the endpoint

### 6. Horizontal Scaling

The Flask layer is **fully stateless** — all quota state lives in Redis. This means:

- Adding more Flask/Gunicorn instances behind a load balancer scales throughput linearly
- No sticky sessions are needed
- All instances call `EVAL` against the same Redis, which serialises the Lua scripts atomically regardless of caller count

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Health check (verifies Redis connectivity) |
| `POST` | `/warm-cache` | Sync default limits from PostgreSQL → Redis |
| `PUT` | `/default-limit/<org_id>/<feature>` | Update a default limit (DB + Redis) |
| `GET` | `/usage/<org_id>/<feature>` | Get current usage, limit, remaining, and reset time |
| `POST` | `/containers` | Track containers (quota-protected, multi-unit) |
| `GET` | `/sailing-schedule` | Get sailing schedule (quota-protected, 1 unit) |

### Required Headers

| Header | Required | Description |
|---|---|---|
| `X-Org-Id` | Yes (on protected routes) | Identifies the calling organisation |
| `Idempotency-Key` | No | Enables safe client retries |

### Example Requests

**Check usage:**
```bash
curl http://localhost:8080/usage/MSC/container-tracking
```

**Track containers (consumes quota):**
```bash
curl -X POST http://localhost:8080/containers \
  -H "X-Org-Id: MSC" \
  -H "Idempotency-Key: req-001" \
  -H "Content-Type: application/json" \
  -d '{"containers": ["MSCU1234567", "MSCU7654321"]}'
```

**Update a default limit:**
```bash
curl -X PUT http://localhost:8080/default-limit/MSC/container-tracking \
  -H "Content-Type: application/json" \
  -d '{"default_limit": 200}'
```

---

## Running Tests

The project includes a comprehensive test suite (`test_quota_system.py`) covering correctness, concurrency, idempotency, refunds, and load testing:

```bash
# Run tests against the running application
pytest test_quota_system.py -v
```

> **Note:** The application must be running (`docker compose up`) before executing tests.

### Seeded Organisations

| Org ID | Feature | Default Limit |
|---|---|---|
| `MSC` | container-tracking | 50 |
| `MSC` | sailing-schedule | 50 |
| `MAERSK` | container-tracking | 150 |
| `MAERSK` | sailing-schedule | 150 |
| `COSC` | container-tracking | 130 |
| `COSC` | sailing-schedule | 150 |
| `ONE` | container-tracking | 120 |
| `ONE` | sailing-schedule | 130 |
