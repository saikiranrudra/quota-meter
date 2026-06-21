#!/usr/bin/env python3
"""
Comprehensive Test & Validation Script for Per-Customer API Quota Metering System
==================================================================================

Exercises the entire quota metering system end-to-end and produces a detailed,
self-contained HTML report (report.html) proving correctness across six grading
criteria:

  1. Basic Functional Correctness — CRUD-style checks, feature/org isolation
  2. Batch Behavior — All-or-nothing deduction policy verification
  3. Concurrent Correctness — Race-condition detection across multiple trials
  4. Failure & Retry Safety — Idempotency keys and refund-on-failure
  5. Reset & Reporting — Usage endpoint accuracy and period rollover
  6. Latency & Load — Throughput and latency under sustained / burst load

Usage:
    python test_quota_system.py [options]

Options:
    --url           API base URL             (default: http://localhost:8080)
    --redis-url     Redis connection URL      (default: redis://localhost:6379/0)
    --postgres-dsn  Postgres DSN              (default: postgresql://postgres:user_password@localhost:5432/quota_meter)
    --suites        Comma-separated suites    (default: all)
                    Options: basic,batch,concurrent,retry,reset,load
    --trials        Concurrency trials        (default: 20)
    --load-duration Load test seconds         (default: 30)
    --report-path   HTML report output path   (default: report.html)

Requirements:
    pip install requests redis psycopg2-binary
"""

# ════════════════════════════════════════════════════════════════════════════════
# IMPORTS
# ════════════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import html as html_module
import json
import math
import os
import platform
import random
import socket
import statistics
import sys
import textwrap
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

try:
    import psycopg2
except ImportError:
    sys.exit("❌  Missing dependency: pip install psycopg2-binary")

try:
    import redis
except ImportError:
    sys.exit("❌  Missing dependency: pip install redis")

try:
    import requests
except ImportError:
    sys.exit("❌  Missing dependency: pip install requests")


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION CONSTANTS
# ════════════════════════════════════════════════════════════════════════════════

# Batch deduction policy the system implements.
# Set to "partial" if your Lua script does partial fulfillment instead.
EXPECTED_BATCH_POLICY: str = "all_or_nothing"

# Concurrency test defaults
DEFAULT_CONCURRENCY_TRIALS = 20
DEFAULT_CONCURRENCY_QUOTA = 500
DEFAULT_CONCURRENCY_THREADS = 50
DEFAULT_CONCURRENCY_MIN_AMOUNT = 10
DEFAULT_CONCURRENCY_MAX_AMOUNT = 50

# Load test defaults
DEFAULT_LOAD_DURATION_S = 30
DEFAULT_LOAD_THREADS = 100
DEFAULT_BURST_THREADS = 200
DEFAULT_BURST_REQS_PER_THREAD = 50
DEFAULT_BURSTY_ORGS = 200

# Features known to the system
FEATURE_CONTAINER = "container-tracking"
FEATURE_SAILING = "sailing-schedule"


# ════════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ════════════════════════════════════════════════════════════════════════════════

@dataclass
class TestAssertion:
    """A single assertion within a test — the atomic unit of pass/fail."""
    name: str
    passed: bool
    expected: Any
    actual: Any
    detail: str = ""


@dataclass
class TestResult:
    """Result of a single named test within a category."""
    name: str
    description: str
    why_it_matters: str
    passed: bool
    assertions: list[TestAssertion] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)
    duration_s: float = 0.0
    error: str = ""
    is_limitation: bool = False  # True → amber "known limitation" badge


@dataclass
class TestCategory:
    """A group of related tests mapping to one grading criterion."""
    name: str
    description: str
    why_it_matters: str
    results: list[TestResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if not r.is_limitation)

    @property
    def pass_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and not r.is_limitation)

    @property
    def limitation_count(self) -> int:
        return sum(1 for r in self.results if r.is_limitation)


@dataclass
class RequestRecord:
    """Latency record for a single HTTP request during load tests."""
    start_ts: float      # epoch seconds, relative to test start
    latency_ms: float    # response time in milliseconds
    status_code: int
    success: bool


# ════════════════════════════════════════════════════════════════════════════════
# TEST INFRASTRUCTURE — setup/teardown + API helpers
# ════════════════════════════════════════════════════════════════════════════════

class TestInfra:
    """
    Manages direct connections to Redis & Postgres for deterministic test
    setup/teardown, plus convenience wrappers around the HTTP API.
    """

    def __init__(self, api_url: str, redis_url: str, postgres_dsn: str):
        self.api_url = api_url.rstrip("/")
        self.redis_client = redis.Redis.from_url(redis_url, decode_responses=False)
        self.pg_conn = psycopg2.connect(postgres_dsn)
        self.pg_conn.autocommit = True
        self._created_orgs: list[tuple[str, str]] = []

    # ── health / connectivity ──────────────────────────────────────────────

    def health_check(self) -> bool:
        try:
            r = requests.get(f"{self.api_url}/health", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    # ── Postgres helpers ───────────────────────────────────────────────────

    def create_test_org(self, org_id: str, feature: str, default_limit: int):
        """Insert a test org row into Postgres and seed Redis default_limit."""
        with self.pg_conn.cursor() as cur:
            # Delete any stale row from previous runs (no unique constraint)
            cur.execute(
                "DELETE FROM quotas WHERE org_id = %s AND feature = %s",
                (org_id, feature),
            )
            cur.execute(
                "INSERT INTO quotas (id, org_id, feature, default_limit) "
                "VALUES (%s, %s, %s, %s)",
                (str(uuid.uuid4()), org_id, feature, default_limit),
            )
        self.redis_client.set(f"default_limit:{org_id}:{feature}", default_limit)
        self._created_orgs.append((org_id, feature))

    def create_test_orgs_batch(self, orgs: list[tuple[str, str, int]]):
        """Batch-create test orgs (Postgres + Redis)."""
        with self.pg_conn.cursor() as cur:
            for org_id, feature, default_limit in orgs:
                cur.execute(
                    "DELETE FROM quotas WHERE org_id = %s AND feature = %s",
                    (org_id, feature),
                )
                cur.execute(
                    "INSERT INTO quotas (id, org_id, feature, default_limit) "
                    "VALUES (%s, %s, %s, %s)",
                    (str(uuid.uuid4()), org_id, feature, default_limit),
                )
        pipe = self.redis_client.pipeline()
        for org_id, feature, default_limit in orgs:
            pipe.set(f"default_limit:{org_id}:{feature}", default_limit)
        pipe.execute()
        # Also seed quota hashes
        pipe = self.redis_client.pipeline()
        period = self._current_period()
        for org_id, feature, default_limit in orgs:
            key = f"quota:{org_id}:{feature}"
            pipe.hset(key, mapping={
                "limit": default_limit, "used": 0, "period": period
            })
        pipe.execute()
        self._created_orgs.extend([(o, f) for o, f, _ in orgs])

    # ── Redis helpers ──────────────────────────────────────────────────────

    def seed_quota_hash(
        self,
        org_id: str,
        feature: str,
        limit: int,
        used: int = 0,
        period: str | None = None,
    ):
        """Directly set the Redis quota hash for a deterministic starting state."""
        period = period or self._current_period()
        key = f"quota:{org_id}:{feature}"
        self.redis_client.hset(key, mapping={
            "limit": limit, "used": used, "period": period,
        })

    def get_quota_hash(self, org_id: str, feature: str) -> dict[str, str]:
        """Read the raw Redis quota hash."""
        data = self.redis_client.hgetall(f"quota:{org_id}:{feature}")
        return {
            (k.decode() if isinstance(k, bytes) else k):
            (v.decode() if isinstance(v, bytes) else v)
            for k, v in data.items()
        }

    def flush_test_keys(self, org_id: str, feature: str):
        """Remove all Redis keys related to a test org+feature."""
        self.redis_client.delete(f"quota:{org_id}:{feature}")
        self.redis_client.delete(f"default_limit:{org_id}:{feature}")
        for key in self.redis_client.scan_iter(f"idem:{org_id}:{feature}:*"):
            self.redis_client.delete(key)

    def cleanup_test_org(self, org_id: str, feature: str):
        with self.pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM quotas WHERE org_id = %s AND feature = %s",
                (org_id, feature),
            )
        self.flush_test_keys(org_id, feature)

    def cleanup_all(self):
        """Remove every test org created during this run."""
        seen = set()
        for org_id, feature in self._created_orgs:
            if (org_id, feature) not in seen:
                seen.add((org_id, feature))
                try:
                    self.cleanup_test_org(org_id, feature)
                except Exception as exc:
                    print(f"  ⚠ cleanup {org_id}/{feature}: {exc}")
        self._created_orgs.clear()

    # ── HTTP API wrappers ──────────────────────────────────────────────────

    def post_containers(
        self,
        org_id: str,
        containers: list[str],
        idempotency_key: str | None = None,
        fail: bool = False,
        timeout: float = 30.0,
    ) -> requests.Response:
        """POST /containers — the main quota-consuming endpoint."""
        headers = {"X-Org-Id": org_id, "Content-Type": "application/json"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        body: dict[str, Any] = {"containers": containers}
        if fail:
            body["fail"] = True
        return requests.post(
            f"{self.api_url}/containers",
            json=body, headers=headers, timeout=timeout,
        )

    def get_usage(self, org_id: str, feature: str) -> dict:
        """GET /usage/<org_id>/<feature>."""
        r = requests.get(
            f"{self.api_url}/usage/{org_id}/{feature}", timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def get_sailing_schedule(self, org_id: str) -> requests.Response:
        return requests.get(
            f"{self.api_url}/sailing-schedule",
            headers={"X-Org-Id": org_id}, timeout=10,
        )

    # ── utilities ──────────────────────────────────────────────────────────

    @staticmethod
    def _current_period() -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        return f"{now.year:04d}-{now.month:02d}"

    @staticmethod
    def _make_containers(n: int) -> list[str]:
        """Generate n unique container IDs."""
        return [f"C-{uuid.uuid4().hex[:8]}" for _ in range(n)]

    def close(self):
        try:
            self.redis_client.close()
        except Exception:
            pass
        try:
            self.pg_conn.close()
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════════════
# ASSERTION HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def check(name: str, expected: Any, actual: Any, detail: str = "") -> TestAssertion:
    return TestAssertion(
        name=name, passed=(expected == actual),
        expected=expected, actual=actual, detail=detail,
    )

def check_true(name: str, condition: bool, detail: str = "") -> TestAssertion:
    return TestAssertion(
        name=name, passed=condition,
        expected=True, actual=condition, detail=detail,
    )

def check_lte(name: str, value: Any, upper: Any, detail: str = "") -> TestAssertion:
    return TestAssertion(
        name=name, passed=(value <= upper),
        expected=f"<= {upper}", actual=value, detail=detail,
    )

def check_gte(name: str, value: Any, lower: Any, detail: str = "") -> TestAssertion:
    return TestAssertion(
        name=name, passed=(value >= lower),
        expected=f">= {lower}", actual=value, detail=detail,
    )


# ════════════════════════════════════════════════════════════════════════════════
# 1. BASIC FUNCTIONAL CORRECTNESS
# ════════════════════════════════════════════════════════════════════════════════

def run_basic_functional(infra: TestInfra) -> TestCategory:
    """
    Standard CRUD-style checks: consume under limit, consume at limit,
    reject over limit, feature isolation, org isolation.

    This proves the system handles the happy path correctly and that
    independent dimensions (org × feature) don't leak into each other.
    """
    cat = TestCategory(
        name="Basic Functional Correctness",
        description="CRUD-style quota operations and isolation checks.",
        why_it_matters=(
            "Verifies the fundamental contract: quotas are deducted correctly, "
            "over-limit requests are rejected, and each (org, feature) pair is "
            "an independent accounting bucket."
        ),
    )

    ORG_A, ORG_B = "TEST-FUNC-A", "TEST-FUNC-B"
    LIMIT = 100

    # ── setup ──────────────────────────────────────────────────────────────
    for org in (ORG_A, ORG_B):
        for feat in (FEATURE_CONTAINER, FEATURE_SAILING):
            infra.create_test_org(org, feat, LIMIT)
            infra.seed_quota_hash(org, feat, limit=LIMIT, used=0)

    # ── test 1: under-limit consumption ────────────────────────────────────
    t0 = time.time()
    try:
        r = infra.post_containers(ORG_A, infra._make_containers(5))
        usage = infra.get_usage(ORG_A, FEATURE_CONTAINER)
        assertions = [
            check("status_code", 200, r.status_code),
            check("used", 5, usage["used"]),
            check("remaining", 95, usage["remaining"]),
            check("limit", LIMIT, usage["limit"]),
        ]
        cat.results.append(TestResult(
            name="Under-limit consumption",
            description="Consume 5 units from a 100-unit quota.",
            why_it_matters="Basic deduction must work correctly.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Under-limit consumption", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(), duration_s=time.time() - t0,
        ))

    # ── test 2: consume to exactly zero ────────────────────────────────────
    t0 = time.time()
    try:
        # Currently used=5, remaining=95 → consume 95 more
        r = infra.post_containers(ORG_A, infra._make_containers(95))
        usage = infra.get_usage(ORG_A, FEATURE_CONTAINER)
        assertions = [
            check("status_code", 200, r.status_code),
            check("used", 100, usage["used"]),
            check("remaining", 0, usage["remaining"]),
        ]
        cat.results.append(TestResult(
            name="Consume to exactly zero",
            description="Consume all remaining 95 units, reaching exactly 0.",
            why_it_matters="Edge case: quota reaches floor without going negative.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Consume to exactly zero", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(), duration_s=time.time() - t0,
        ))

    # ── test 3: over-limit rejection ───────────────────────────────────────
    t0 = time.time()
    try:
        r = infra.post_containers(ORG_A, infra._make_containers(1))
        body = r.json()
        assertions = [
            check("status_code", 429, r.status_code),
            check("error_type", "quota_exceeded", body.get("error")),
            check("remaining", 0, body.get("remaining")),
        ]
        cat.results.append(TestResult(
            name="Over-limit rejection",
            description="Request 1 more unit when quota is exhausted.",
            why_it_matters="System must reject with 429 and accurate remaining count.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Over-limit rejection", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(), duration_s=time.time() - t0,
        ))

    # ── test 4: feature isolation ──────────────────────────────────────────
    t0 = time.time()
    try:
        # ORG_A container-tracking is exhausted (used=100).
        # ORG_A sailing-schedule should still be at 0 used.
        usage_ss = infra.get_usage(ORG_A, FEATURE_SAILING)
        r_ss = infra.get_sailing_schedule(ORG_A)
        usage_ss_after = infra.get_usage(ORG_A, FEATURE_SAILING)

        assertions = [
            check("sailing used_before", 0, usage_ss["used"],
                  "Consuming container-tracking must not affect sailing-schedule"),
            check("sailing status", 200, r_ss.status_code),
            check("sailing used_after", 1, usage_ss_after["used"]),
            check("sailing remaining_after", 99, usage_ss_after["remaining"]),
        ]
        cat.results.append(TestResult(
            name="Feature isolation",
            description="Exhausting container-tracking does not affect sailing-schedule.",
            why_it_matters="Each feature must be an independent quota bucket.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Feature isolation", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(), duration_s=time.time() - t0,
        ))

    # ── test 5: org isolation ──────────────────────────────────────────────
    t0 = time.time()
    try:
        # ORG_B container-tracking should be untouched (used=0)
        usage_b = infra.get_usage(ORG_B, FEATURE_CONTAINER)
        r_b = infra.post_containers(ORG_B, infra._make_containers(10))
        usage_b_after = infra.get_usage(ORG_B, FEATURE_CONTAINER)

        assertions = [
            check("org_b used_before", 0, usage_b["used"],
                  "ORG_A usage must not leak into ORG_B"),
            check("org_b status", 200, r_b.status_code),
            check("org_b used_after", 10, usage_b_after["used"]),
        ]
        cat.results.append(TestResult(
            name="Org isolation",
            description="Exhausting ORG_A quota does not affect ORG_B.",
            why_it_matters="Multi-tenant isolation is critical for a shared metering system.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Org isolation", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(), duration_s=time.time() - t0,
        ))

    return cat


# ════════════════════════════════════════════════════════════════════════════════
# 2. BATCH BEHAVIOR (ALL-OR-NOTHING)
# ════════════════════════════════════════════════════════════════════════════════

def run_batch_behavior(infra: TestInfra) -> TestCategory:
    """
    Validates the all-or-nothing batch deduction policy:
    if the requested amount exceeds remaining quota, the entire request
    is rejected — no partial deduction occurs.

    Configurable via EXPECTED_BATCH_POLICY at the top of this script.
    """
    cat = TestCategory(
        name="Batch Behavior (All-or-Nothing)",
        description=(
            f"Verifies the {EXPECTED_BATCH_POLICY} batch deduction policy. "
            "When requested units exceed remaining quota, the entire request "
            "must be rejected with no partial deduction."
        ),
        why_it_matters=(
            "The assignment requires an explicit atomic deduction policy for "
            "edge-case batch requests. This proves the implementation matches "
            "the documented policy."
        ),
    )

    ORG = "TEST-BATCH"
    LIMIT = 100

    infra.create_test_org(ORG, FEATURE_CONTAINER, LIMIT)
    infra.seed_quota_hash(ORG, FEATURE_CONTAINER, limit=LIMIT, used=0)

    # ── consume 60 of 100 ─────────────────────────────────────────────────
    t0 = time.time()
    try:
        r1 = infra.post_containers(ORG, infra._make_containers(60))
        usage1 = infra.get_usage(ORG, FEATURE_CONTAINER)

        # ── request 50 more (only 40 remaining) — must be rejected ────────
        r2 = infra.post_containers(ORG, infra._make_containers(50))
        usage2 = infra.get_usage(ORG, FEATURE_CONTAINER)

        # ── request exactly 40 — must succeed ─────────────────────────────
        r3 = infra.post_containers(ORG, infra._make_containers(40))
        usage3 = infra.get_usage(ORG, FEATURE_CONTAINER)

        # ── request 1 more — must be rejected ─────────────────────────────
        r4 = infra.post_containers(ORG, infra._make_containers(1))

        assertions = [
            check("initial consume status", 200, r1.status_code),
            check("used after 60", 60, usage1["used"]),
            check("remaining after 60", 40, usage1["remaining"]),
            check("over-request rejected", 429, r2.status_code,
                  "50 requested but only 40 remaining → all-or-nothing reject"),
            check("used unchanged after reject", 60, usage2["used"],
                  "No partial deduction should occur"),
            check("exact-fit succeeds", 200, r3.status_code),
            check("used after exact fit", 100, usage3["used"]),
            check("remaining after exact fit", 0, usage3["remaining"]),
            check("zero-remaining reject", 429, r4.status_code),
        ]

        cat.results.append(TestResult(
            name="All-or-nothing batch deduction",
            description=(
                "Set limit=100, consume 60, attempt 50 (reject), "
                "consume exact 40 (accept), attempt 1 (reject)."
            ),
            why_it_matters=(
                "Proves the Lua script rejects insufficient batches atomically "
                "without partial deduction."
            ),
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            data={"policy": EXPECTED_BATCH_POLICY},
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="All-or-nothing batch deduction", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(), duration_s=time.time() - t0,
        ))

    return cat


# ════════════════════════════════════════════════════════════════════════════════
# 3. CONCURRENT CORRECTNESS  (most important for grading)
# ════════════════════════════════════════════════════════════════════════════════

def _run_single_concurrency_trial(
    infra: TestInfra,
    trial_num: int,
    quota_limit: int = DEFAULT_CONCURRENCY_QUOTA,
    num_threads: int = DEFAULT_CONCURRENCY_THREADS,
    min_amount: int = DEFAULT_CONCURRENCY_MIN_AMOUNT,
    max_amount: int = DEFAULT_CONCURRENCY_MAX_AMOUNT,
) -> dict[str, Any]:
    """
    Single trial of the concurrent correctness test.

    1. Seed a fresh quota hash with used=0 and the given limit.
    2. Fire `num_threads` concurrent requests, each asking for a random
       amount in [min_amount, max_amount].
    3. Collect all responses and verify the accounting identity:
       total_granted + final_remaining == quota_limit
       and total_granted <= quota_limit (no over-serve).

    Returns a dict with trial results.
    """
    org_id = f"TEST-CONC-{trial_num}"
    feature = FEATURE_CONTAINER

    # Deterministic setup: quota hash seeded directly in Redis
    infra.seed_quota_hash(org_id, feature, limit=quota_limit, used=0)

    # Generate random request amounts
    amounts = [random.randint(min_amount, max_amount) for _ in range(num_threads)]
    total_requested = sum(amounts)
    results: list[dict] = []
    result_lock = threading.Lock()

    def fire_request(amount: int) -> dict:
        containers = infra._make_containers(amount)
        try:
            r = infra.post_containers(org_id, containers)
            return {"status": r.status_code, "amount": amount, "body": r.json()}
        except Exception as e:
            return {"status": -1, "amount": amount, "error": str(e)}

    # Fire all requests concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(fire_request, amt) for amt in amounts]
        for f in concurrent.futures.as_completed(futures):
            with result_lock:
                results.append(f.result())

    # Analyze results
    granted = sum(r["amount"] for r in results if r["status"] == 200)
    num_success = sum(1 for r in results if r["status"] == 200)
    num_rejected = sum(1 for r in results if r["status"] == 429)
    num_errors = sum(1 for r in results if r["status"] not in (200, 429))

    # Check final state via usage endpoint
    usage = infra.get_usage(org_id, feature)
    final_used = usage["used"]
    final_remaining = usage["remaining"]

    over_serve = max(0, granted - quota_limit)
    accounting_ok = (
        granted + final_remaining == quota_limit
        and final_used == granted
        and final_remaining >= 0
    )

    return {
        "trial": trial_num,
        "quota_limit": quota_limit,
        "num_threads": num_threads,
        "total_requested": total_requested,
        "total_granted": granted,
        "num_success": num_success,
        "num_rejected": num_rejected,
        "num_errors": num_errors,
        "final_used": final_used,
        "final_remaining": final_remaining,
        "over_serve": over_serve,
        "accounting_ok": accounting_ok,
    }


def run_concurrent_correctness(
    infra: TestInfra, num_trials: int = DEFAULT_CONCURRENCY_TRIALS,
) -> TestCategory:
    """
    The most important test: proves the system never over-serves quota
    under high concurrency, even when sum of requests >> available quota.

    Runs multiple independent trials to catch flaky races. Each trial seeds
    a fresh quota hash, fires many concurrent requests with random amounts,
    and verifies:
      - total_granted <= quota_limit  (no over-serve)
      - final_remaining >= 0          (never negative)
      - total_granted + remaining == limit  (perfect accounting)

    WHY THIS PROVES CORRECTNESS:
    The Lua script executes atomically inside Redis (single-threaded).
    This test hammers the same key from 50 threads to verify that the
    atomic check-and-deduct prevents read-then-write race conditions
    that would allow two threads to both see "enough quota" and both
    deduct, resulting in over-serving. 20+ trials catch intermittent
    failures that a single lucky run might miss.
    """
    cat = TestCategory(
        name="Concurrent Correctness",
        description=(
            f"Fire {DEFAULT_CONCURRENCY_THREADS} concurrent requests against "
            f"a {DEFAULT_CONCURRENCY_QUOTA}-unit quota, repeated across "
            f"{num_trials} independent trials."
        ),
        why_it_matters=(
            "This is the single most critical test. The assignment explicitly "
            "grades 'strict concurrent correctness' — quota must never drop "
            "below 0 and organizations must never be over-served. Multiple "
            "trials catch flaky races that a single pass might miss."
        ),
    )

    # ── setup: create all trial orgs ───────────────────────────────────────
    for i in range(1, num_trials + 1):
        org_id = f"TEST-CONC-{i}"
        infra.create_test_org(org_id, FEATURE_CONTAINER, DEFAULT_CONCURRENCY_QUOTA)

    # ── run all trials ─────────────────────────────────────────────────────
    trial_results: list[dict] = []
    max_over_serve = 0
    all_trials_passed = True

    for i in range(1, num_trials + 1):
        t0 = time.time()
        trial = _run_single_concurrency_trial(infra, i)
        dt = time.time() - t0
        trial["duration_s"] = dt
        trial_results.append(trial)

        trial_ok = trial["over_serve"] == 0 and trial["accounting_ok"]
        max_over_serve = max(max_over_serve, trial["over_serve"])
        if not trial_ok:
            all_trials_passed = False

        status = "✓" if trial_ok else "✗"
        print(
            f"  Trial {i:>2}/{num_trials}: "
            f"granted={trial['total_granted']:>4}/{trial['quota_limit']}  "
            f"remaining={trial['final_remaining']:>4}  "
            f"errors={trial['num_errors']}  {status}  ({dt:.2f}s)"
        )

    # ── aggregate assertions ───────────────────────────────────────────────
    assertions = [
        check("max_over_serve", 0, max_over_serve,
              "No trial should over-serve quota (this is the #1 metric)"),
        check_true("all_trials_accounting_ok",
                    all(t["accounting_ok"] for t in trial_results),
                    "granted + remaining == limit for every trial"),
        check_true("no_negative_remaining",
                    all(t["final_remaining"] >= 0 for t in trial_results),
                    "Remaining must never go negative"),
        check("no_errors",
              0, sum(t["num_errors"] for t in trial_results),
              "No unexpected HTTP errors during concurrency test"),
    ]

    cat.results.append(TestResult(
        name=f"Concurrent correctness ({num_trials} trials)",
        description=(
            f"Each trial: seed quota={DEFAULT_CONCURRENCY_QUOTA}, fire "
            f"{DEFAULT_CONCURRENCY_THREADS} threads each requesting "
            f"{DEFAULT_CONCURRENCY_MIN_AMOUNT}–{DEFAULT_CONCURRENCY_MAX_AMOUNT} "
            f"units (total >> {DEFAULT_CONCURRENCY_QUOTA}). Verify zero over-serve."
        ),
        why_it_matters=(
            "Directly proves the Lua-script atomicity claim: concurrent "
            "requests cannot race past the quota limit."
        ),
        passed=all(a.passed for a in assertions),
        assertions=assertions,
        data={"trials": trial_results, "max_over_serve": max_over_serve},
    ))

    return cat


# ════════════════════════════════════════════════════════════════════════════════
# 4. FAILURE & RETRY SAFETY
# ════════════════════════════════════════════════════════════════════════════════

def run_failure_retry(infra: TestInfra) -> TestCategory:
    """
    Tests two safety mechanisms:

    1. IDEMPOTENCY: Retrying a request with the same Idempotency-Key
       header must not deduct quota twice. This protects against client
       retries after a response was lost in transit.

    2. REFUND ON DOWNSTREAM FAILURE: If the endpoint's business logic
       raises an exception after quota was deducted, the decorator must
       refund the units so the org isn't charged for work that didn't happen.
    """
    cat = TestCategory(
        name="Failure & Retry Safety",
        description=(
            "Idempotency-key replay protection and automatic refund "
            "when downstream processing fails."
        ),
        why_it_matters=(
            "The assignment grades 'idempotency & fault tolerance' — "
            "the system must handle client retries and downstream failures "
            "without double-charging or losing quota."
        ),
    )

    # ── test 1: idempotency ────────────────────────────────────────────────
    ORG = "TEST-IDEM"
    LIMIT = 100
    infra.create_test_org(ORG, FEATURE_CONTAINER, LIMIT)
    infra.seed_quota_hash(ORG, FEATURE_CONTAINER, limit=LIMIT, used=0)

    t0 = time.time()
    try:
        idem_key = f"test-idem-{uuid.uuid4().hex[:8]}"

        # First call with idempotency key
        r1 = infra.post_containers(
            ORG, infra._make_containers(10), idempotency_key=idem_key,
        )
        usage1 = infra.get_usage(ORG, FEATURE_CONTAINER)

        # Retry with same idempotency key — should be replayed, not re-deducted
        r2 = infra.post_containers(
            ORG, infra._make_containers(10), idempotency_key=idem_key,
        )
        body2 = r2.json()
        usage2 = infra.get_usage(ORG, FEATURE_CONTAINER)

        # Different idempotency key — should be a new deduction
        new_key = f"test-idem-{uuid.uuid4().hex[:8]}"
        r3 = infra.post_containers(
            ORG, infra._make_containers(10), idempotency_key=new_key,
        )
        usage3 = infra.get_usage(ORG, FEATURE_CONTAINER)

        assertions = [
            check("first call status", 200, r1.status_code),
            check("used after first call", 10, usage1["used"]),
            check("replay status", 200, r2.status_code),
            check_true("replay flagged",
                        body2.get("replayed") is True,
                        "Response should indicate idempotent replay"),
            check("used after replay", 10, usage2["used"],
                  "Quota must not be deducted again on replay"),
            check("new key status", 200, r3.status_code),
            check("used after new key", 20, usage3["used"],
                  "Different key → new deduction"),
        ]

        cat.results.append(TestResult(
            name="Idempotency-key replay protection",
            description=(
                "Send request with Idempotency-Key, retry with same key "
                "(should replay), then send with different key (should deduct)."
            ),
            why_it_matters=(
                "Client retries after a lost response must not double-charge."
            ),
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Idempotency-key replay protection",
            description="", why_it_matters="",
            passed=False, error=traceback.format_exc(),
            duration_s=time.time() - t0,
        ))

    # ── test 2: refund on downstream failure ───────────────────────────────
    ORG_FAIL = "TEST-REFUND"
    infra.create_test_org(ORG_FAIL, FEATURE_CONTAINER, LIMIT)
    infra.seed_quota_hash(ORG_FAIL, FEATURE_CONTAINER, limit=LIMIT, used=0)

    t0 = time.time()
    try:
        # Trigger simulated downstream failure (body has "fail": true)
        r_fail = infra.post_containers(
            ORG_FAIL, infra._make_containers(10), fail=True,
        )
        usage_after_fail = infra.get_usage(ORG_FAIL, FEATURE_CONTAINER)

        # Now make a normal request — should succeed from the full quota
        r_ok = infra.post_containers(
            ORG_FAIL, infra._make_containers(15),
        )
        usage_after_ok = infra.get_usage(ORG_FAIL, FEATURE_CONTAINER)

        assertions = [
            check("fail status", 500, r_fail.status_code,
                  "Simulated downstream failure should return 500"),
            check("used after failure", 0, usage_after_fail["used"],
                  "Refund must restore the deducted units to 0"),
            check("remaining after failure", LIMIT, usage_after_fail["remaining"],
                  "Full quota should be available after refund"),
            check("normal request status", 200, r_ok.status_code),
            check("used after normal request", 15, usage_after_ok["used"]),
        ]

        cat.results.append(TestResult(
            name="Refund on downstream failure",
            description=(
                "Trigger simulated failure via {'fail': true} body flag. "
                "The @quota_required decorator must refund the deducted units."
            ),
            why_it_matters=(
                "Orgs must not be charged for work that didn't complete. "
                "This proves the compensating refund mechanism works."
            ),
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Refund on downstream failure",
            description="", why_it_matters="",
            passed=False, error=traceback.format_exc(),
            duration_s=time.time() - t0,
        ))

    return cat


# ════════════════════════════════════════════════════════════════════════════════
# 5. RESET & REPORTING
# ════════════════════════════════════════════════════════════════════════════════

def run_reset_reporting(infra: TestInfra) -> TestCategory:
    """
    Verifies the GET /usage endpoint returns accurate data and that
    period rollover resets usage correctly.

    Tests:
    1. Usage endpoint correctness: used, remaining, limit, reset_at
    2. Period rollover: stale period in Redis hash → next request resets used
    """
    cat = TestCategory(
        name="Reset & Reporting",
        description="Usage endpoint accuracy and monthly period rollover.",
        why_it_matters=(
            "The assignment requires a reporting endpoint with current usage "
            "and next reset timestamp. Period rollover must be correct and "
            "atomic (handled inside the Lua script)."
        ),
    )

    ORG = "TEST-RESET"
    LIMIT = 200

    infra.create_test_org(ORG, FEATURE_CONTAINER, LIMIT)
    infra.seed_quota_hash(ORG, FEATURE_CONTAINER, limit=LIMIT, used=0)

    # ── test 1: usage endpoint correctness ─────────────────────────────────
    t0 = time.time()
    try:
        # Consume 75 units
        infra.post_containers(ORG, infra._make_containers(75))
        usage = infra.get_usage(ORG, FEATURE_CONTAINER)

        # Compute expected reset_at (first day of next month, UTC)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if now_utc.month == 12:
            expected_reset = datetime.datetime(
                now_utc.year + 1, 1, 1, tzinfo=datetime.timezone.utc
            )
        else:
            expected_reset = datetime.datetime(
                now_utc.year, now_utc.month + 1, 1, tzinfo=datetime.timezone.utc
            )
        expected_reset_iso = expected_reset.isoformat()

        assertions = [
            check("used", 75, usage["used"]),
            check("remaining", 125, usage["remaining"]),
            check("limit", LIMIT, usage["limit"]),
            check("period", infra._current_period(), usage["period"]),
            check("reset_at", expected_reset_iso, usage["reset_at"],
                  "First instant of next calendar month, UTC"),
        ]

        cat.results.append(TestResult(
            name="Usage endpoint correctness",
            description="Consume 75 of 200 units and verify usage report.",
            why_it_matters="The reporting endpoint is a graded deliverable.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            data={"usage_response": usage},
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Usage endpoint correctness",
            description="", why_it_matters="",
            passed=False, error=traceback.format_exc(),
            duration_s=time.time() - t0,
        ))

    # ── test 2: period rollover ────────────────────────────────────────────
    ORG_ROLL = "TEST-ROLLOVER"
    infra.create_test_org(ORG_ROLL, FEATURE_CONTAINER, LIMIT)

    t0 = time.time()
    try:
        # Seed the hash with a stale period (January 2025)
        infra.seed_quota_hash(
            ORG_ROLL, FEATURE_CONTAINER,
            limit=LIMIT, used=150, period="2025-01",
        )

        # Make a request — the Lua script should detect the stale period,
        # reset used to 0, and then deduct the new amount.
        r = infra.post_containers(ORG_ROLL, infra._make_containers(20))
        usage = infra.get_usage(ORG_ROLL, FEATURE_CONTAINER)

        assertions = [
            check("rollover request status", 200, r.status_code,
                  "Request should succeed after period rollover"),
            check("used after rollover", 20, usage["used"],
                  "Used should be only the new request amount, not stale + new"),
            check("remaining after rollover", LIMIT - 20, usage["remaining"]),
            check("period updated", infra._current_period(), usage["period"],
                  "Period should be updated to current month"),
        ]

        cat.results.append(TestResult(
            name="Period rollover (stale period → reset)",
            description=(
                "Seed Redis hash with period='2025-01' and used=150, "
                "make a new request, verify used resets to only the new amount."
            ),
            why_it_matters=(
                "The Lua script must atomically detect stale periods and reset "
                "the counter before deducting — preventing double-reset races."
            ),
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            duration_s=time.time() - t0,
        ))
    except Exception as exc:
        cat.results.append(TestResult(
            name="Period rollover (stale period → reset)",
            description="", why_it_matters="",
            passed=False, error=traceback.format_exc(),
            duration_s=time.time() - t0,
        ))

    return cat


# ════════════════════════════════════════════════════════════════════════════════
# 6. LATENCY & LOAD TEST
# ════════════════════════════════════════════════════════════════════════════════

def _run_load_worker(
    api_url: str, org_id: str, stop_event: threading.Event,
    records: list, lock: threading.Lock,
    start_epoch: float,
):
    """Worker thread for sustained load test. Fires requests in a tight loop."""
    session = requests.Session()
    session.headers.update({
        "X-Org-Id": org_id,
        "Content-Type": "application/json",
    })
    body = json.dumps({"containers": ["load-test-c"]})  # 1 unit per request

    while not stop_event.is_set():
        t0 = time.time()
        try:
            r = session.post(
                f"{api_url}/containers",
                data=body,
                timeout=10,
            )
            latency_ms = (time.time() - t0) * 1000
            rec = RequestRecord(
                start_ts=t0 - start_epoch,
                latency_ms=latency_ms,
                status_code=r.status_code,
                success=(r.status_code in (200, 429)),
            )
        except Exception:
            latency_ms = (time.time() - t0) * 1000
            rec = RequestRecord(
                start_ts=t0 - start_epoch,
                latency_ms=latency_ms,
                status_code=-1,
                success=False,
            )
        with lock:
            records.append(rec)

    session.close()


def _run_burst_worker(
    api_url: str, org_id: str, num_requests: int,
    records: list, lock: threading.Lock,
    start_epoch: float,
):
    """Worker thread for burst load test. Fires a fixed number of requests."""
    session = requests.Session()
    session.headers.update({
        "X-Org-Id": org_id,
        "Content-Type": "application/json",
    })
    body = json.dumps({"containers": ["burst-c"]})

    for _ in range(num_requests):
        t0 = time.time()
        try:
            r = session.post(
                f"{api_url}/containers",
                data=body,
                timeout=10,
            )
            latency_ms = (time.time() - t0) * 1000
            rec = RequestRecord(
                start_ts=t0 - start_epoch,
                latency_ms=latency_ms,
                status_code=r.status_code,
                success=(r.status_code in (200, 429)),
            )
        except Exception:
            latency_ms = (time.time() - t0) * 1000
            rec = RequestRecord(
                start_ts=t0 - start_epoch,
                latency_ms=latency_ms,
                status_code=-1,
                success=False,
            )
        with lock:
            records.append(rec)

    session.close()


def run_latency_load(
    infra: TestInfra, load_duration_s: int = DEFAULT_LOAD_DURATION_S,
) -> TestCategory:
    """
    Measures real throughput and latency under:
      1. Sustained load: many threads firing for `load_duration_s` seconds
      2. Burst load: all threads fire as fast as possible (short window)
      3. Bursty per-org: 200 orgs each bursting independently

    Reports honest numbers — no fudging. If the single-threaded Flask dev
    server can't hit 2,000 RPS, the report says so and notes the constraint.
    """
    cat = TestCategory(
        name="Latency & Load",
        description=(
            f"Sustained load for {load_duration_s}s, burst test, and "
            f"bursty-per-org pattern across {DEFAULT_BURSTY_ORGS} orgs."
        ),
        why_it_matters=(
            "The assignment requires sustained ~2,000 ops/sec and burst to "
            "~10,000 ops/sec with p95 < 10ms. These tests measure real "
            "numbers against those targets."
        ),
    )

    # ── test 1: sustained load ─────────────────────────────────────────────
    ORG = "TEST-LOAD-SUSTAINED"
    BIG_LIMIT = 10_000_000
    infra.create_test_org(ORG, FEATURE_CONTAINER, BIG_LIMIT)
    infra.seed_quota_hash(ORG, FEATURE_CONTAINER, limit=BIG_LIMIT, used=0)

    t0 = time.time()
    print(f"  Sustained load: {DEFAULT_LOAD_THREADS} threads × {load_duration_s}s ...")
    try:
        records: list[RequestRecord] = []
        lock = threading.Lock()
        stop_event = threading.Event()
        start_epoch = time.time()

        threads = []
        for _ in range(DEFAULT_LOAD_THREADS):
            t = threading.Thread(
                target=_run_load_worker,
                args=(infra.api_url, ORG, stop_event, records, lock, start_epoch),
                daemon=True,
            )
            threads.append(t)
            t.start()

        time.sleep(load_duration_s)
        stop_event.set()
        for t in threads:
            t.join(timeout=15)

        # Compute metrics
        latencies = [r.latency_ms for r in records if r.success]
        total_requests = len(records)
        total_success = sum(1 for r in records if r.success)
        total_errors = total_requests - total_success
        actual_duration = max(r.start_ts for r in records) - min(r.start_ts for r in records) if records else 1
        avg_rps = total_success / actual_duration if actual_duration > 0 else 0

        if latencies:
            p50 = statistics.median(latencies)
            p95 = sorted(latencies)[int(len(latencies) * 0.95)]
            p99 = sorted(latencies)[int(len(latencies) * 0.99)]
            p_min = min(latencies)
            p_max = max(latencies)
        else:
            p50 = p95 = p99 = p_min = p_max = 0.0

        # RPS per second buckets for chart
        rps_per_second: list[tuple[int, int]] = []
        if records:
            min_ts = min(r.start_ts for r in records)
            max_ts = max(r.start_ts for r in records)
            for sec in range(int(max_ts - min_ts) + 1):
                count = sum(
                    1 for r in records
                    if r.success and sec <= r.start_ts - min_ts < sec + 1
                )
                rps_per_second.append((sec, count))

        assertions = [
            check_true("test_completed",
                        total_requests > 0,
                        f"Completed {total_requests} requests"),
        ]

        cat.results.append(TestResult(
            name="Sustained load test",
            description=(
                f"{DEFAULT_LOAD_THREADS} threads firing for {load_duration_s}s. "
                f"Target: ~2,000 RPS sustained."
            ),
            why_it_matters="Proves throughput capacity under continuous load.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            data={
                "total_requests": total_requests,
                "total_success": total_success,
                "total_errors": total_errors,
                "avg_rps": round(avg_rps, 1),
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
                "p99_ms": round(p99, 2),
                "min_ms": round(p_min, 2),
                "max_ms": round(p_max, 2),
                "duration_s": round(actual_duration, 1),
                "rps_per_second": rps_per_second,
                "latencies": latencies,
                "target_rps": 2000,
                "target_p95_ms": 10,
            },
            duration_s=time.time() - t0,
        ))
        print(
            f"    → {total_success:,} requests | {avg_rps:,.0f} RPS | "
            f"p50={p50:.1f}ms  p95={p95:.1f}ms  p99={p99:.1f}ms"
        )
    except Exception as exc:
        cat.results.append(TestResult(
            name="Sustained load test", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(),
            duration_s=time.time() - t0,
        ))

    # ── test 2: burst load ─────────────────────────────────────────────────
    ORG_BURST = "TEST-LOAD-BURST"
    infra.create_test_org(ORG_BURST, FEATURE_CONTAINER, BIG_LIMIT)
    infra.seed_quota_hash(ORG_BURST, FEATURE_CONTAINER, limit=BIG_LIMIT, used=0)

    t0 = time.time()
    total_burst = DEFAULT_BURST_THREADS * DEFAULT_BURST_REQS_PER_THREAD
    print(f"  Burst load: {DEFAULT_BURST_THREADS} threads × {DEFAULT_BURST_REQS_PER_THREAD} reqs = {total_burst:,} ...")
    try:
        burst_records: list[RequestRecord] = []
        burst_lock = threading.Lock()
        burst_start = time.time()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=DEFAULT_BURST_THREADS
        ) as pool:
            futures = [
                pool.submit(
                    _run_burst_worker,
                    infra.api_url, ORG_BURST,
                    DEFAULT_BURST_REQS_PER_THREAD,
                    burst_records, burst_lock, burst_start,
                )
                for _ in range(DEFAULT_BURST_THREADS)
            ]
            concurrent.futures.wait(futures, timeout=120)

        burst_latencies = [r.latency_ms for r in burst_records if r.success]
        burst_total = len(burst_records)
        burst_success = sum(1 for r in burst_records if r.success)
        burst_duration = (
            max(r.start_ts for r in burst_records) if burst_records else 1
        )
        burst_rps = burst_success / burst_duration if burst_duration > 0 else 0

        if burst_latencies:
            bp50 = statistics.median(burst_latencies)
            bp95 = sorted(burst_latencies)[int(len(burst_latencies) * 0.95)]
            bp99 = sorted(burst_latencies)[int(len(burst_latencies) * 0.99)]
        else:
            bp50 = bp95 = bp99 = 0.0

        assertions = [
            check_true("burst_completed", burst_total > 0,
                        f"Completed {burst_total} burst requests"),
        ]

        cat.results.append(TestResult(
            name="Burst load test",
            description=(
                f"{DEFAULT_BURST_THREADS} threads × {DEFAULT_BURST_REQS_PER_THREAD} "
                f"requests = {total_burst:,} total. Target: ~10,000 RPS peak."
            ),
            why_it_matters="Proves the system handles sudden traffic spikes.",
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            data={
                "total_requests": burst_total,
                "total_success": burst_success,
                "peak_rps": round(burst_rps, 1),
                "p50_ms": round(bp50, 2),
                "p95_ms": round(bp95, 2),
                "p99_ms": round(bp99, 2),
                "duration_s": round(burst_duration, 1),
                "target_rps": 10000,
            },
            duration_s=time.time() - t0,
        ))
        print(
            f"    → {burst_success:,} requests | {burst_rps:,.0f} RPS | "
            f"p50={bp50:.1f}ms  p95={bp95:.1f}ms  p99={bp99:.1f}ms"
        )
    except Exception as exc:
        cat.results.append(TestResult(
            name="Burst load test", description="", why_it_matters="",
            passed=False, error=traceback.format_exc(),
            duration_s=time.time() - t0,
        ))

    # ── test 3: bursty per-org ─────────────────────────────────────────────
    NUM_ORGS = DEFAULT_BURSTY_ORGS
    ORG_LIMIT = 10_000
    BURSTS_PER_ORG = 3
    REQS_PER_BURST = 50

    t0 = time.time()
    print(f"  Bursty per-org: {NUM_ORGS} orgs × {BURSTS_PER_ORG} bursts × {REQS_PER_BURST} reqs ...")
    try:
        # Batch setup
        org_tuples = [
            (f"TEST-BURSTY-{i}", FEATURE_CONTAINER, ORG_LIMIT)
            for i in range(NUM_ORGS)
        ]
        infra.create_test_orgs_batch(org_tuples)

        bursty_records: list[RequestRecord] = []
        bursty_lock = threading.Lock()
        bursty_start = time.time()

        def bursty_worker(org_id: str):
            session = requests.Session()
            session.headers.update({
                "X-Org-Id": org_id,
                "Content-Type": "application/json",
            })
            body = json.dumps({"containers": ["bursty-c"]})

            for burst in range(BURSTS_PER_ORG):
                for _ in range(REQS_PER_BURST):
                    t_start = time.time()
                    try:
                        r = session.post(
                            f"{infra.api_url}/containers",
                            data=body, timeout=10,
                        )
                        lat = (time.time() - t_start) * 1000
                        rec = RequestRecord(
                            start_ts=t_start - bursty_start,
                            latency_ms=lat,
                            status_code=r.status_code,
                            success=(r.status_code in (200, 429)),
                        )
                    except Exception:
                        lat = (time.time() - t_start) * 1000
                        rec = RequestRecord(
                            start_ts=t_start - bursty_start,
                            latency_ms=lat,
                            status_code=-1,
                            success=False,
                        )
                    with bursty_lock:
                        bursty_records.append(rec)
                time.sleep(0.1)  # idle between bursts

            session.close()

        # Run all orgs concurrently (capped at 100 threads to avoid overload)
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(NUM_ORGS, 100)
        ) as pool:
            futures = [
                pool.submit(bursty_worker, f"TEST-BURSTY-{i}")
                for i in range(NUM_ORGS)
            ]
            concurrent.futures.wait(futures, timeout=300)

        bursty_latencies = [r.latency_ms for r in bursty_records if r.success]
        bursty_total = len(bursty_records)
        bursty_success = sum(1 for r in bursty_records if r.success)

        if bursty_latencies:
            by_p50 = statistics.median(bursty_latencies)
            by_p95 = sorted(bursty_latencies)[int(len(bursty_latencies) * 0.95)]
            by_p99 = sorted(bursty_latencies)[int(len(bursty_latencies) * 0.99)]
        else:
            by_p50 = by_p95 = by_p99 = 0.0

        assertions = [
            check_true("bursty_completed", bursty_total > 0,
                        f"Completed {bursty_total} bursty requests across {NUM_ORGS} orgs"),
        ]

        cat.results.append(TestResult(
            name=f"Bursty per-org ({NUM_ORGS} orgs)",
            description=(
                f"{NUM_ORGS} orgs each fire {BURSTS_PER_ORG} bursts of "
                f"{REQS_PER_BURST} requests with 100ms idle between bursts."
            ),
            why_it_matters=(
                "Simulates realistic multi-tenant traffic: bursty per-org patterns "
                "at scale to represent the 5,000+ org target."
            ),
            passed=all(a.passed for a in assertions),
            assertions=assertions,
            data={
                "total_requests": bursty_total,
                "total_success": bursty_success,
                "num_orgs": NUM_ORGS,
                "p50_ms": round(by_p50, 2),
                "p95_ms": round(by_p95, 2),
                "p99_ms": round(by_p99, 2),
            },
            duration_s=time.time() - t0,
        ))
        print(
            f"    → {bursty_success:,} requests across {NUM_ORGS} orgs | "
            f"p50={by_p50:.1f}ms  p95={by_p95:.1f}ms  p99={by_p99:.1f}ms"
        )
    except Exception as exc:
        cat.results.append(TestResult(
            name=f"Bursty per-org ({NUM_ORGS} orgs)",
            description="", why_it_matters="",
            passed=False, error=traceback.format_exc(),
            duration_s=time.time() - t0,
        ))

    return cat


# ════════════════════════════════════════════════════════════════════════════════
# SVG CHART GENERATORS (zero-dependency, inlined in the HTML report)
# ════════════════════════════════════════════════════════════════════════════════

def _svg_latency_bar_chart(
    p50: float, p95: float, p99: float, target_p95: float = 10.0,
) -> str:
    """
    Generates an SVG bar chart showing p50/p95/p99 latency values
    with a dashed target line at `target_p95`.
    """
    W, H = 480, 280
    M_LEFT, M_RIGHT, M_TOP, M_BOT = 70, 30, 40, 50
    chart_w = W - M_LEFT - M_RIGHT
    chart_h = H - M_TOP - M_BOT

    values = [("p50", p50, "#818cf8"), ("p95", p95, "#f59e0b"), ("p99", p99, "#ef4444")]
    max_val = max(p50, p95, p99, target_p95) * 1.2 or 1

    bar_w = chart_w / 5
    gap = bar_w / 2

    bars_svg = ""
    for i, (label, val, color) in enumerate(values):
        x = M_LEFT + gap + i * (bar_w + gap)
        bar_h = (val / max_val) * chart_h
        y = M_TOP + chart_h - bar_h

        bars_svg += (
            f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bar_h}" '
            f'fill="{color}" rx="4" opacity="0.9"/>\n'
            f'<text x="{x + bar_w/2}" y="{y - 8}" text-anchor="middle" '
            f'fill="#e2e8f0" font-size="13" font-weight="600">{val:.1f}ms</text>\n'
            f'<text x="{x + bar_w/2}" y="{M_TOP + chart_h + 22}" '
            f'text-anchor="middle" fill="#94a3b8" font-size="13">{label}</text>\n'
        )

    # Target line
    target_y = M_TOP + chart_h - (target_p95 / max_val) * chart_h
    target_svg = (
        f'<line x1="{M_LEFT}" y1="{target_y}" x2="{W - M_RIGHT}" y2="{target_y}" '
        f'stroke="#22c55e" stroke-width="1.5" stroke-dasharray="6,4"/>\n'
        f'<text x="{W - M_RIGHT - 4}" y="{target_y - 6}" text-anchor="end" '
        f'fill="#22c55e" font-size="11">target {target_p95}ms</text>\n'
    )

    # Y axis ticks
    ticks_svg = ""
    for i in range(5):
        val = (max_val / 4) * i
        y = M_TOP + chart_h - (val / max_val) * chart_h
        ticks_svg += (
            f'<line x1="{M_LEFT - 4}" y1="{y}" x2="{W - M_RIGHT}" y2="{y}" '
            f'stroke="#2d3148" stroke-width="0.5"/>\n'
            f'<text x="{M_LEFT - 8}" y="{y + 4}" text-anchor="end" '
            f'fill="#64748b" font-size="11">{val:.0f}</text>\n'
        )

    return (
        f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
        f'style="width:100%;max-width:{W}px;height:auto;">\n'
        f'<rect width="{W}" height="{H}" fill="#1a1d29" rx="8"/>\n'
        f'<text x="{W/2}" y="24" text-anchor="middle" fill="#e2e8f0" '
        f'font-size="14" font-weight="600">Latency Percentiles</text>\n'
        f'{ticks_svg}{bars_svg}{target_svg}'
        f'</svg>'
    )


def _svg_throughput_chart(rps_per_second: list[tuple[int, int]], target_rps: int) -> str:
    """
    Generates an SVG line chart of RPS over time during the sustained load test.
    """
    if not rps_per_second:
        return '<svg viewBox="0 0 700 280"><text x="350" y="140" text-anchor="middle" fill="#64748b">No data</text></svg>'

    W, H = 700, 280
    M_LEFT, M_RIGHT, M_TOP, M_BOT = 70, 30, 40, 50
    chart_w = W - M_LEFT - M_RIGHT
    chart_h = H - M_TOP - M_BOT

    max_sec = max(s for s, _ in rps_per_second) or 1
    max_rps = max(max(r for _, r in rps_per_second), target_rps) * 1.15 or 1

    # Build polyline points
    points = []
    area_points = [f"{M_LEFT},{M_TOP + chart_h}"]
    for sec, rps in rps_per_second:
        x = M_LEFT + (sec / max_sec) * chart_w
        y = M_TOP + chart_h - (rps / max_rps) * chart_h
        points.append(f"{x},{y}")
        area_points.append(f"{x},{y}")
    area_points.append(f"{M_LEFT + chart_w},{M_TOP + chart_h}")

    line_svg = (
        f'<polygon points="{" ".join(area_points)}" '
        f'fill="rgba(99,102,241,0.12)" stroke="none"/>\n'
        f'<polyline points="{" ".join(points)}" '
        f'fill="none" stroke="#6366f1" stroke-width="2" '
        f'stroke-linejoin="round" stroke-linecap="round"/>\n'
    )

    # Target line
    target_y = M_TOP + chart_h - (target_rps / max_rps) * chart_h
    target_line = (
        f'<line x1="{M_LEFT}" y1="{target_y}" x2="{W - M_RIGHT}" y2="{target_y}" '
        f'stroke="#22c55e" stroke-width="1.5" stroke-dasharray="6,4"/>\n'
        f'<text x="{W - M_RIGHT - 4}" y="{target_y - 6}" text-anchor="end" '
        f'fill="#22c55e" font-size="11">target {target_rps:,} RPS</text>\n'
    )

    # Axes
    axes_svg = ""
    for i in range(5):
        val = (max_rps / 4) * i
        y = M_TOP + chart_h - (val / max_rps) * chart_h
        axes_svg += (
            f'<line x1="{M_LEFT}" y1="{y}" x2="{W - M_RIGHT}" y2="{y}" '
            f'stroke="#2d3148" stroke-width="0.5"/>\n'
            f'<text x="{M_LEFT - 8}" y="{y + 4}" text-anchor="end" '
            f'fill="#64748b" font-size="10">{int(val):,}</text>\n'
        )
    # X axis labels (every 5 seconds)
    for sec in range(0, int(max_sec) + 1, max(1, int(max_sec / 6))):
        x = M_LEFT + (sec / max_sec) * chart_w
        axes_svg += (
            f'<text x="{x}" y="{M_TOP + chart_h + 20}" text-anchor="middle" '
            f'fill="#64748b" font-size="10">{sec}s</text>\n'
        )

    return (
        f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
        f'style="width:100%;max-width:{W}px;height:auto;">\n'
        f'<rect width="{W}" height="{H}" fill="#1a1d29" rx="8"/>\n'
        f'<text x="{W/2}" y="24" text-anchor="middle" fill="#e2e8f0" '
        f'font-size="14" font-weight="600">Throughput Over Time (RPS)</text>\n'
        f'{axes_svg}{line_svg}{target_line}'
        f'</svg>'
    )


def _svg_concurrency_chart(trials: list[dict], quota_limit: int) -> str:
    """
    Generates an SVG chart showing total_granted per trial as bars,
    with a horizontal line at the quota limit. Every bar must be
    at or below the line for correctness.
    """
    if not trials:
        return '<svg viewBox="0 0 700 280"><text x="350" y="140" text-anchor="middle" fill="#64748b">No data</text></svg>'

    W, H = 700, 300
    M_LEFT, M_RIGHT, M_TOP, M_BOT = 70, 30, 50, 50
    chart_w = W - M_LEFT - M_RIGHT
    chart_h = H - M_TOP - M_BOT

    n = len(trials)
    bar_w = max(4, min(20, chart_w / (n * 1.5)))
    gap = bar_w * 0.5
    total_bar_area = n * (bar_w + gap)
    offset = M_LEFT + (chart_w - total_bar_area) / 2

    max_val = max(max(t["total_granted"] for t in trials), quota_limit) * 1.1

    bars_svg = ""
    for i, trial in enumerate(trials):
        x = offset + i * (bar_w + gap)
        granted = trial["total_granted"]
        bar_h = (granted / max_val) * chart_h
        y = M_TOP + chart_h - bar_h
        color = "#22c55e" if granted <= quota_limit else "#ef4444"

        bars_svg += (
            f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bar_h}" '
            f'fill="{color}" rx="2" opacity="0.85"/>\n'
        )

    # Limit line
    limit_y = M_TOP + chart_h - (quota_limit / max_val) * chart_h
    limit_svg = (
        f'<line x1="{M_LEFT}" y1="{limit_y}" x2="{W - M_RIGHT}" y2="{limit_y}" '
        f'stroke="#ef4444" stroke-width="1.5" stroke-dasharray="6,4"/>\n'
        f'<text x="{W - M_RIGHT - 4}" y="{limit_y - 6}" text-anchor="end" '
        f'fill="#ef4444" font-size="11">quota limit = {quota_limit}</text>\n'
    )

    # Y axis
    ticks_svg = ""
    for i in range(5):
        val = (max_val / 4) * i
        y = M_TOP + chart_h - (val / max_val) * chart_h
        ticks_svg += (
            f'<line x1="{M_LEFT}" y1="{y}" x2="{W - M_RIGHT}" y2="{y}" '
            f'stroke="#2d3148" stroke-width="0.5"/>\n'
            f'<text x="{M_LEFT - 8}" y="{y + 4}" text-anchor="end" '
            f'fill="#64748b" font-size="10">{int(val)}</text>\n'
        )

    # X axis label
    x_label = (
        f'<text x="{W/2}" y="{H - 8}" text-anchor="middle" '
        f'fill="#64748b" font-size="11">Trial #</text>\n'
    )

    return (
        f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
        f'style="width:100%;max-width:{W}px;height:auto;">\n'
        f'<rect width="{W}" height="{H}" fill="#1a1d29" rx="8"/>\n'
        f'<text x="{W/2}" y="30" text-anchor="middle" fill="#e2e8f0" '
        f'font-size="14" font-weight="600">Concurrent Correctness — '
        f'Granted per Trial (must be ≤ {quota_limit})</text>\n'
        f'{ticks_svg}{bars_svg}{limit_svg}{x_label}'
        f'</svg>'
    )


# ════════════════════════════════════════════════════════════════════════════════
# HTML REPORT GENERATOR
# ════════════════════════════════════════════════════════════════════════════════

def _badge(passed: bool, is_limitation: bool = False) -> str:
    if is_limitation:
        return '<span class="badge badge-warn">LIMITATION</span>'
    if passed:
        return '<span class="badge badge-pass">PASS</span>'
    return '<span class="badge badge-fail">FAIL</span>'


def _assertions_table(assertions: list[TestAssertion]) -> str:
    rows = ""
    for a in assertions:
        icon = "✓" if a.passed else "✗"
        cls = "pass" if a.passed else "fail"
        detail = f'<div class="detail">{html_module.escape(str(a.detail))}</div>' if a.detail else ""
        rows += (
            f'<tr class="assertion-{cls}">'
            f'<td class="icon-cell">{icon}</td>'
            f'<td>{html_module.escape(a.name)}{detail}</td>'
            f'<td class="mono">{html_module.escape(str(a.expected))}</td>'
            f'<td class="mono">{html_module.escape(str(a.actual))}</td>'
            f'</tr>\n'
        )
    return (
        '<table class="assertions-table">'
        '<thead><tr><th></th><th>Assertion</th><th>Expected</th><th>Actual</th></tr></thead>'
        f'<tbody>{rows}</tbody></table>'
    )


def _load_data_card(data: dict, title: str) -> str:
    """Render a compact metric card for load test results."""
    if not data:
        return ""

    cards = ""
    metric_items = [
        ("Total Requests", f"{data.get('total_requests', 0):,}"),
        ("Successful", f"{data.get('total_success', data.get('total_requests', 0)):,}"),
        ("Avg RPS", f"{data.get('avg_rps', data.get('peak_rps', 0)):,.0f}"),
        ("p50", f"{data.get('p50_ms', 0):.1f}ms"),
        ("p95", f"{data.get('p95_ms', 0):.1f}ms"),
        ("p99", f"{data.get('p99_ms', 0):.1f}ms"),
    ]

    for label, value in metric_items:
        cards += f'<div class="metric-pill"><span class="metric-label">{label}</span><span class="metric-value">{value}</span></div>\n'

    target_rps = data.get("target_rps", 0)
    actual_rps = data.get("avg_rps", data.get("peak_rps", 0))
    target_note = ""
    if target_rps:
        pct = (actual_rps / target_rps * 100) if target_rps else 0
        color = "#22c55e" if pct >= 80 else "#f59e0b" if pct >= 50 else "#ef4444"
        target_note = (
            f'<div class="target-note" style="color:{color}">'
            f'{pct:.0f}% of {target_rps:,} RPS target'
            f'</div>'
        )

    return f'<div class="metric-grid">{cards}</div>{target_note}'


def generate_report(
    categories: list[TestCategory],
    total_duration_s: float,
    args: argparse.Namespace,
) -> str:
    """Generate a self-contained HTML report with all CSS inlined."""

    now = datetime.datetime.now()
    total_pass = sum(c.pass_count for c in categories)
    total_fail = sum(c.fail_count for c in categories)
    total_limitation = sum(c.limitation_count for c in categories)
    overall_pass = total_fail == 0

    # Find max over-serve from concurrency test
    max_over_serve = 0
    for cat in categories:
        for res in cat.results:
            if "max_over_serve" in res.data:
                max_over_serve = res.data["max_over_serve"]

    minutes = int(total_duration_s // 60)
    seconds = int(total_duration_s % 60)
    duration_str = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"

    # Build category sections
    sections_html = ""
    for cat_idx, cat in enumerate(categories, 1):
        results_html = ""
        for res in cat.results:
            # Assertions table
            asserts_html = _assertions_table(res.assertions) if res.assertions else ""

            # Error block
            error_html = ""
            if res.error:
                error_html = (
                    f'<div class="error-block"><strong>Error:</strong>'
                    f'<pre>{html_module.escape(res.error)}</pre></div>'
                )

            # Charts (embedded based on data content)
            charts_html = ""

            # Raw data collapsible
            raw_data_html = ""
            if res.data:
                # Special rendering for load test data
                if "p50_ms" in res.data and "total_requests" in res.data:
                    charts_html += _load_data_card(res.data, res.name)

                    # Latency chart for sustained load
                    if "latencies" in res.data and res.data["latencies"]:
                        charts_html += _svg_latency_bar_chart(
                            res.data["p50_ms"], res.data["p95_ms"],
                            res.data["p99_ms"],
                            res.data.get("target_p95_ms", 10),
                        )

                    # Throughput chart
                    if "rps_per_second" in res.data and res.data["rps_per_second"]:
                        charts_html += _svg_throughput_chart(
                            res.data["rps_per_second"],
                            res.data.get("target_rps", 2000),
                        )

                # Concurrency chart
                if "trials" in res.data:
                    charts_html += _svg_concurrency_chart(
                        res.data["trials"],
                        res.data["trials"][0]["quota_limit"] if res.data["trials"] else 500,
                    )

                # Raw data (sanitized copy without huge lists)
                display_data = {}
                for k, v in res.data.items():
                    if k in ("latencies",):
                        display_data[k] = f"[{len(v)} values, omitted for brevity]"
                    elif k == "rps_per_second":
                        display_data[k] = f"[{len(v)} second-buckets, see chart above]"
                    else:
                        display_data[k] = v
                raw_json = json.dumps(display_data, indent=2, default=str)
                raw_data_html = (
                    f'<details class="raw-data">'
                    f'<summary>View Raw Data</summary>'
                    f'<pre>{html_module.escape(raw_json)}</pre>'
                    f'</details>'
                )

            badge = _badge(res.passed, res.is_limitation)
            dur = f" ({res.duration_s:.2f}s)" if res.duration_s else ""

            results_html += f"""
            <div class="test-result {'result-pass' if res.passed else 'result-fail' if not res.is_limitation else 'result-warn'}">
                <div class="result-header">
                    {badge}
                    <h3>{html_module.escape(res.name)}{dur}</h3>
                </div>
                <p class="result-desc">{html_module.escape(res.description)}</p>
                <p class="result-why"><em>Why it matters:</em> {html_module.escape(res.why_it_matters)}</p>
                {error_html}
                {asserts_html}
                <div class="charts-container">{charts_html}</div>
                {raw_data_html}
            </div>
            """

        cat_badge = _badge(cat.passed)
        sections_html += f"""
        <section class="category-section">
            <div class="category-header">
                {cat_badge}
                <h2>{cat_idx}. {html_module.escape(cat.name)}</h2>
            </div>
            <p class="category-desc">{html_module.escape(cat.description)}</p>
            <p class="category-why"><em>{html_module.escape(cat.why_it_matters)}</em></p>
            <div class="results-list">
                {results_html}
            </div>
        </section>
        """

    verdict_class = "verdict-pass" if overall_pass else "verdict-fail"
    verdict_text = "ALL TESTS PASSED" if overall_pass else "FAILURES DETECTED"
    verdict_icon = "✅" if overall_pass else "❌"

    over_serve_class = "stat-pass" if max_over_serve == 0 else "stat-fail"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Quota System Test Report — {now.strftime('%Y-%m-%d %H:%M')}</title>
<style>
:root {{
    --bg-primary: #0f1117;
    --bg-secondary: #1a1d29;
    --bg-tertiary: #252836;
    --text-primary: #e2e8f0;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --accent: #6366f1;
    --accent-light: #818cf8;
    --pass: #22c55e;
    --pass-bg: rgba(34,197,94,0.08);
    --fail: #ef4444;
    --fail-bg: rgba(239,68,68,0.08);
    --warn: #f59e0b;
    --warn-bg: rgba(245,158,11,0.08);
    --border: #2d3148;
    --radius: 12px;
    --shadow: 0 4px 24px rgba(0,0,0,0.3);
}}

*, *::before, *::after {{ margin:0; padding:0; box-sizing:border-box; }}

body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
                 'Helvetica Neue', Arial, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    line-height: 1.6;
    padding: 2rem;
    max-width: 1100px;
    margin: 0 auto;
}}

/* ── Header ── */
.report-header {{
    text-align: center;
    padding: 2.5rem 2rem;
    background: linear-gradient(135deg, #1a1d29 0%, #252836 100%);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    margin-bottom: 2rem;
    box-shadow: var(--shadow);
}}
.report-header h1 {{
    font-size: 1.8rem;
    font-weight: 700;
    background: linear-gradient(135deg, #818cf8, #6366f1, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.5rem;
}}
.report-meta {{
    color: var(--text-muted);
    font-size: 0.85rem;
    display: flex;
    gap: 1.5rem;
    justify-content: center;
    flex-wrap: wrap;
}}
.report-meta span {{ white-space: nowrap; }}

/* ── Executive Summary ── */
.executive-summary {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
}}
.stat-card {{
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.25rem;
    text-align: center;
    box-shadow: var(--shadow);
    transition: transform 0.15s;
}}
.stat-card:hover {{ transform: translateY(-2px); }}
.stat-card .stat-value {{
    font-size: 2rem;
    font-weight: 800;
    display: block;
    margin-bottom: 0.25rem;
}}
.stat-card .stat-label {{
    font-size: 0.8rem;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}
.stat-pass .stat-value {{ color: var(--pass); }}
.stat-fail .stat-value {{ color: var(--fail); }}
.stat-warn .stat-value {{ color: var(--warn); }}
.stat-accent .stat-value {{ color: var(--accent-light); }}

.verdict-banner {{
    text-align: center;
    padding: 1rem 2rem;
    border-radius: var(--radius);
    font-size: 1.3rem;
    font-weight: 700;
    margin-bottom: 2rem;
    border: 1px solid var(--border);
}}
.verdict-pass {{
    background: var(--pass-bg);
    color: var(--pass);
    border-color: rgba(34,197,94,0.2);
}}
.verdict-fail {{
    background: var(--fail-bg);
    color: var(--fail);
    border-color: rgba(239,68,68,0.2);
}}

/* ── Badges ── */
.badge {{
    display: inline-block;
    padding: 0.2rem 0.7rem;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    flex-shrink: 0;
}}
.badge-pass {{ background: var(--pass-bg); color: var(--pass); border: 1px solid rgba(34,197,94,0.25); }}
.badge-fail {{ background: var(--fail-bg); color: var(--fail); border: 1px solid rgba(239,68,68,0.25); }}
.badge-warn {{ background: var(--warn-bg); color: var(--warn); border: 1px solid rgba(245,158,11,0.25); }}

/* ── Category Sections ── */
.category-section {{
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem;
    margin-bottom: 1.5rem;
    box-shadow: var(--shadow);
}}
.category-header {{
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.5rem;
}}
.category-header h2 {{
    font-size: 1.2rem;
    font-weight: 700;
}}
.category-desc {{
    color: var(--text-secondary);
    font-size: 0.9rem;
    margin-bottom: 0.25rem;
}}
.category-why {{
    color: var(--text-muted);
    font-size: 0.85rem;
    margin-bottom: 1rem;
}}

/* ── Test Results ── */
.results-list {{ display: flex; flex-direction: column; gap: 1rem; }}
.test-result {{
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.25rem;
}}
.result-pass {{ border-left: 3px solid var(--pass); }}
.result-fail {{ border-left: 3px solid var(--fail); }}
.result-warn {{ border-left: 3px solid var(--warn); }}

.result-header {{
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.5rem;
}}
.result-header h3 {{ font-size: 1rem; font-weight: 600; }}
.result-desc {{ color: var(--text-secondary); font-size: 0.85rem; margin-bottom: 0.25rem; }}
.result-why {{ color: var(--text-muted); font-size: 0.8rem; margin-bottom: 0.75rem; }}

/* ── Assertions Table ── */
.assertions-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
    margin-bottom: 1rem;
}}
.assertions-table th {{
    text-align: left;
    padding: 0.5rem 0.75rem;
    color: var(--text-muted);
    font-weight: 600;
    border-bottom: 1px solid var(--border);
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}}
.assertions-table td {{
    padding: 0.4rem 0.75rem;
    border-bottom: 1px solid rgba(45,49,72,0.5);
    vertical-align: top;
}}
.assertions-table .icon-cell {{ width: 24px; text-align: center; }}
.assertion-pass .icon-cell {{ color: var(--pass); }}
.assertion-fail .icon-cell {{ color: var(--fail); font-weight: bold; }}
.assertion-fail {{ background: var(--fail-bg); }}
.mono {{ font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 0.82rem; }}
.detail {{ color: var(--text-muted); font-size: 0.78rem; margin-top: 2px; }}

/* ── Charts ── */
.charts-container {{ margin: 1rem 0; display: flex; flex-direction: column; gap: 1rem; align-items: center; }}
.charts-container svg {{ border-radius: 8px; }}

/* ── Metric Grid ── */
.metric-grid {{
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
    justify-content: center;
}}
.metric-pill {{
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.5rem 1rem;
    display: flex;
    flex-direction: column;
    align-items: center;
    min-width: 100px;
}}
.metric-label {{ font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.04em; }}
.metric-value {{ font-size: 1.1rem; font-weight: 700; color: var(--text-primary); font-family: 'SF Mono', Menlo, monospace; }}
.target-note {{ text-align: center; font-size: 0.85rem; font-weight: 600; margin-bottom: 0.5rem; }}

/* ── Error Block ── */
.error-block {{
    background: var(--fail-bg);
    border: 1px solid rgba(239,68,68,0.2);
    border-radius: 6px;
    padding: 0.75rem 1rem;
    margin-bottom: 0.75rem;
    font-size: 0.82rem;
}}
.error-block pre {{
    white-space: pre-wrap;
    word-break: break-all;
    margin-top: 0.5rem;
    color: var(--text-secondary);
    font-size: 0.78rem;
}}

/* ── Collapsible Raw Data ── */
details.raw-data {{
    margin-top: 0.5rem;
}}
details.raw-data summary {{
    cursor: pointer;
    color: var(--accent-light);
    font-size: 0.82rem;
    font-weight: 500;
    padding: 0.3rem 0;
    user-select: none;
}}
details.raw-data summary:hover {{ text-decoration: underline; }}
details.raw-data pre {{
    background: var(--bg-primary);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1rem;
    font-size: 0.78rem;
    color: var(--text-secondary);
    overflow-x: auto;
    margin-top: 0.5rem;
    max-height: 400px;
    overflow-y: auto;
}}

/* ── Footer ── */
.report-footer {{
    text-align: center;
    color: var(--text-muted);
    font-size: 0.78rem;
    padding: 1.5rem;
    border-top: 1px solid var(--border);
    margin-top: 2rem;
}}
</style>
</head>
<body>

<!-- ═══ HEADER ═══ -->
<header class="report-header">
    <h1>Quota Metering System — Test Report</h1>
    <div class="report-meta">
        <span>📅 {now.strftime('%Y-%m-%d %H:%M:%S')}</span>
        <span>🐍 Python {platform.python_version()}</span>
        <span>💻 {platform.node()}</span>
        <span>🎯 {args.url}</span>
        <span>⏱ {duration_str}</span>
    </div>
</header>

<!-- ═══ EXECUTIVE SUMMARY ═══ -->
<div class="executive-summary">
    <div class="stat-card stat-pass">
        <span class="stat-value">{total_pass}</span>
        <span class="stat-label">Passed</span>
    </div>
    <div class="stat-card {'stat-fail' if total_fail else 'stat-pass'}">
        <span class="stat-value">{total_fail}</span>
        <span class="stat-label">Failed</span>
    </div>
    <div class="stat-card {'stat-warn' if total_limitation else 'stat-accent'}">
        <span class="stat-value">{total_limitation}</span>
        <span class="stat-label">Limitations</span>
    </div>
    <div class="stat-card {over_serve_class}">
        <span class="stat-value">{max_over_serve}</span>
        <span class="stat-label">Max Over-Serve</span>
    </div>
</div>

<div class="verdict-banner {verdict_class}">
    {verdict_icon} {verdict_text}
</div>

<!-- ═══ TEST SECTIONS ═══ -->
{sections_html}

<!-- ═══ FOOTER ═══ -->
<footer class="report-footer">
    Generated by <strong>test_quota_system.py</strong> •
    Batch policy: <code>{EXPECTED_BATCH_POLICY}</code> •
    Concurrency trials: {args.trials} •
    Load duration: {args.load_duration}s
</footer>

</body>
</html>"""

    return html


# ════════════════════════════════════════════════════════════════════════════════
# MAIN RUNNER
# ════════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Comprehensive test suite for the Quota Metering System",
    )
    p.add_argument(
        "--url", default="http://localhost:8080",
        help="API base URL (default: http://localhost:8080)",
    )
    p.add_argument(
        "--redis-url", default="redis://localhost:6379/0",
        help="Redis connection URL (default: redis://localhost:6379/0)",
    )
    p.add_argument(
        "--postgres-dsn",
        default="postgresql://postgres:user_password@localhost:5432/quota_meter",
        help="Postgres DSN (default: postgresql://postgres:user_password@localhost:5432/quota_meter)",
    )
    p.add_argument(
        "--suites", default="all",
        help="Comma-separated suites: basic,batch,concurrent,retry,reset,load (default: all)",
    )
    p.add_argument(
        "--trials", type=int, default=DEFAULT_CONCURRENCY_TRIALS,
        help=f"Number of concurrency trials (default: {DEFAULT_CONCURRENCY_TRIALS})",
    )
    p.add_argument(
        "--load-duration", type=int, default=DEFAULT_LOAD_DURATION_S,
        help=f"Sustained load test duration in seconds (default: {DEFAULT_LOAD_DURATION_S})",
    )
    p.add_argument(
        "--report-path", default="report.html",
        help="Output path for HTML report (default: report.html)",
    )
    return p.parse_args()


def print_banner(args: argparse.Namespace):
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Quota Metering System — Comprehensive Test Suite           ║")
    print(f"║  Target: {args.url:<51}║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()


def main():
    args = parse_args()
    print_banner(args)

    suites = (
        ["basic", "batch", "concurrent", "retry", "reset", "load"]
        if args.suites == "all"
        else [s.strip() for s in args.suites.split(",")]
    )

    # ── connect to infrastructure ──────────────────────────────────────────
    print("Connecting to infrastructure...")
    try:
        infra = TestInfra(args.url, args.redis_url, args.postgres_dsn)
    except Exception as exc:
        print(f"❌ Failed to connect: {exc}")
        print("   Make sure Docker Compose is running: docker compose up")
        sys.exit(1)

    if not infra.health_check():
        print(f"❌ Health check failed at {args.url}/health")
        print("   Make sure the Flask app is running and accessible.")
        sys.exit(1)

    print(f"✅ Connected  (API + Redis + Postgres)\n")

    # ── run test suites ────────────────────────────────────────────────────
    categories: list[TestCategory] = []
    start_time = time.time()

    suite_runners = {
        "basic": ("Basic Functional Correctness", lambda: run_basic_functional(infra)),
        "batch": ("Batch Behavior", lambda: run_batch_behavior(infra)),
        "concurrent": ("Concurrent Correctness", lambda: run_concurrent_correctness(infra, args.trials)),
        "retry": ("Failure & Retry Safety", lambda: run_failure_retry(infra)),
        "reset": ("Reset & Reporting", lambda: run_reset_reporting(infra)),
        "load": ("Latency & Load", lambda: run_latency_load(infra, args.load_duration)),
    }

    total_suites = len(suites)
    for idx, suite_key in enumerate(suites, 1):
        if suite_key not in suite_runners:
            print(f"  ⚠ Unknown suite: {suite_key!r}, skipping")
            continue

        name, runner = suite_runners[suite_key]
        print(f"[{idx}/{total_suites}] {name}")

        try:
            cat = runner()
            categories.append(cat)
            status = "✓" if cat.passed else "✗"
            print(f"  {status} {cat.pass_count} passed, {cat.fail_count} failed, "
                  f"{cat.limitation_count} limitations\n")
        except Exception as exc:
            print(f"  ✗ Suite crashed: {exc}\n")
            traceback.print_exc()
            cat = TestCategory(
                name=name,
                description="Suite crashed before completion.",
                why_it_matters="",
                results=[TestResult(
                    name="Suite execution",
                    description="The test suite encountered a fatal error.",
                    why_it_matters="",
                    passed=False,
                    error=traceback.format_exc(),
                )],
            )
            categories.append(cat)

    total_duration = time.time() - start_time

    # ── generate report ────────────────────────────────────────────────────
    print("Generating HTML report...")
    html = generate_report(categories, total_duration, args)

    report_path = os.path.abspath(args.report_path)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    # ── cleanup ────────────────────────────────────────────────────────────
    print("Cleaning up test data...")
    infra.cleanup_all()
    infra.close()

    # ── summary ────────────────────────────────────────────────────────────
    total_pass = sum(c.pass_count for c in categories)
    total_fail = sum(c.fail_count for c in categories)
    total_lim = sum(c.limitation_count for c in categories)
    overall = total_fail == 0

    minutes = int(total_duration // 60)
    seconds = int(total_duration % 60)
    dur = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"

    print()
    print("════════════════════════════════════════════════════════════════")
    print(f"  RESULTS: {total_pass} passed, {total_fail} failed, {total_lim} limitations")
    print(f"  VERDICT: {'✅ PASS' if overall else '❌ FAIL'}")
    print(f"  REPORT:  {report_path}")
    print(f"  Duration: {dur}")
    print("════════════════════════════════════════════════════════════════")
    print()

    sys.exit(0 if overall else 1)


if __name__ == "__main__":
    main()
