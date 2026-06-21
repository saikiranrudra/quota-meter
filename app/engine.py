from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import redis
import scripts
from constants import IDEMPOTENCY_TTL_SECONDS


@dataclass
class QuotaResult:
    allowed: bool
    remaining: int
    used: int
    limit: int
    period: str
    idempotent_replay: bool = False  # True if this result came from a cached retry


@dataclass
class UsageReport:
    feature: str
    used: int
    limit: int
    remaining: int
    period: str
    reset_at: datetime  # first instant of next calendar month, UTC


def current_period(now: Optional[datetime] = None) -> str:
    """Calendar month, UTC. e.g. '2026-06'.

    UTC is chosen so reset behavior is unambiguous regardless of which
    instance/region serves the request -- per-org timezone anchoring
    is a reasonable future extension but adds real complexity (DST,
    per-org config) for a benefit most B2B API customers won't notice.
    See DESIGN.md for the tradeoff discussion.
    """
    now = now or datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def next_reset_at(period: str) -> datetime:
    """First instant (UTC) of the month AFTER `period`."""
    year, month = (int(x) for x in period.split("-"))
    if month == 12:
        return datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    return datetime(year, month + 1, 1, tzinfo=timezone.utc)


class QuotaEngine:
    # Redis key prefix for storing default limits per org+feature
    DEFAULT_LIMIT_PREFIX = "default_limit"

    def __init__(self, redis_client: redis.Redis):
        """
          redis_client: a connected redis.Redis instance (or compatible,
            e.g. pointed at a single ElastiCache endpoint or a
            Redis Cluster client for the 50k-org scale -- see DESIGN.md).

        Default limits are loaded from the Quota DB model into Redis via
        `sync_limits_to_redis()`. The engine reads them from Redis at
        runtime
        """

        self._redis: redis.Redis = redis_client

        self._check_and_deduct = self._redis.register_script(scripts.CHECK_AND_DEDUCT)
        self._refund_script = self._redis.register_script(scripts.REFUND)
        self._get_usage_script = self._redis.register_script(scripts.GET_USAGE)

    def sync_limits_to_redis(self, quotas: list[dict]) -> int:
        """Load default limits from the Quota model into Redis.

        Args:
            quotas: list of dicts with keys 'org_id', 'feature', 'default_limit',
                    typically from [q.to_dict() for q in Quota.query.all()].

        Returns:
            Number of limits synced.
        """
        pipe = self._redis.pipeline()
        for q in quotas:
            key = self._default_limit_key(q["org_id"], q["feature"])
            pipe.set(key, q["default_limit"])
        pipe.execute()
        return len(quotas)

    def set_default_limit(self, org_id: str, feature: str, default_limit: int) -> None:
        """Update the default limit in Redis, and patch the current quota hash limit if present."""
        default_key = self._default_limit_key(org_id, feature)
        quota_key = self._quota_key(org_id, feature)
        self._redis.set(default_key, default_limit)
        if self._redis.exists(quota_key):
            self._redis.hset(quota_key, "limit", default_limit)

    @staticmethod
    def _default_limit_key(org_id: str, feature: str) -> str:
        """Redis key for a default limit: default_limit:{org_id}:{feature}."""
        return f"default_limit:{org_id}:{feature}"

    @staticmethod
    def _quota_key(org_id: str, feature: str):
        return f"quota:{org_id}:{feature}"

    @staticmethod
    def _idem_key(org_id: str, feature: str, idempotency_key: str) -> str:
        return f"idem:{org_id}:{feature}:{idempotency_key}"

    def check_and_deduct(
        self,
        org_id: str,
        feature: str,
        amount: int,
        idempotency_key: Optional[str] = None,
    ) -> QuotaResult:
        """Atomically check and deduct `amount` units.

        If idempotency_key is provided and was already used for a
        successful deduction, returns the cached prior result instead
        of deducting again (protects against client retries after a
        response was lost in transit).
        """
        if amount <= 0:
            raise ValueError("amount must be positive")

        if idempotency_key:
            idem_key = self._idem_key(org_id, feature, idempotency_key)
            cached = self._redis.get(idem_key)
            if cached is not None:
                # Replay prior outcome verbatim. We stored it as
                # "allowed:remaining:used:limit:period".
                allowed, remaining, used, limit, period = (
                    cached.split(":")
                    if isinstance(cached, str)
                    else cached.decode("utf-8").split(":")
                )

                print(allowed, remaining, used, limit, period)

                return QuotaResult(
                    allowed=(allowed == "1"),
                    remaining=int(remaining),
                    used=int(used),
                    limit=int(limit),
                    period=str(period),
                    idempotent_replay=True,
                )

        key = self._quota_key(org_id, feature)
        # Fetch default limit from Redis (synced from DB on startup)
        cached_limit = self._redis.get(self._default_limit_key(org_id, feature))
        default_limit: str = (
            cached_limit.decode()
            if isinstance(cached_limit, bytes)
            else cached_limit or "0"
        )
        period = current_period()

        allowed, remaining, used, limit, period = self._check_and_deduct(
            keys=[key], args=[amount, period, default_limit]
        )
        result = QuotaResult(
            allowed=bool(allowed),
            remaining=int(remaining),
            used=int(used),
            limit=int(limit),
            period=period.decode() if isinstance(period, bytes) else period,
        )

        if idempotency_key:
            idem_key = self._idem_key(org_id, feature, idempotency_key)
            payload = f"{int(result.allowed)}:{result.remaining}:{result.used}:{result.limit}:{result.period}"
            self._redis.set(idem_key, payload, ex=IDEMPOTENCY_TTL_SECONDS)

        return result

    def refund(self, org_id: str, feature: str, amount: int) -> int:
        """Return `amount` units to the org's quota for this period.

        Used by the decorator when a deduction succeeded but the
        downstream operation failed, so the org isn't charged for
        work that didn't happen. Floors at 0 (see scripts.REFUND).
        """
        key = self._quota_key(org_id, feature)
        result = self._refund_script(keys=[key], args=[amount])
        print(result)
        return int(result)

    def get_usage(self, org_id: str, feature: str) -> UsageReport:
        key = self._quota_key(org_id, feature)
        period = current_period()
        limit, used, stored_period = self._get_usage_script(keys=[key], args=[period])
        limit: int = int(limit)
        used: int = int(used)
        if limit == 0:
            # never seen before -> fall back to default limit from Redis
            cached_limit = self._redis.get(self._default_limit_key(org_id, feature))
            limit = int(cached_limit) if cached_limit else 0

        return UsageReport(
            feature=feature,
            used=used,
            limit=limit,
            remaining=max(limit - used, 0),
            period=period,
            reset_at=next_reset_at(period),
        )


##
# Custom Exceptions
##


class QuotaExceeded(Exception):
    """Raised by the decorator layer when a request can't be fulfilled."""

    def __init__(self, feature: str, requested: int, remaining: int):
        self.feature = feature
        self.requested = requested
        self.remaining = remaining
        super().__init__(
            f"quota exceeded for feature={feature!r}: "
            f"requested={requested}, remaining={remaining}"
        )
