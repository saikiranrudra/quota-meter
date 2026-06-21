"""
Flask integration surface: a single decorator, `quota_required`.

Usage on an endpoint:

    @app.route("/containers", methods=["POST"])
    @quota_required(
        feature="container-tracking",
        amount=lambda: len(request.json["containers"]),
    )
    def track_containers():
        ...  # business logic. If this raises, the decorator refunds.

Design:
- `amount` can be a fixed int or a callable evaluated against the
  current request (covers the "100 containers = 100 units" case).
- org_id is resolved via `org_id_resolver` (defaults to reading
  `X-Org-Id` header) so the decorator doesn't hardcode an auth scheme.
- Idempotency key is read from `Idempotency-Key` header if present;
  if the client doesn't send one, no replay protection is applied for
  that call (their choice -- we don't invent one, since a server-
  generated key wouldn't actually match across a real client retry).
- On QuotaExceeded -> HTTP 429 with remaining/limit/reset_at in body.
- On any exception from the wrapped view -> refund the deduction,
  then re-raise (so Flask's normal error handling still applies).
"""

import functools
from typing import Callable, Optional, Union

from engine import QuotaEngine
from flask import g, jsonify, request


def default_org_id_resolver() -> str:
    org_id = request.headers.get("X-Org-Id")
    if not org_id:
        raise ValueError("X-Org-Id header is required")
    return org_id


def quota_required(
    feature: str,
    amount: Union[int, Callable[[], int]] = 1,
    org_id_resolver: Callable[[], str] = default_org_id_resolver,
    engine_getter: Optional[Callable[[], QuotaEngine]] = None,
):
    def decorator(view_fn):
        @functools.wraps(view_fn)
        def wrapped(*args, **kwargs):
            engine = engine_getter() if engine_getter else g.quota_engine
            try:
                org_id = org_id_resolver()
            except ValueError as e:
                return jsonify({"error": "bad_request", "detail": str(e)}), 400
            units = amount() if callable(amount) else amount
            idem_key = request.headers.get("Idempotency-Key")

            result = engine.check_and_deduct(
                org_id=org_id,
                feature=feature,
                amount=units,
                idempotency_key=idem_key,
            )

            if not result.allowed:
                return (
                    jsonify(
                        {
                            "error": "quota_exceeded",
                            "feature": feature,
                            "requested": units,
                            "remaining": result.remaining,
                            "limit": result.limit,
                            "period": result.period,
                        }
                    ),
                    429,
                )

            # Replayed idempotent results were already a final outcome
            # (deducted or denied) from a prior call -- don't re-run
            # the view, since that would do the work twice.
            if result.idempotent_replay:
                return jsonify(
                    {"status": "ok", "replayed": True, "remaining": result.remaining}
                ), 200

            try:
                print("Running the function")
                return view_fn(*args, **kwargs)
            except Exception:
                # Downstream failed after we deducted -- give the
                # units back so the org isn't charged for nothing.
                print("Executing refund due to fn failure")
                engine.refund(org_id, feature, units)
                raise

        return wrapped

    return decorator
