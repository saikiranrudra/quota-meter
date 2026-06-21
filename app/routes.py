import time

import redis
from constants import SYNC_BATCH_SIZE
from flask import Blueprint, current_app, jsonify, request
from models import Quota, db
from utils import quota_required

api = Blueprint("api", __name__)


@api.route("/warm-cache", methods=["POST"])
def warm_cache():
    """Sync default limits from DB → Redis (paginated)."""
    engine = current_app.config["QUOTA_ENGINE"]
    total = 0
    offset = 0
    while True:
        batch = Quota.query.order_by("id").offset(offset).limit(SYNC_BATCH_SIZE).all()
        if not batch:
            break
        engine.sync_limits_to_redis([q.to_dict() for q in batch])
        total += len(batch)
        offset += SYNC_BATCH_SIZE
    print(f"✅ Synced {total} default limits from DB to Redis")
    return jsonify({"status": "ok", "synced": total})


@api.route("/default-limit/<org_id>/<feature>", methods=["PUT"])
def update_default_limit(org_id: str, feature: str):
    body = request.get_json(force=True)
    if not body or "default_limit" not in body:
        return jsonify({"error": "default_limit is required"}), 400

    try:
        default_limit = int(body["default_limit"])
    except (TypeError, ValueError):
        return jsonify({"error": "default_limit must be an integer"}), 400

    if default_limit < 0:
        return jsonify({"error": "default_limit must be non-negative"}), 400

    quota = Quota.query.filter_by(org_id=org_id, feature=feature).first()
    if quota is None:
        return jsonify({"error": "quota not found"}), 404

    quota.default_limit = default_limit
    db.session.commit()

    engine = current_app.config["QUOTA_ENGINE"]
    engine.set_default_limit(org_id, feature, default_limit)

    return jsonify(
        {
            "status": "ok",
            "org_id": org_id,
            "feature": feature,
            "default_limit": default_limit,
        }
    )


@api.route("/usage/<org_id>/<feature>", methods=["GET"])
def usage(org_id: str, feature: str):
    engine = current_app.config["QUOTA_ENGINE"]
    report = engine.get_usage(org_id, feature)
    return jsonify(
        {
            "org_id": org_id,
            "feature": report.feature,
            "used": report.used,
            "limit": report.limit,
            "remaining": report.remaining,
            "period": report.period,
            "reset_at": report.reset_at.isoformat(),
        }
    )


@api.route("/health", methods=["GET"])
def health():
    redis_client = current_app.config["REDIS_CLIENT"]
    try:
        redis_client.ping()
        return jsonify({"status": "ok"})
    except redis.RedisError as e:
        return jsonify({"status": "error", "detail": str(e)}), 503


## quota routes


@api.route("/containers", methods=["POST"])
@quota_required(
    feature="container-tracking",
    amount=lambda: len(request.get_json(force=True).get("containers", [])),
)
def track_containers():
    body = request.get_json(force=True)
    containers = body.get("containers", [])
    # Simulate downstream work. Set "fail": true in the body to
    # exercise the refund-on-failure path in tests/demos.
    if body.get("fail"):
        raise RuntimeError("simulated downstream failure")
    time.sleep(0.001)  # pretend we did some work
    return jsonify({"status": "tracked", "count": len(containers)})


@api.route("/sailing-schedule", methods=["GET"])
@quota_required(feature="sailing-schedule", amount=1)
def sailing_schedule():
    return jsonify({"status": "ok", "schedule": []})
