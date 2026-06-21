from gevent import monkey

monkey.patch_all()

import os

import redis
from engine import QuotaEngine
from flask import Flask, g
from flask_migrate import Migrate
from models import db
from routes import api

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = (
    os.environ.get("DATABASE_URL") or "postgresql://postgres:admin@db:5432/quota_meter"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Initialize extensions
db.init_app(app)
migrate = Migrate(app, db)

REDIS_URL = os.environ.get("REDIS_URL") or "http://localhost:6379"
_redis_client = redis.Redis.from_url(REDIS_URL)

## if redis is not available, terminate the app with an error
try:
    _redis_client.ping()
except redis.RedisError as e:
    raise RuntimeError("Redis is not available") from e

_engine = QuotaEngine(_redis_client)

# Store shared objects in app config so routes can access via current_app
app.config["QUOTA_ENGINE"] = _engine
app.config["REDIS_CLIENT"] = _redis_client

# Register the Blueprint
app.register_blueprint(api)


@app.before_request
def attach_engine():
    g.quota_engine = _engine


if __name__ == "__main__":
    app.run(debug=True)
