#!/bin/sh
# entrypoint.sh - in project root

echo "========================================"
echo "Starting Quota Meter Application"
echo "========================================"

# Wait for database to be ready
echo "Waiting for database to be ready..."
while ! python -c "import socket; socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(('db', 5432))" 2>/dev/null; do
  sleep 1
done
echo "✅ Database is ready!"

# Wait for redis to be ready
echo "Waiting for redis to be ready..."
while ! python -c "import socket; socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(('redis', 6379))" 2>/dev/null; do
  sleep 1
done
echo "✅ Redis is ready!"

# Run database migrations
echo "----------------------------------------"
echo "Running database migrations..."
flask db upgrade
if [ $? -eq 0 ]; then
  echo "✅ Migrations completed successfully!"
else
  echo "❌ Migrations failed!"
  exit 1
fi

# Seed the database
echo "----------------------------------------"
echo "Seeding database..."
python seed.py
if [ $? -eq 0 ]; then
  echo "✅ Seeding completed successfully!"
else
  echo "❌ Seeding failed!"
  exit 1
fi

# Start the Flask application
echo "----------------------------------------"
echo "Starting Flask application..."
gunicorn --workers 4 --worker-class gevent --worker-connections 1000 --bind 0.0.0.0:5000 --access-logfile - app:app &
FLASK_PID=$!

# Wait for Flask to be ready
echo "Waiting for Flask to be ready..."
until curl -sf http://0.0.0.0:5000/health > /dev/null 2>&1; do
  echo "🔄 Flask not reachable yet, retrying..."
  sleep 1
done
echo "✅ Flask is ready!"

# Warm the Redis cache from DB
echo "----------------------------------------"
echo "Warming Redis cache..."
WARM_RESPONSE=$(curl -sf -X POST http://0.0.0.0:5000/warm-cache)
if [ $? -eq 0 ]; then
  echo "✅ Cache warmed: $WARM_RESPONSE"
else
  echo "❌ Cache warm-up failed!"
fi

# Keep Flask running in the foreground
wait $FLASK_PID
