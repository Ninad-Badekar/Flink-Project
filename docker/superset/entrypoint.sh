#!/bin/bash
set -e

echo ">>> Running Superset DB upgrade..."
superset db upgrade

echo ">>> Creating admin user (if not exists)..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin 2>/dev/null || echo "Admin already exists"

echo ">>> Initializing Superset roles..."
superset init

echo ">>> Running custom init script (Trino connection + saved queries)..."
python /app/init_superset.py

echo ">>> Starting Superset server..."
superset run -h 0.0.0.0 -p 8088 --with-threads
