#!/bin/sh
set -e

# Run migrations
python -m piclstats.cli init-db

# Seed reference data (idempotent)
python -m piclstats.cli seed

# Start the server
exec uvicorn piclstats.web.app:app --host 0.0.0.0 --port 8080
