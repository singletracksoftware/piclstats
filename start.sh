#!/bin/sh
set -e

if [ -n "$DATABASE_URL" ] && [ -z "$PICLSTATS_DATABASE_URL" ]; then
    export PICLSTATS_DATABASE_URL=$(echo "$DATABASE_URL" | sed -E 's|^postgres(ql)?://|postgresql+psycopg://|')
fi

exec uvicorn piclstats.web.app:app --host 0.0.0.0 --port ${PORT:-8080}
