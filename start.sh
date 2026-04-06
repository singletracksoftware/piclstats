#!/bin/sh
set -e

# Test DB connection before starting
python -c "
from sqlalchemy import create_engine, text
import os
url = os.environ.get('PICLSTATS_DATABASE_URL', '')
print(f'Connecting to: {url[:50]}...')
print(f'User portion: {url.split(\"://\")[1].split(\"@\")[0].split(\":\")[0]}')
engine = create_engine(url)
with engine.connect() as conn:
    print(f'Connected: {conn.execute(text(\"SELECT current_database()\")).scalar()}')
" || echo "DB connection test failed"

# Start the server
exec uvicorn piclstats.web.app:app --host 0.0.0.0 --port ${PORT:-8080}
