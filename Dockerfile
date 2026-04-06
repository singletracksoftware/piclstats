FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/
COPY alembic.ini .
COPY alembic/ alembic/

RUN pip install --no-cache-dir .

EXPOSE ${PORT:-8080}

COPY start.sh .

CMD ["./start.sh"]
