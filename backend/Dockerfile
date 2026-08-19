FROM python:3.12-slim

WORKDIR /app/backend

RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./backend/
COPY requirements.txt ./

ENV PYTHONPATH=/app

# Render supplies PORT; uvicorn binds to all interfaces.
CMD exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
