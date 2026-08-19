FROM python:3.12-slim

WORKDIR /app/backend

RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app/backend

CMD exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
