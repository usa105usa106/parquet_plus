FROM python:3.12-slim

LABEL org.opencontainers.image.title="Coolify Trading + Market Data Bot" \
      org.opencontainers.image.version="v019"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_ROOT=/app/storage

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip && pip install -r requirements.txt

COPY . /app
RUN mkdir -p /app/storage /app/storage_backup

EXPOSE 80

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=10 \
    CMD python -c "import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:80/healthz', timeout=3); raise SystemExit(0 if r.status == 200 else 1)"

CMD ["python", "app.py"]
