FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY shared/ /app/shared/
COPY services/tenant-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/tenant-service/ .
ENV PORT=8002
EXPOSE ${PORT}
RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid appuser --create-home --home-dir /home/appuser appuser \
    && chown -R 10001:10001 /app
USER 10001
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import httpx; httpx.get('http://localhost:${PORT:-8002}/health')"
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8002} --workers 1"]
