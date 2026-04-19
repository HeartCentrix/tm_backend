FROM python:3.12-slim

# WeasyPrint system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpango-1.0-0 libpangoft2-1.0-0 libcairo2 libgdk-pixbuf-2.0-0 \
    fonts-dejavu-core fonts-noto-color-emoji \
    build-essential curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY workers/chat-export-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY workers/chat-export-worker /app/workers/chat_export_worker

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 9102 8081
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD curl -fsS http://localhost:8081/health || exit 1

CMD ["python", "-m", "workers.chat_export_worker.main"]
