FROM python:3.11-slim

WORKDIR /app

# Install build dependencies for aarch64 packages
RUN apt-get update && apt-get install -y --no-install-recommends gcc libffi-dev && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY workers/backup-worker /app/workers/backup-worker

RUN pip install --no-cache-dir -r /app/workers/backup-worker/requirements.txt

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "workers/backup-worker/main.py"]
