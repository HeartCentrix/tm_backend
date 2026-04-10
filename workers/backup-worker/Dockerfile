FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY workers/backup-worker /app/workers/backup-worker

RUN pip install --no-cache-dir -r /app/workers/backup-worker/requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "workers/backup-worker/main.py"]
