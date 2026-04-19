FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY workers/restore-worker /app/workers/restore-worker

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/workers/restore-worker/requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "workers/restore-worker/main.py"]
