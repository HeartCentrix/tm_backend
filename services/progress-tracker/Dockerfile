FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/progress-tracker /app/services/progress-tracker

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/services/progress-tracker/requirements.txt

ENV PYTHONPATH=/app
ENV PORT=8011

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.progress-tracker.main:app --host 0.0.0.0 --port ${PORT:-8011}"]
