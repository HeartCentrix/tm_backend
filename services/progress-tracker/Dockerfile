FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/progress-tracker /app/services/progress-tracker

RUN pip install --no-cache-dir -r /app/services/progress-tracker/requirements.txt

ENV PYTHONPATH=/app

EXPOSE 8011

CMD ["python", "-m", "uvicorn", "services.progress-tracker.main:app", "--host", "0.0.0.0", "--port", "8011"]
