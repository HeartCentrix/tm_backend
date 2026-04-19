FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/audit-service /app/services/audit-service

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/services/audit-service/requirements.txt

ENV PYTHONPATH=/app
ENV PORT=8012

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.audit-service.main:app --host 0.0.0.0 --port ${PORT:-8012}"]
