FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/search-service /app/services/search-service

RUN pip install --no-cache-dir -r /app/services/search-service/requirements.txt

ENV PYTHONPATH=/app
ENV PORT=8013

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.search-service.main:app --host 0.0.0.0 --port ${PORT:-8013}"]
