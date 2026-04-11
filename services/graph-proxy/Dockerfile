FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/graph-proxy /app/services/graph-proxy

RUN pip install --no-cache-dir -r /app/services/graph-proxy/requirements.txt

ENV PYTHONPATH=/app
ENV PORT=8009

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.graph-proxy.main:app --host 0.0.0.0 --port ${PORT:-8009}"]
