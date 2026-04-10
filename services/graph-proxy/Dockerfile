FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/graph-proxy /app/services/graph-proxy

RUN pip install --no-cache-dir -r /app/services/graph-proxy/requirements.txt

ENV PYTHONPATH=/app

EXPOSE 8009

CMD ["python", "-m", "uvicorn", "services.graph-proxy.main:app", "--host", "0.0.0.0", "--port", "8009"]
