FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY shared/ /app/shared/
COPY services/auth-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/auth-service/ .
EXPOSE 8001
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import httpx; httpx.get('http://localhost:8001/health')"
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "2"]
