FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY shared/ /app/shared/
COPY services/job-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/job-service/ .
ENV PORT=8004
EXPOSE ${PORT}
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import httpx; httpx.get('http://localhost:${PORT:-8004}/health')"
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8004} --workers 2"]
