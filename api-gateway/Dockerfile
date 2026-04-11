FROM python:3.11-slim
WORKDIR /app
COPY shared/ /app/shared/
COPY api-gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api-gateway/ .
ENV PORT=8000
EXPOSE ${PORT}
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import httpx; httpx.get('http://localhost:${PORT:-8000}/health')"
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2"]
