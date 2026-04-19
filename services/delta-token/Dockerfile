FROM python:3.11-slim

WORKDIR /app

# Copy shared library first
COPY shared /app/shared

# Copy service code
COPY services/delta-token /app/services/delta-token

# Install dependencies
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/services/delta-token/requirements.txt

ENV PYTHONPATH=/app
ENV PORT=8010

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.delta-token.main:app --host 0.0.0.0 --port ${PORT:-8010}"]
