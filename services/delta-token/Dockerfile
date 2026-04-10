FROM python:3.11-slim

WORKDIR /app

# Copy shared library first
COPY shared /app/shared

# Copy service code
COPY services/delta-token /app/services/delta-token

# Install dependencies
RUN pip install --no-cache-dir -r /app/services/delta-token/requirements.txt

ENV PYTHONPATH=/app

EXPOSE 8010

CMD ["python", "-m", "uvicorn", "services.delta-token.main:app", "--host", "0.0.0.0", "--port", "8010"]
