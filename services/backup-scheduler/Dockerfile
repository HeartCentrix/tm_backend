FROM python:3.11-slim

WORKDIR /app

# Copy shared library first
COPY shared /app/shared

# Copy service code
COPY services/backup-scheduler /app/services/backup-scheduler

# Install dependencies
RUN pip install --no-cache-dir -r /app/services/backup-scheduler/requirements.txt

ENV PYTHONPATH=/app

EXPOSE 8008

CMD ["python", "-m", "uvicorn", "services.backup-scheduler.main:app", "--host", "0.0.0.0", "--port", "8008"]
