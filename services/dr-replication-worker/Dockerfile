FROM python:3.11

WORKDIR /app

# Install build dependencies for compiling C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
        libssl-dev \
        libpq-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY services/dr-replication-worker /app/services/dr-replication-worker

RUN pip install --no-cache-dir --prefer-binary -r /app/services/dr-replication-worker/requirements.txt

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "services/dr-replication-worker/main.py"]
