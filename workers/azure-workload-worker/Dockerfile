FROM python:3.11

WORKDIR /app

# Install build dependencies for compiling C extensions (cryptography, yarl, cffi, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
        libssl-dev \
        libpq-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY workers/azure-workload-worker /app/workers/azure-workload-worker

RUN pip install --no-cache-dir --prefer-binary -r /app/workers/azure-workload-worker/requirements.txt

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "workers/azure-workload-worker/main.py"]
