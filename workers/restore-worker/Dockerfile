FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY workers/restore-worker /app/workers/restore-worker

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        wget \
        ca-certificates \
        gnupg \
        libgdiplus \
        libicu-dev \
    && wget -q https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends dotnet-runtime-6.0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/workers/restore-worker/requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "workers/restore-worker/main.py"]
