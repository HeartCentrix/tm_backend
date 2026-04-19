FROM python:3.11

WORKDIR /app

# Install build dependencies for compiling C extensions (cryptography, yarl, cffi, etc.)
# Plus ODBC driver 18 for SQL Server (2026 Afi.ai standard)
# Plus SqlPackage CLI for passwordless SQL backup (multi-TB support)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
        libssl-dev \
        libpq-dev \
        pkg-config \
        curl \
        gnupg2 \
        ca-certificates \
        unzip \
    && curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/* \
    && curl -Lo /tmp/sqlpackage.zip https://aka.ms/sqlpackage-linux \
    && mkdir -p /opt/sqlpackage \
    && cd /opt/sqlpackage && unzip /tmp/sqlpackage.zip \
    && chmod +x /opt/sqlpackage/sqlpackage \
    && ln -s /opt/sqlpackage/sqlpackage /usr/local/bin/SqlPackage \
    && rm /tmp/sqlpackage.zip \
    && mkdir -p /data/scratch && chmod 700 /data/scratch

COPY shared /app/shared
COPY workers/azure-workload-worker /app/workers/azure-workload-worker

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir --prefer-binary -r /app/workers/azure-workload-worker/requirements.txt

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "workers/azure-workload-worker/main.py"]
