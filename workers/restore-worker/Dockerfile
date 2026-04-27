# Pull .NET 8 runtime from the official Microsoft image (avoids apt repo / CDN issues)
FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim AS dotnet-runtime

FROM python:3.11-slim

WORKDIR /app

# Copy .NET runtime from the official image
COPY --from=dotnet-runtime /usr/share/dotnet /usr/share/dotnet

ENV DOTNET_ROOT=/usr/share/dotnet
ENV PATH=$PATH:/usr/share/dotnet

# Use .NET invariant globalization mode — skips the ICU library version dependency.
# PST content (email bodies, subjects, contacts) is stored as binary MAPI/UTF-16;
# locale-sensitive string ops are not used in PST generation.
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

COPY shared /app/shared
COPY workers/restore-worker /app/workers/restore-worker

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir \
    -r /app/workers/restore-worker/requirements.txt

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "workers/restore-worker/main.py"]
