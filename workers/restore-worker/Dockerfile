# --- Stage 1: build the vendored pstwriter CLI ----------------------------
# Compiles the standalone C++17 PST writer (vendored from `PST Dev`) and
# produces the `pst_convert` binary the worker shells out to. Aspose.Email
# is no longer used; the .NET runtime stage was removed with this change.
FROM debian:bookworm-slim AS pstwriter-build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cmake \
        g++ \
        ninja-build \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY vendor/pstwriter /src

RUN cmake -S /src -B /src/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
 && cmake --build /src/build --target pst_convert \
 && /src/build/bin/pst_convert 2>/dev/null || true   # smoke: prints usage on argc!=4

# --- Stage 2: Python runtime ----------------------------------------------
FROM python:3.11-slim

WORKDIR /app

# Runtime libraries the pst_convert ELF needs (libstdc++, libgcc_s).
# python:3.11-slim already ships them; this RUN is a no-op safety net.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Drop the pst_convert binary from the build stage onto PATH.
COPY --from=pstwriter-build /src/build/bin/pst_convert /usr/local/bin/pst_convert
RUN chmod +x /usr/local/bin/pst_convert \
 && /usr/local/bin/pst_convert 2>/dev/null || true   # confirm it loads

COPY shared /app/shared
COPY workers/restore-worker /app/workers/restore-worker

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir \
    -r /app/workers/restore-worker/requirements.txt

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Path to the bundled CLI; pstwriter_cli.py honours this override.
ENV PSTWRITER_CLI=/usr/local/bin/pst_convert

CMD ["python", "workers/restore-worker/main.py"]
