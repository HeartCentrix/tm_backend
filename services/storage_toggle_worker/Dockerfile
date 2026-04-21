FROM python:3.11-slim

WORKDIR /app
COPY requirements.base.txt /app/requirements.base.txt
RUN pip install --no-cache-dir -r requirements.base.txt

COPY . /app
ENV PYTHONPATH=/app

CMD ["python", "-m", "services.storage-toggle-worker.main"]
