FROM python:3.11-slim
WORKDIR /app
COPY shared/ /app/shared/
COPY workers/discovery-worker/requirements.txt .
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r requirements.txt
COPY workers/discovery-worker/ .
CMD ["python", "main.py"]
 