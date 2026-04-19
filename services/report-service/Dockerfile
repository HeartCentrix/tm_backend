FROM python:3.11-slim

WORKDIR /app

COPY requirements.base.txt .
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r requirements.base.txt

COPY shared/ ./shared/
COPY services/report-service/ ./services/report-service/

EXPOSE 8014

CMD ["sh", "-c", "uvicorn services.report-service.main:app --host 0.0.0.0 --port 8014"]
