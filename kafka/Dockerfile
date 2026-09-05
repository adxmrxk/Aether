# Kafka ingestion worker. Build context is the repo root.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /app

COPY kafka/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY kafka/ /app/kafka/

RUN chown -R appuser:appuser /app
USER appuser

CMD ["python", "-u", "-m", "kafka.runner"]
