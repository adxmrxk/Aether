# AetherFlow local ingest worker
# Build context is the repository root: docker build -f ingest/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /app

COPY ingest/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY ingest/ /app/ingest/

RUN chown -R appuser:appuser /app
USER appuser

CMD ["python", "-u", "ingest/local_ingest.py"]
