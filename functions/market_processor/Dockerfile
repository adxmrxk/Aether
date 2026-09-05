# Runs the Cloud Function locally under functions-framework, so the same
# handler that deploys to Cloud Functions Gen2 is exercised by the Compose
# stack. Build context is the repo root.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /app

COPY functions/market_processor/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY functions/market_processor/ /app/

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

CMD ["functions-framework", "--target=process_market_data", \
     "--signature-type=cloudevent", "--host=0.0.0.0", "--port=8080"]
