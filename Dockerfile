FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt snowflake-connector-python azure-identity
COPY src/ src/
COPY config/ config/

ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
RUN useradd --create-home ingest
USER ingest

ENTRYPOINT []
CMD ["python", "-m", "ingest", "plan"]
