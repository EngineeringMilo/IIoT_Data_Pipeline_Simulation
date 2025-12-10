# Use a lightweight Python base image
FROM python:3.12-slim

# Set working directory, all command are relative to /app
WORKDIR /app

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy your ingestion script into container
COPY ingest_data.py .

# Ensure Python output is not buffered (helps with logs in Docker)
ENV PYTHONUNBUFFERED=1

# Optional: set environment variables for Kafka broker
ENV BROKER="redpanda:9092"
ENV INTERVAL=2

# Default command to run your ingestion script
CMD ["python", "ingest_data.py"]