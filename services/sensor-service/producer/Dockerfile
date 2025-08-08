FROM python:3.9-slim

# Set environment variables to optimize Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install only necessary system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf ~/.cache/pip/*

# Copy only necessary files
COPY producer.py .
COPY wait_for_kafka.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Non-root user for security
RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["./entrypoint.sh"]