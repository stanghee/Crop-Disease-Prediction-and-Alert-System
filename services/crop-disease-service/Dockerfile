FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Java and procps
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    default-jdk \
    procps \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Note: JAR dependencies are provided by the Spark cluster
# No need to download JARs locally since we connect to existing cluster

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# RIMOSSO: creazione della cartella /app/logs perché non serve più
# RUN mkdir -p /app/logs /tmp/alerts-checkpoints
RUN mkdir -p /tmp/alerts-checkpoints

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose ports
EXPOSE 8000 4042

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["python", "main.py"] 