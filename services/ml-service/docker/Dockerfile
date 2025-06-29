FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME automatically
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose Spark UI port
EXPOSE 4040

# Create directories for models and checkpoints
RUN mkdir -p models spark-checkpoints

CMD ["python", "spark_preprocessing_service.py"] 