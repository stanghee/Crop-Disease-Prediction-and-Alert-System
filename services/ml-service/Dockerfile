FROM openjdk:11-jre-slim

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copy all ML service components
COPY spark_preprocessing_service.py .
COPY entrypoint.sh .
COPY preprocessing/data_preprocessor.py ./data_preprocessor.py
COPY anomaly-detection/anomaly_detector.py ./anomaly_detector.py
COPY alert-generation/alert_generator.py ./alert_generator.py

# Create models directory
RUN mkdir -p models

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Expose Spark UI port
EXPOSE 4040

# Set entrypoint
ENTRYPOINT ["./entrypoint.sh"] 