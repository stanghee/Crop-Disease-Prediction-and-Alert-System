# Use the same Spark image as storage-service for consistency
FROM apache/spark:3.5.0-python3

# Switch to root to install additional packages
USER root

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download additional JARs for S3 integration and Kafka streaming
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    wget -O /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -O /opt/spark/jars/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    wget -O /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -O /opt/spark/jars/lz4-java-1.8.0.jar https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    wget -O /opt/spark/jars/zstd-jni-1.5.5-6.jar https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-6/zstd-jni-1.5.5-6.jar

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables for our app
ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
ENV CLASSPATH="/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar:/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar:/opt/spark/jars/kafka-clients-3.4.1.jar:/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar:/opt/spark/jars/commons-pool2-2.11.1.jar:/opt/spark/jars/lz4-java-1.8.0.jar:/opt/spark/jars/zstd-jni-1.5.5-6.jar"
ENV SPARK_CLASSPATH="/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar:/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar:/opt/spark/jars/kafka-clients-3.4.1.jar:/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar:/opt/spark/jars/commons-pool2-2.11.1.jar:/opt/spark/jars/lz4-java-1.8.0.jar:/opt/spark/jars/zstd-jni-1.5.5-6.jar"

# Create necessary directories with proper permissions
RUN mkdir -p /tmp/ml-checkpoints /app/logs && \
    chmod -R 777 /tmp/ml-checkpoints && \
    chown -R spark:spark /app/logs /tmp/ml-checkpoints

# Switch back to spark user for security
USER spark

# Expose ports
EXPOSE 8002 4041

# Start the service
CMD ["python3", "main.py"]
