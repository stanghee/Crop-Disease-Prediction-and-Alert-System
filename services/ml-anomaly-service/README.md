# ML Anomaly Detection Service

## Quick Start - Testing the ML System

### 1. Start the System
```bash
docker-compose up -d
```

### 2. Wait for Data Collection (Important!)
The system needs some data before ML can work:
- Wait ~10-15 minutes for Gold zone to have ML features
- Check Gold processor logs: `docker logs crop-disease-prediction-and-alert-system-spark-data-lake-service-1`

### 3. Check ML Service Health
```bash
# Check if ML service is running
curl http://localhost:8002/health

# Check service status
curl http://localhost:8002/status
```

### 4. Trigger Initial Model Training
```bash
# This will train with whatever data is available
curl -X POST http://localhost:8002/train/immediate
```

### 5. Check Model Info
```bash
# See if model was trained successfully
curl http://localhost:8002/model/info
```

### 6. Start Streaming Inference
```bash
# Start real-time anomaly detection
curl -X POST http://localhost:8002/inference/start
```

### 7. View Recent Predictions
```bash
# See ML predictions
curl http://localhost:8002/predictions/recent

# Filter by field
curl "http://localhost:8002/predictions/recent?field_id=field_01"
```

### 8. Check PostgreSQL for Results
```bash
# Connect to PostgreSQL
docker exec -it crop-disease-prediction-and-alert-system-postgres-1 psql -U ml_user -d crop_disease_ml

# View predictions
SELECT * FROM ml_predictions ORDER BY timestamp DESC LIMIT 10;

# View anomalies only
SELECT * FROM ml_predictions WHERE is_anomaly = true ORDER BY timestamp DESC;
```

### 9. Monitor Kafka Topics
```bash
# Check if ML anomalies are being published
docker exec -it crop-disease-prediction-and-alert-system-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic ml-anomalies --from-beginning
```

## API Endpoints

- `GET /health` - Service health check
- `GET /status` - Detailed service status
- `POST /train` - Trigger training (background)
- `POST /train/immediate` - Immediate training
- `GET /predictions/recent` - View recent predictions
- `GET /model/info` - Current model information
- `POST /inference/start` - Start streaming
- `POST /inference/stop` - Stop streaming

## Troubleshooting

### No data for training
- Check if Gold zone has data: `docker exec -it crop-disease-prediction-and-alert-system-minio-1 mc ls minio/gold/ml_feature/`
- Wait more time or check data pipeline logs

### Model training fails
- Check logs: `docker logs crop-disease-prediction-and-alert-system-ml-anomaly-service-1`
- Ensure MinIO is accessible
- Check if enough data exists (minimum 10 records)

### No predictions appearing
- Ensure Gold processor is publishing to Kafka
- Check if streaming is active: `curl http://localhost:8002/status`
- Verify Kafka connectivity

## Architecture

```
Gold Zone (Parquet) → Kafka (gold-ml-features) → ML Service → Kafka (ml-anomalies)
                 ↓                                          ↓
              Training                                PostgreSQL
               Model
``` 