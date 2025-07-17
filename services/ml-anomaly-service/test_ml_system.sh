#!/bin/bash

echo "=== ML Anomaly Detection System Test ==="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Base URL
BASE_URL="http://localhost:8002"

echo "1. Checking ML Service Health..."
health_response=$(curl -s $BASE_URL/health)
if [[ $health_response == *"healthy"* ]]; then
    echo -e "${GREEN}✓ ML Service is healthy${NC}"
else
    echo -e "${RED}✗ ML Service is not responding${NC}"
    exit 1
fi
echo

echo "2. Checking Service Status..."
status_response=$(curl -s $BASE_URL/status)
echo "Status: $status_response"
echo

echo "3. Triggering Immediate Training..."
train_response=$(curl -s -X POST $BASE_URL/train/immediate)
echo "Training Response: $train_response"
if [[ $train_response == *"success"* ]]; then
    echo -e "${GREEN}✓ Training completed successfully${NC}"
else
    echo -e "${YELLOW}⚠ Training may have failed - check logs${NC}"
fi
echo

echo "4. Checking Model Info..."
model_response=$(curl -s $BASE_URL/model/info)
echo "Model Info: $model_response"
echo

echo "5. Starting Streaming Inference..."
inference_response=$(curl -s -X POST $BASE_URL/inference/start)
echo "Inference Response: $inference_response"
echo

echo "6. Waiting 10 seconds for predictions..."
sleep 10

echo "7. Checking Recent Predictions..."
predictions_response=$(curl -s $BASE_URL/predictions/recent?limit=5)
echo "Recent Predictions: $predictions_response"
echo

echo -e "${GREEN}=== Test Complete ===${NC}"
echo "Check the following for more details:"
echo "- Logs: docker logs crop-disease-prediction-and-alert-system-ml-anomaly-service-1"
echo "- Kafka: docker exec -it crop-disease-prediction-and-alert-system-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic ml-anomalies --from-beginning"
echo "- PostgreSQL: docker exec -it crop-disease-prediction-and-alert-system-postgres-1 psql -U ml_user -d crop_disease_ml -c 'SELECT * FROM ml_predictions ORDER BY timestamp DESC LIMIT 5;'" 