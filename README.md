# Crop Disease Prediction and Alert System

## Sistema Big Data per Monitoraggio Agricolo in Real-time

Questo progetto implementa una pipeline Big Data completa per il rilevamento e la previsione delle malattie delle colture utilizzando:

- **Sensori IoT** (temperatura, umidità, pH del suolo)
- **Dati meteorologici** (API esterne)
- **Immagini satellitari** (Copernicus Sentinel-2)
- **Apache Spark** per elaborazione batch e streaming
- **Architettura Data Lake a 3 zone** (Bronze, Silver, Gold)

## 🏗️ Architettura del Sistema

### Data Lake a 3 Zone

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kafka Topics  │───▶│   BRONZE ZONE   │───▶│   SILVER ZONE   │───▶│    GOLD ZONE    │
│                 │    │                 │    │                 │    │                 │
│ • sensor_data   │    │ Raw Data (JSON) │    │ Validated Data  │    │ ML Features &   │
│ • weather_data  │    │ Immutable       │    │ (Parquet)       │    │ KPIs (Delta)    │
│ • satellite_data│    │ + Metadata      │    │ Cleaned         │    │ Business Ready  │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Tecnologie Utilizzate

- **Apache Spark 3.5.0** - Processing engine
- **Delta Lake** - ACID transactions, versioning
- **Apache Kafka** - Streaming data ingestion
- **MinIO** - S3-compatible object storage
- **PostgreSQL** - Metadata storage
- **Docker & Docker Compose** - Containerization

## 🚀 Quick Start

### Prerequisiti

- Docker & Docker Compose
- Almeno 8GB RAM disponibili
- Credenziali API:
  - WeatherAPI key
  - Copernicus Sentinel Hub credentials

### 1. Setup Environment

```bash
# Clone repository
git clone <repository-url>
cd Crop-Disease-Prediction-and-Alert-System

# Create environment file
cp .env.example .env

# Edit .env with your API keys
vim .env
```

### 2. Avvio del Sistema

```bash
# Start all services
docker-compose up -d

# Check services status
docker-compose ps

# View logs
docker-compose logs -f spark-data-lake-service
```

### 3. Accesso alle UI

- **Spark UI**: http://localhost:4040
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Kafka**: localhost:9092

## 📊 Struttura dei Dati

### 🥉 BRONZE ZONE (Raw Data)

#### Sensori IoT
```json
{
  "timestamp": "2024-01-15T14:30:00+01:00",
  "field_id": "field_01",
  "temperature": 24.5,
  "humidity": 65.2,
  "soil_ph": 6.8,
  "kafka_timestamp": "2024-01-15T14:30:15Z",
  "ingestion_timestamp": "2024-01-15T14:30:16Z",
  "source_type": "sensor"
}
```

#### Dati Meteo
```json
{
  "message_id": "uuid-12345",
  "timestamp": "2024-01-15T14:30:00+01:00",
  "location": "Verona",
  "temp_c": 18.5,
  "humidity": 72,
  "wind_kph": 12.5,
  "condition": "Partly cloudy",
  "uv": 4.2
}
```

#### Immagini Satellitari
```json
{
  "timestamp": "2024-01-15T14:30:00Z",
  "image_path": "s3a://satellite-images/satellite_2024-01-15T14-30-00Z.png",
  "bbox_min_lon": 10.894444,
  "bbox_min_lat": 45.266667,
  "bbox_max_lon": 10.909444,
  "bbox_max_lat": 45.281667
}
```

### 🥈 SILVER ZONE (Validated Data)

- **Formato**: Parquet (compresso)
- **Validazione**: Range check, anomaly detection
- **Partitioning**: Per data e field_id/location
- **Schema**: Enforced e versionato

### 🥇 GOLD ZONE (ML Features & KPIs)

#### ML Features per Sensori
```sql
SELECT 
  field_id,
  date,
  avg_temperature,
  std_temperature,
  anomaly_rate,
  risk_score,
  temp_7day_avg,
  environmental_risk_score
FROM delta.`s3a://gold/sensor_ml_features/`
```

#### Dashboard KPIs
```sql
SELECT 
  field_id,
  current_temperature,
  current_humidity,
  overall_status,
  week_anomaly_rate
FROM delta.`s3a://gold/sensor_dashboard_kpis/`
```

## 🔧 Configurazione

### Variabili d'Ambiente (.env)

```bash
# Weather API
WEATHER_API_KEY=your_weather_api_key
DEFAULT_LOCATION=Verona

# Copernicus Sentinel Hub
COPERNICUS_CLIENT_ID=your_client_id
COPERNICUS_CLIENT_SECRET=your_client_secret

# Geographic area (Verona by default)
BBOX_MIN_LON=10.894444
BBOX_MIN_LAT=45.266667
BBOX_MAX_LON=10.909444
BBOX_MAX_LAT=45.281667
```

### Scaling Configuration

```yaml
# docker-compose.override.yml
services:
  spark-data-lake-service:
    environment:
      - SPARK_PARALLELISM=8
    mem_limit: 6g
    mem_reservation: 4g
```

## 📈 Monitoring e Debugging

### Logs dei Servizi
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark-data-lake-service
docker-compose logs -f sensor-producer
```

### Spark UI
- Accedi a http://localhost:4040
- Monitora streaming queries
- Verifica performance jobs

### MinIO Console
- Accedi a http://localhost:9001
- Verifica buckets: bronze, silver, gold, satellite-images
- Monitora storage usage

### Health Checks
```bash
# Check system status
curl -f http://localhost:4040/api/v1/applications

# Check MinIO
curl -f http://localhost:9000/minio/health/live

# Check Kafka
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## 🔍 Query dei Dati

### Accesso via Spark SQL

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataAnalysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read ML features
ml_features = spark.read.format("delta").load("s3a://gold/integrated_ml_features/")
ml_features.show()

# Query high-risk fields
high_risk = spark.sql("""
    SELECT field_id, date, risk_score, anomaly_rate
    FROM delta.`s3a://gold/sensor_ml_features/`
    WHERE risk_score = 'HIGH'
    ORDER BY date DESC
""")
```

### Esempi di Analisi

#### 1. Trend Anomalie per Campo
```sql
SELECT 
  field_id,
  date,
  anomaly_rate,
  LAG(anomaly_rate, 1) OVER (PARTITION BY field_id ORDER BY date) as prev_anomaly_rate
FROM delta.`s3a://gold/sensor_ml_features/`
WHERE date >= '2024-01-01'
ORDER BY field_id, date
```

#### 2. Correlazione Meteo-Sensori
```sql
SELECT 
  s.field_id,
  s.date,
  s.avg_temperature as sensor_temp,
  w.avg_temperature as weather_temp,
  ABS(s.avg_temperature - w.avg_temperature) as temp_diff
FROM delta.`s3a://gold/sensor_ml_features/` s
JOIN delta.`s3a://gold/weather_ml_features/` w ON s.date = w.date
WHERE temp_diff > 5
```

## 🚨 Alerting e Machine Learning

### Risk Scoring

Il sistema calcola automaticamente:
- **Anomaly Rate**: % di letture anomale per giorno
- **Risk Score**: LOW/MEDIUM/HIGH basato su soglie
- **Environmental Risk**: Combinazione multi-sorgente
- **Crop Health Prediction**: Previsione stato colture

### Feature per ML

Le feature Gold sono ottimizzate per:
- **Classificazione** malattie colture
- **Regressione** resa prevista
- **Anomaly Detection** in real-time
- **Time Series Forecasting** condizioni future

## 🛠️ Troubleshooting

### Problemi Comuni

#### 1. Spark Out of Memory
```bash
# Increase memory in docker-compose.yml
mem_limit: 4g
mem_reservation: 3g

# Or reduce parallelism
SPARK_PARALLELISM: 2
```

#### 2. Kafka Connection Issues
```bash
# Check Kafka health
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Restart Kafka
docker-compose restart kafka
```

#### 3. MinIO Access Denied
```bash
# Check credentials
docker-compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin

# List buckets
docker-compose exec minio mc ls local/
```

#### 4. Missing API Data
```bash
# Check API keys in .env
cat .env | grep API

# Check producer logs
docker-compose logs weather-producer
docker-compose logs satellite-producer
```

### Reset Sistema
```bash
# Stop all services
docker-compose down

# Remove volumes (ATTENZIONE: cancella tutti i dati)
docker-compose down -v

# Rebuild and restart
docker-compose build --no-cache
docker-compose up -d
```

## 📚 Documentazione Tecnica

### Struttura del Progetto
```
├── services/
│   ├── spark-service/          # Data Lake a 3 zone
│   │   ├── bronze_zone_processor.py
│   │   ├── silver_zone_processor.py
│   │   ├── gold_zone_processor.py
│   │   └── main_service.py
│   ├── sensor-service/         # Produttori IoT
│   ├── weather-service/        # Produttori meteo
│   └── satellite-service/      # Produttori satellitari
├── docker-compose.yml          # Orchestrazione servizi
└── README.md                   # Questa documentazione
```

### API Reference

Per dettagli completi sull'architettura delle zone, vedere:
- [Data Lake Zones Documentation](services/spark-service/data_lake_zones_documentation.md)

## 🤝 Contribuire

1. Fork del repository
2. Crea feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Apri Pull Request

## 📄 Licenza

Questo progetto è rilasciato sotto licenza MIT. Vedere `LICENSE` per dettagli.

## 🙏 Riconoscimenti

- **Apache Spark** community
- **Delta Lake** project
- **Copernicus Sentinel Hub** per i dati satellitari
- **WeatherAPI** per i dati meteorologici 