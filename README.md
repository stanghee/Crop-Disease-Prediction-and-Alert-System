# 🌾 Crop Disease Prediction and Alert System

## 📋 Overview

Distributed system for crop disease prediction and alerting based on Big Data technologies. The system collects, processes, and analyzes data from IoT sensors, weather stations, and satellite imagery to provide real-time predictions on potential crop diseases.

## 🏗️ Architecture

The system is based on a **distributed microservices architecture** that uses:

- **Apache Kafka** for asynchronous messaging and data streaming
- **Apache Spark** for distributed processing and machine learning
- **PostgreSQL** for structured data persistence
- **MinIO** for object storage (satellite images)
- **Docker** for containerization and orchestration

### Main Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Processing    │    │     Storage     │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ Weather API     │───▶│ Kafka Topics    │───▶│ PostgreSQL      │
│ IoT Sensors     │    │ Spark ML        │    │ MinIO           │
│ Satellite API   │    │ Data Validation │    │ Volume Storage  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB+ available RAM
- API keys for external services

### Configuration

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Crop-Disease-Prediction-and-Alert-System
   ```

2. **Configure environment variables**
   ```bash
   # Create .env file in project root
   WEATHER_API_KEY=your_weather_api_key
   DEFAULT_LOCATION=Verona
   COPERNICUS_CLIENT_ID=your_copernicus_client_id
   COPERNICUS_CLIENT_SECRET=your_copernicus_client_secret
   
   # Geographic area configuration (optional)
   BBOX_MIN_LON=10.894444
   BBOX_MIN_LAT=45.266667
   BBOX_MAX_LON=10.909444
   BBOX_MAX_LAT=45.281667
   ```

3. **Start the system**
   ```bash
   docker-compose up --build -d
   ```

4. **Verify operation**
   ```bash
   docker-compose ps
   docker-compose logs -f
   ```

### Service Access

- **Spark UI**: http://localhost:4040
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **PostgreSQL**: localhost:5432 (postgres/postgres)
- **Kafka**: localhost:9092

## 📊 Services and Components

### 🌡️ Weather Service
**Weather data collection**

- **Producer**: Collects data from WeatherAPI every 60 seconds
- **Consumer**: Processes and saves data to `weatherdb` database
- **Data**: temperature, humidity, wind, UV, weather conditions

### 📡 Sensor Service  
**IoT field sensor simulation**

- **Producer**: Simulates 3 fields with multiple sensors
- **Consumer**: Processes sensor data for ML training
- **Data**: temperature, humidity, soil pH
- **Configuration**: 30% anomaly probability

### 🛰️ Satellite Service
**Satellite image processing**

- **Producer**: Collects images from Copernicus API
- **Consumer**: Saves processed images to MinIO
- **Storage**: `satellite-images` bucket
- **Area**: Configurable (default: Verona)

### 🤖 ML Service (Spark)
**Distributed processing and machine learning**

- **Preprocessing**: Data validation and normalization
- **Anomaly Detection**: Real-time anomaly detection
- **Alert Generation**: Crop disease alert generation
- **Streaming**: Continuous processing with Spark Streaming

## 📈 Data Flow and Kafka Topics

### Raw Data Topics
- `sensor_data` - IoT sensor data
- `weather_data` - Weather data
- `satellite_data` - Satellite images

### Processed Topics
- `processed_sensor_data` - Validated sensor data
- `processed_weather_data` - Validated weather data
- `processed_satellite_data` - Validated image metadata

### Alert Topics
- `crop_disease_alerts` - ML alerts for diseases

### Processing Flow
```
Raw Data → Kafka → Spark Processing → Validation → Database/Storage
                                   ↓
                            ML Analysis → Alerts
```

## 🗄️ Database Schema

### SensorDB (`sensordb`)
```sql
sensor_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    field_id TEXT,
    temperature REAL,
    humidity REAL,
    soil_ph REAL,
    data_quality TEXT
);
```

### WeatherDB (`weatherdb`)
```sql
weather_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    location TEXT,
    region TEXT,
    country TEXT,
    lat REAL,
    lon REAL,
    temp_c REAL,
    humidity INT,
    wind_kph REAL,
    condition TEXT,
    uv REAL,
    message_id TEXT,
    data_quality TEXT
);
```

## 🔧 Configuration

### Spark ML Service
- **Parallelism**: 4 cores
- **Memory**: 2GB limit, 1GB reserved
- **Checkpointing**: Persistent volume
- **Timezone**: Europe/Rome

### Anomaly Detection
- **Temperature**: Normal range -10°C to 50°C
- **Humidity**: Normal range 10% to 95%
- **Soil pH**: Normal range 4.5 to 8.0
- **Anomaly Probability**: 30% (configurable)

### Resource Limits
- **Kafka**: 1GB RAM
- **PostgreSQL**: 1GB RAM + 256MB shared memory
- **Spark ML**: 2GB RAM
- **Other services**: 256MB RAM

## 🛠️ Technologies Used

### Core Technologies
- **Apache Kafka 7.4.0** - Streaming platform
- **Apache Spark 3.5.0** - Distributed processing
- **PostgreSQL** - Relational database
- **MinIO** - Object storage
- **Docker** - Containerization

### Python Libraries
- **PySpark** - Spark integration
- **Pandas & NumPy** - Data manipulation
- **Scikit-learn** - Machine learning
- **Kafka-Python** - Kafka client
- **Psycopg2** - PostgreSQL adapter

### Integrated APIs
- **WeatherAPI** - Weather data
- **Copernicus** - Sentinel satellite imagery




