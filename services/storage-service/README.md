# 3-Zone Data Lake Pipeline for Agricultural Monitoring

## General Architecture

The pipeline implements a **3-zone Data Lake structure** (Bronze, Silver, Gold) following the **Medallion Architecture** pattern to ensure progressive data quality improvement and optimized analytics performance.

```
Kafka Topics → Bronze Zone → Silver Zone → Gold Zone → ML Service → Hybrid Storage
     ↓              ↓             ↓            ↓           ↓
Raw Data    Immutable Raw   Validated &   Dashboard KPIs  ML Predictions
            + Metadata      Cleaned                       & Alerts
```

## Zone Structure

### 🥉 BRONZE ZONE (Raw Data Lake)
**Principle**: Immutable data as close as possible to the original form

### 🥈 SILVER ZONE (Curated Data Lake) 
**Principle**: Validated, cleaned data with consistent schema

### 🥇 GOLD ZONE (Business-Ready Data Lake)
**Principle**: Aggregated KPIs for dashboard

---

## 1. IoT SENSOR DATA

### 🥉 BRONZE ZONE - IoT Sensors

**Source**: Kafka topic `sensor_data`

**Original Data Structure**:
```json
{
  "timestamp": "2024-01-15T14:30:00+01:00",
  "field_id": "field_01",
  "temperature": 24.5,
  "humidity": 65.2,
  "soil_ph": 6.8
}
```

**Bronze Transformations**:
- ✅ Addition of ingestion timestamp
- ✅ Addition of Kafka metadata (offset, partition, topic)
- ✅ Temporal partitioning (year, month, day, hour)
- ❌ NO validation or cleaning

**Storage Format**: JSON
**Path**: `s3a://bronze/iot/`
**Partitioning**: By `year`, `month`, `day`, `hour`
**Schema**:
```python
StructType([
    StructField("timestamp", StringType(), True),           # Original timestamp
    StructField("field_id", StringType(), True),           # Field identifier
    StructField("temperature", DoubleType(), True),        # Temperature in °C
    StructField("humidity", DoubleType(), True),           # Humidity %
    StructField("soil_ph", DoubleType(), True),           # Soil pH
    StructField("kafka_topic", StringType(), True),       # Kafka topic
    StructField("kafka_partition", LongType(), True),     # Kafka partition
    StructField("kafka_offset", LongType(), True),        # Kafka offset
    StructField("kafka_timestamp", StringType(), True),   # Kafka ingestion time
    StructField("year", IntegerType(), True),             # Temporal partitioning
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True)
])
```

### 🥈 SILVER ZONE - IoT Sensors

**Silver Transformations**:
- ✅ Value range validation (temp: -20°C to 60°C, humidity: 0-100%, pH: 3-9)
- ✅ Timestamp parsing with timezone handling
- ✅ Addition of derived columns (date, hour, day_of_week, month, year)
- ✅ Data validity flags (temperature_valid, humidity_valid, ph_valid)
- ✅ Removal of records with critical null values

**Storage Format**: Parquet (compressed, schema evolution)
**Path**: `s3a://silver/iot/`
**Partitioning**: By `date` and `field_id`
**Additional Schema**:
```python
# Original schema +
StructField("timestamp_parsed", TimestampType(), True),
StructField("date", DateType(), True),
StructField("hour", IntegerType(), True),
StructField("day_of_week", IntegerType(), True),
StructField("month", IntegerType(), True),
StructField("year", IntegerType(), True),
StructField("temperature_valid", BooleanType(), True),
StructField("humidity_valid", BooleanType(), True),
StructField("ph_valid", BooleanType(), True)
```

### 🥇 GOLD ZONE - IoT Sensors

**Gold Transformations**:

#### ML Features with Sliding Window (`s3a://gold/ml_features/`)
**10-minute sliding window aggregations per field**:
```python
# Sensor aggregations (10-minute window)
- sensor_avg_temperature, sensor_std_temperature, sensor_min_temperature, sensor_max_temperature, sensor_temp_range
- sensor_avg_humidity, sensor_std_humidity, sensor_min_humidity, sensor_max_humidity, sensor_humidity_range
- sensor_avg_soil_ph, sensor_std_soil_ph, sensor_min_soil_ph, sensor_max_soil_ph, sensor_ph_range
- sensor_temp_valid_rate, sensor_humidity_valid_rate, sensor_ph_valid_rate
- sensor_readings_count, sensor_anomaly_count, sensor_anomaly_rate, sensor_data_quality_score

# Weather aggregations (10-minute window)
- weather_avg_temperature, weather_std_temperature, weather_min_temperature, weather_max_temperature, weather_temp_range
- weather_avg_humidity, weather_std_humidity, weather_min_humidity, weather_max_humidity, weather_humidity_range
- weather_avg_wind_speed, weather_std_wind_speed, weather_min_wind_speed, weather_max_wind_speed, weather_wind_range
- weather_avg_uv_index, weather_std_uv_index, weather_min_uv_index, weather_max_uv_index, weather_uv_range
- weather_dominant_condition, weather_readings_count

# Derived features
- temp_differential, humidity_differential
- environmental_stress_score (HIGH/MEDIUM/LOW)
- combined_risk_score (HIGH/MEDIUM/LOW)
- sensor_data_freshness_minutes, weather_data_freshness_minutes

# Processing metadata
- processing_timestamp, window_start_time, window_end_time, window_duration_minutes
```

**Caratteristiche**:
- **Finestra scorrevole**: 10 minuti di dati aggregati
- **Join location-based**: Sensor e weather data uniti per location
- **3 record per file**: Uno per ogni field (field_01, field_02, field_03)
- **Timestamp nel nome**: `ml_features_YYYY-MM-DD_HH-MM`
- **Formato**: Parquet compresso con Snappy
- **Overwrite mode**: Ogni esecuzione sovrascrive il file precedente

**Storage Format**: Parquet (compressed with Snappy)
**Path**: `s3a://gold/ml_features/ml_features_{timestamp}`
**Partitioning**: Nessuna (file singolo con timestamp nel nome)

**Availability**:
- **ML Service**: Lettura diretta dei file ML features per predizioni
- **Dashboard**: Query sui file ML features per visualizzazioni
- **Monitoring**: Real-time risk assessment e data quality metrics

---

## 2. WEATHER DATA

### 🥉 BRONZE ZONE - Weather

**Source**: Kafka topic `weather_data` (WeatherAPI)

**Original Data Structure**:
```json
{
  "message_id": "uuid-12345",
  "timestamp": "2024-01-15T14:30:00+01:00",
  "location": "Verona",
  "region": "Veneto",
  "country": "Italy",
  "lat": 45.4384,
  "lon": 10.9916,
  "temp_c": 18.5,
  "humidity": 72,
  "wind_kph": 12.5,
  "condition": "Partly cloudy",
  "uv": 4.2
}
```

**Bronze Transformations**:
- ✅ Complete API response preservation
- ✅ Addition of ingestion metadata
- ✅ Temporal partitioning (year, month, day, hour)
- ❌ NO validation

**Format**: JSON
**Path**: `s3a://bronze/weather/`
**Partitioning**: By `year`, `month`, `day`, `hour`

### 🥈 SILVER ZONE - Weather

**Silver Transformations**:
- ✅ Temperature range validation (-50°C to 60°C)
- ✅ Humidity validation (0-100%)
- ✅ Geographic coordinates validation
- ✅ Timestamp parsing and timezone handling
- ✅ Addition of derived columns (date, hour, day_of_week, month, year)
- ✅ Data validity flags (temp_valid, humidity_valid, coordinates_valid)

**Format**: Parquet
**Path**: `s3a://silver/weather/`
**Partitioning**: By `date` and `location`

### 🥇 GOLD ZONE - Weather

**Note**: Weather data Gold zone processing is not currently implemented in the GoldZoneProcessor.
Weather data is processed through Bronze and Silver zones only.

---

## 3. SATELLITE IMAGES

### 🥉 BRONZE ZONE - Satellite

**Source**: Kafka topic `satellite_data` (Copernicus Sentinel-2)

**Original Data Structure**:
```json
{
  "timestamp": "2024-01-15T14:30:00Z",
  "image_base64": "iVBORw0KGgoAAAANSUhEUgAA...",
  "location": {
    "bbox": [10.894444, 45.266667, 10.909444, 45.281667]
  }
}
```

**Bronze Transformations**:
- ✅ Image extraction from base64
- ✅ Image storage on separate MinIO bucket
- ✅ Metadata record creation with image path
- ✅ Bounding box coordinates extraction
- ✅ Temporal partitioning

**Image Format**: PNG on MinIO bucket `satellite-images`
**Metadata Format**: JSON
**Path**: `s3a://bronze/satellite_data/`
**Metadata Schema**:
```python
{
  "timestamp": "2024-01-15T14:30:00Z",
  "image_path": "s3a://satellite-images/satellite_2024-01-15T14-30-00Z.png",
  "bbox_min_lon": 10.894444,
  "bbox_min_lat": 45.266667,
  "bbox_max_lon": 10.909444,
  "bbox_max_lat": 45.281667,
  "image_size_bytes": 1024000
}
```

### 🥈 SILVER ZONE - Satellite

**Silver Transformations**:
- ✅ Image metadata validation
- ✅ File integrity check
- ✅ Geographic coordinates validation
- ✅ Coverage area calculation (km²)
- ✅ Image quality assessment (cloud coverage, resolution)

**Format**: Parquet (metadata) + PNG (validated images)
**Path**: `s3a://silver/satellite_data/`
**Partitioning**: By `date`

### 🥇 GOLD ZONE - Satellite

**Note**: Satellite data Gold zone processing is not currently implemented in the GoldZoneProcessor.
Satellite data is processed through Bronze and Silver zones only.

---

## 4. PIPELINE PROCESSING

### Streaming Processing (Bronze Zone)
- **Frequency**: Real-time (30 seconds micro-batches)
- **Technology**: Spark Structured Streaming
- **Checkpointing**: Fault-tolerance with automatic recovery

### Streaming Processing (Silver Zone)
- **Frequency**: Real-time (1 minute micro-batches)
- **Technology**: Spark Structured Streaming
- **Checkpointing**: Fault-tolerance with automatic recovery

### Batch Processing (Gold Zone)  
- **Frequency**: Every 5 minutes
- **Technology**: Spark Batch Jobs
- **Orchestration**: Internal scheduling with retry logic

### Schema Evolution
- **Bronze**: Flexible schema (JSON)
- **Silver**: Validated schema with evolution support
- **Gold**: Optimized schema for query performance



