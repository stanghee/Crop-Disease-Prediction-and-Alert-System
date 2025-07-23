# 3-Zone Medallion Data Lake for Agricultural Monitoring

## Overview

The pipeline implements a **3-zone Data Lake structure** (Bronze, Silver, Gold) following the **Medallion Architecture** pattern to ensure progressive data quality improvement and optimized analytics performance.

```
Kafka Topics → Bronze Zone → Silver Zone → Gold Zone 
     ↓              ↓             ↓            ↓           
Raw Data    Immutable Raw   Validated &   ML Features  
            + Metadata      Cleaned                       
```

## Zone Structure

### 🥉 BRONZE ZONE (Raw Data Lake)
**Principle**: Immutable data as close as possible to the original form


### 🥈 SILVER ZONE (Curated Data Lake) 
**Principle**: Validated, cleaned data with consistent schema


### 🥇 GOLD ZONE (ML-Ready Data Lake)
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
    StructField("timestamp", StringType(), True),         # Original timestamp
    StructField("field_id", StringType(), True),          # Field identifier
    StructField("temperature", DoubleType(), True),       # Temperature in °C
    StructField("humidity", DoubleType(), True),          # Humidity %
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
  - Usefull information implemented for a possible Machine Learning in the field of timeseries 
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

### 🥇 GOLD ZONE - Satellite

- Check point 4

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

- Check point 4 

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
Satellite data is processed through Bronze and Silver zones only. Tt present, we have preferred not to use the images even through machine learning models due to the low quality and poor time availability determined by the free service offered by Copernicus.

---
## 4. 🥇 GOLD ZONE - ML ready

**Key Transformations (see `gold_zone_processor.py`):**
- Sliding window aggregations (typically 10-minute windows) to compute temporal statistics on sensor and weather data.
- Join operations between sensor and weather datasets on location and time window, providing a unified view of environmental conditions.
- Advanced feature engineering for ML, including:
  - Calculation of means, variances, min/max, trends, and other statistics for temperature, humidity, pH, etc.
  - Anomaly and data quality indicators.
  - Derived features tailored for predictive models and classifiers.
- Output written as compressed Parquet files (Snappy), with one file per processed time window.

**Format:** Parquet (Snappy compression)  
**Path:** `s3a://gold/ml_feature/`  
**Partitioning:** By time window (e.g., every 10 minutes, with timestamp in the filename)

---

## 5. PIPELINE PROCESSING

### Streaming Processing (Bronze Zone)
- **Frequency**: Real-time (30 seconds micro-batches)
- **Technology**: Spark Structured Streaming


### Streaming Processing (Silver Zone)
- **Frequency**: Real-time (1 minute micro-batches)
- **Technology**: Spark Structured Streaming


### Batch Processing (Gold Zone)  
- **Frequency**: Every 10 minutes
- **Technology**: Spark Batch Jobs





