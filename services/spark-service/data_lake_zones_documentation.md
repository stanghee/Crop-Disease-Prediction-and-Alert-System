# 3-Zone Data Lake Pipeline for Agricultural Monitoring

## General Architecture

The pipeline implements a **3-zone Data Lake structure** (Bronze, Silver, Gold) following the **Delta Lake** methodology to ensure ACID transactions, schema evolution, and time travel capabilities.

```
Kafka Topics → Bronze Zone → Silver Zone → Gold Zone → Dashboard
     ↓              ↓             ↓            ↓
Raw Data    Immutable Raw   Validated &   Dashboard KPIs
            + Metadata      Cleaned       
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

#### A) Real-time Metrics Table (`s3a://gold/realtime_metrics/`)
**30-minute aggregations per field**:
```python
- current_temperature, current_humidity, current_soil_ph
- avg_temperature_30min, std_temperature_30min, min_temperature_30min, max_temperature_30min
- avg_humidity_30min, std_humidity_30min, min_humidity_30min, max_humidity_30min
- avg_soil_ph_30min, std_soil_ph_30min
- temperature_valid_rate_30min, humidity_valid_rate_30min, ph_valid_rate_30min
- readings_count_30min, temp_range_30min, humidity_range_30min
- status (HEALTHY/DATA_ISSUE), data_freshness_minutes, is_online
```

#### B) Hourly Aggregations Table (`s3a://gold/hourly_aggregations/`)
**Hourly statistics for trend analysis**:
```python
- avg_temperature, std_temperature, min_temperature, max_temperature
- avg_humidity, std_humidity, min_humidity, max_humidity
- avg_soil_ph, std_soil_ph
- temperature_valid_rate, humidity_valid_rate, ph_valid_rate
- readings_count, temp_range, humidity_range
- hourly_quality_status (VALID/DATA_ISSUE)
```

#### C) Alert Summary Table (`s3a://gold/alert_summary/`)
**Data quality and alert metrics**:
```python
- invalid_temperature_count, invalid_humidity_count, invalid_ph_count
- total_readings, invalid_rate
- alert_status (HIGH/MEDIUM/LOW)
```

#### D) Field Performance Ranking Table (`s3a://gold/field_performance_ranking/`)
**Field performance and quality assessment**:
```python
- temperature_valid_rate, humidity_valid_rate, ph_valid_rate
- overall_validity_score, performance_grade (A/B/C/D/F)
- performance_rank, total_readings
```

#### E) Dashboard Overview Table (`s3a://gold/dashboard_overview/`)
**System-wide statistics and KPIs**:
```python
- total_fields, grade_a_fields, grade_b_fields, grade_c_fields, grade_d_fields, grade_f_fields
- avg_validity_score, avg_temperature_valid_rate, avg_humidity_valid_rate, avg_ph_valid_rate
```

**Storage Format**: Delta Lake (ACID, versioning)
**Paths**: 
- `s3a://gold/realtime_metrics/`
- `s3a://gold/hourly_aggregations/`
- `s3a://gold/alert_summary/`
- `s3a://gold/field_performance_ranking/`
- `s3a://gold/dashboard_overview/`

**Availability**:
- **Dashboard**: SQL queries on Delta tables via Spark SQL
- **Monitoring**: Real-time KPIs and performance metrics

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

---

## 5. DATA ACCESS

### For Dashboard
```python
# Real-time KPIs
dashboard_data = spark.read.format("delta").load("s3a://gold/realtime_metrics/")

# Time-series analysis
historical_data = spark.read.format("delta").load("s3a://gold/hourly_aggregations/") \
    .filter(col("date") >= "2024-01-01")

# Field performance
performance_data = spark.read.format("delta").load("s3a://gold/field_performance_ranking/")

# System overview
overview_data = spark.read.format("delta").load("s3a://gold/dashboard_overview/")

# Alert summary
alert_data = spark.read.format("delta").load("s3a://gold/alert_summary/")
```

### Query Performance
- **Partitioning**: Optimized for temporal and geographic queries
- **Indexing**: Z-ordering on Delta tables for multi-dimensional queries
- **Caching**: Spark cache for frequently accessed tables

---

## 6. MONITORING AND DATA QUALITY

### Data Quality Checks
- **Bronze**: Data volume monitoring and lag
- **Silver**: Validation rules and data profiling
- **Gold**: Business rules validation and anomaly detection

### Metrics
- **Throughput**: Records/second per source
- **Latency**: End-to-end processing time
- **Quality**: % records passed validation
- **Availability**: Service uptime and data freshness

---

## 7. SECURITY AND GOVERNANCE

### Backup & Recovery
- **Disaster Recovery**: MinIO replication + Spark checkpointing

This architecture ensures **scalability**, **reliability**, and **performance** for the agricultural monitoring system, supporting both real-time analysis and batch processing for crop disease prevention. 