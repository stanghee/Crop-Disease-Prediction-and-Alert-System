# Storage Service

This component is a comprehensive data storage and processing engine designed to implement a 3-zone Medallion Data Lake architecture for agricultural monitoring. The scripts process raw IoT sensor data, weather information, and satellite imagery through three quality stages (Bronze, Silver, Gold) to create validated, cleaned, and machine learning-ready datasets for agricultural disease prediction and monitoring. 

## Purpose

Our team developed this service to create the foundational data infrastructure that powers our crop disease prediction system. The storage service processes raw IoT sensor data (temperature, humidity, soil pH from 3 agricultural fields), weather conditions, and satellite imagery through a 3-zone Medallion architecture to generate validated datasets that feed directly into our ML anomaly detection models and real-time alert systems. 

## Data Processing Pipeline

### Zone 1: Bronze (Raw Data Lake)
**What it does**: Captures and preserves raw data with comprehensive metadata tracking

**IoT Sensors Stream**:
- **Schema**: 6 variables (`timestamp`, `field_id`, `location`, `temperature`, `humidity`, `soil_ph`)
- **Kafka Topic**: `sensor_data` 
- **Temporal Partitioning**: `year/month/day/hour` structure
- **Output**: JSON format in `s3a://bronze/iot/` with 30-second micro-batches

**Weather Data Stream**:
- **Schema**: 12 fields from WeatherAPI.com (`temp_c`, `humidity`, `wind_kph`, `condition`, `uv`, `coordinates`...) #TODO: fix this part 
- **Kafka Topic**: `weather_data` 
- **Temporal Partitioning**: `year/month/day/hour` structure
- **Output**: JSON format in `s3a://bronze/weather/` with 30-second micro-batches

**Satellite Images Stream**:
- **Schema**: 3 fields (`timestamp`, `image_base64`, `location.bbox`)
- **Kafka Topic**: `satellite_data` 
- **Processing**: Base64 image decoding, PNG extraction, MinIO storage with unique filenames
- **Image Storage**: Direct MinIO storage in `satellite-images` bucket with metadata tracking
- **Output**: Metadata JSON in `s3a://bronze/satellite/` with 60-second micro-batches


### Zone 2: Silver (Curated Data Lake)
**What it does**: Validates and cleans data for analysis with dual output (Parquet storage + Kafka real-time)

**IoT Sensors Validation Stream**:
- **Input**: JSON from `s3a://bronze/iot/` 
- **Validations**:
  - **Completeness**: `temperature`, `humidity`, `soil_ph`, `field_id`, `location`, `timestamp` not null
  - **Range Checks**: `temperature` (-20°C to 60°C), `humidity` (0% to 100%), `soil_ph` (3.0 to 9.0)
  - **Timestamp Parsing**: Multi-format handling with fallback (`SSSSSSXXX` → `XXX`)
- **Dual Output**: 
  - **Parquet**: `s3a://silver/iot/` partitioned by `date, field_id`
  - **Kafka**: Topic `iot_valid_data` with validation flags included
- **Processing**: 1-minute micro-batches 

**Weather Data Validation Stream**:
- **Input**: JSON from `s3a://bronze/weather/` 
- **Validations**:
  - **Completeness**: `temp_c`, `humidity`, `location`, `timestamp` not null
  - **Range Checks**: `temp_c` (-50°C to 60°C), `humidity` (0% to 100%)
  - **Coordinate Validation**: `lat` (-90,90), `lon` (-180,180) with existence checks
- **Dual Output**:
  - **Parquet**: `s3a://silver/weather/` partitioned by `date, location`
  - **Kafka**: Topic `weather_valid_data` with validation flags included
- **Processing**: 1-minute micro-batches 

**Satellite Data Validation Stream**:
- **Input**: JSON from `s3a://bronze/satellite/` 
- **Validations**:
  - **Completeness**: `timestamp`, `image_path`, `image_size_bytes` not null
  - **BBOX Validation**: Geographic coordinates with range and logic checks
  - **Image Size Validation**: 1KB < size < 50MB to prevent corrupted images
- **Output**: **Parquet only** in `s3a://silver/satellite/` partitioned by `date`
- **Processing**: 1-minute micro-batches


### Zone 3: Gold (ML-Ready Data Lake)
**What it does**: Creates features optimized for machine learning with advanced feature engineering

**Sliding Window Processing**:
- **Window Size**: 10-minute aggregations for temporal pattern recognition
- **Input Sources**: Sensor data from `s3a://silver/iot/` and weather data from `s3a://silver/weather/`
- **Location-based Join**: Correlates sensor and weather data by location (Verona in the test phase) for micro-climatic analysis

**Sensor Data Aggregation (per field)**:
- **Temperature**: Average, standard deviation, min/max, range for each field
- **Humidity**: Average, standard deviation, min/max, range for each field
- **Soil pH**: Average, standard deviation, min/max, range for each field
- **Quality Metrics**: Validity rates, anomaly counts, data quality scores

**Weather Data Aggregation (per location)**:
- **Temperature**: Average, standard deviation, min/max, range for Verona area
- **Humidity**: Average, standard deviation, min/max, range for Verona area
- **Wind Speed**: Average, standard deviation, min/max, range for Verona area
- **UV Index**: Average, standard deviation, min/max, range for Verona area #TODO: check this
- **Conditions**: Dominant weather condition (mode) for the time window

**Advanced Feature Engineering**:
- **Differential Features**: Temperature and humidity differences between sensor and weather data
- **Environmental Stress Scoring**: HIGH/MEDIUM/LOW based on differential thresholds 
- **Combined Risk Assessment**: HIGH/MEDIUM/LOW based on anomaly rates and environmental stress
- **Data Freshness**: Minutes since last sensor and weather readings

**Output Strategy**:
- **Kafka Priority**: Topic `gold-ml-features` for real-time ML processing
- **MinIO Storage**: Parquet files with Snappy compression for data lake

## Service Orchestration

**Main Service Controller**:
- **Spark Session**: Cluster-connected with S3 and Kafka support
- **MinIO Integration**: Automatic bucket creation (`bronze`, `silver`, `gold`, `satellite-images`)
- **Zone Coordination**: Orchestrates all three zones with proper startup sequence

**Processing Workflow**:
- **Bronze Zone**: Continuous streaming (30-60 second micro-batches)
- **Silver Zone**: Continuous streaming with dependency management (1-minute micro-batches)
- **Gold Zone**: Scheduled processing every 10 minutes with sliding window

**Error Handling & Resilience**: #TODO: fix it
- **Graceful Degradation**: Continues processing even if individual components fail
- **Checkpoint Management**: Separate checkpoints for each zone and stream
- **Data Freshness Monitoring**: Tracks data staleness for quality assessment

## Technical Architecture

**Core Technologies**:
- **Apache Spark**: Distributed processing with Structured Streaming
- **Apache Kafka**: Message streaming for real-time data flow
- **MinIO**: S3-compatible object storage for data lake
- **Parquet**: Columnar storage format for analytics optimization

**Storage Paths**:
- **Bronze Zone**: `s3a://bronze/iot/`, `s3a://bronze/weather/`, `s3a://bronze/satellite/`
- **Silver Zone**: `s3a://silver/iot/`, `s3a://silver/weather/`, `s3a://silver/satellite/` 
- **Gold Zone**: `s3a://gold/ml_feature/` 
- **Images**: `s3a://satellite-images/` 

