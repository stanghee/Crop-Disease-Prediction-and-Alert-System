# Pipeline Big Data a 3 Zone per Monitoraggio Agricolo

## Architettura Generale

La pipeline implementa una struttura **Data Lake a 3 zone** (Bronze, Silver, Gold) seguendo la metodologia **Delta Lake** per garantire ACID transactions, schema evolution e time travel capabilities.

```
Kafka Topics → Bronze Zone → Silver Zone → Gold Zone → ML Models & Dashboard
     ↓              ↓             ↓            ↓
Raw Data    Immutable Raw   Validated &   ML Features &
            + Metadata      Cleaned       Aggregations
```

## Struttura delle Zone

### 🥉 BRONZE ZONE (Raw Data Lake)
**Principio**: Dati immutabili il più vicino possibile alla forma originale

### 🥈 SILVER ZONE (Curated Data Lake) 
**Principio**: Dati validati, puliti e con schema consistente

### 🥇 GOLD ZONE (Business-Ready Data Lake)
**Principio**: Feature pronte per ML e KPI aggregati per dashboard

---

## 1. DATI IoT SENSORI

### 🥉 BRONZE ZONE - Sensori IoT

**Fonte**: Kafka topic `sensor_data`

**Struttura Dati Originali**:
```json
{
  "timestamp": "2024-01-15T14:30:00+01:00",
  "field_id": "field_01",
  "temperature": 24.5,
  "humidity": 65.2,
  "soil_ph": 6.8
}
```

**Trasformazioni Bronze**:
- ✅ Aggiunta timestamp di ingestione
- ✅ Aggiunta metadati Kafka (offset, partition)
- ✅ Aggiunta source_type per tracciabilità
- ❌ NESSUNA validazione o pulizia

**Formato di Salvataggio**: JSON
**Percorso**: `s3a://bronze/sensor_data/`
**Partitioning**: Per `source_type`
**Schema**:
```python
StructType([
    StructField("timestamp", StringType(), True),           # Original timestamp
    StructField("field_id", StringType(), True),           # Field identifier
    StructField("temperature", DoubleType(), True),        # Temperature in °C
    StructField("humidity", DoubleType(), True),           # Humidity %
    StructField("soil_ph", DoubleType(), True),           # Soil pH
    StructField("kafka_timestamp", TimestampType(), True), # Kafka ingestion time
    StructField("ingestion_timestamp", TimestampType(), True), # Spark ingestion time
    StructField("source_type", StringType(), True)        # Always "sensor"
])
```

### 🥈 SILVER ZONE - Sensori IoT

**Trasformazioni Silver**:
- ✅ Validazione range valori (temp: -10°C to 50°C, humidity: 0-100%, pH: 3-9)
- ✅ Parsing timestamp con timezone handling
- ✅ Rilevamento anomalie per sensor (flag booleani)
- ✅ Aggiunta colonne derivate (date, hour)
- ✅ Rimozione record con valori null critici

**Formato di Salvataggio**: Parquet (compresso, schema evolution)
**Percorso**: `s3a://silver/sensor_data/`
**Partitioning**: Per `date` e `field_id`
**Schema Aggiuntivo**:
```python
# Schema originale +
StructField("timestamp_parsed", TimestampType(), True),
StructField("date", DateType(), True),
StructField("hour", IntegerType(), True),
StructField("temperature_anomaly", BooleanType(), True),
StructField("humidity_anomaly", BooleanType(), True),
StructField("ph_anomaly", BooleanType(), True),
StructField("has_anomaly", BooleanType(), True)
```

### 🥇 GOLD ZONE - Sensori IoT

**Trasformazioni Gold**:

#### A) ML Features Table
**Aggregazioni giornaliere per field**:
```python
- avg_temperature, std_temperature, min_temperature, max_temperature
- avg_humidity, std_humidity  
- avg_soil_ph, std_soil_ph
- anomaly_count, total_readings, anomaly_rate
- risk_score (HIGH/MEDIUM/LOW basato su anomaly_rate)
```

#### B) Dashboard KPIs Table
**Metriche real-time (ultimi 7 giorni)**:
```python
- current_avg_temp, current_avg_humidity, current_avg_ph
- recent_anomalies (count)
```

**Formato di Salvataggio**: Delta Lake (ACID, versioning)
**Percorsi**: 
- `s3a://gold/sensor_ml_features/`
- `s3a://gold/sensor_dashboard_kpis/`

**Disponibilità**:
- **ML Models**: Lettura diretta da Delta tables per training
- **Dashboard**: Query SQL su Delta tables via Spark SQL

---

## 2. DATI METEO

### 🥉 BRONZE ZONE - Meteo

**Fonte**: Kafka topic `weather_data` (WeatherAPI)

**Struttura Dati Originali**:
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

**Trasformazioni Bronze**:
- ✅ Conservazione completa risposta API
- ✅ Aggiunta metadati ingestione
- ❌ NESSUNA validazione

**Formato**: JSON
**Percorso**: `s3a://bronze/weather_data/`
**Partitioning**: Per `source_type`

### 🥈 SILVER ZONE - Meteo

**Trasformazioni Silver**:
- ✅ Validazione range temperatura (-50°C to 60°C)
- ✅ Validazione humidity (0-100%)
- ✅ Validazione coordinate geografiche
- ✅ Parsing timestamp e timezone
- ✅ Flagging dati validi/invalidi

**Formato**: Parquet
**Percorso**: `s3a://silver/weather_data/`
**Partitioning**: Per `date` e `location`

### 🥇 GOLD ZONE - Meteo

**Trasformazioni Gold**:

#### A) Weather ML Features
**Aggregazioni giornaliere per location**:
```python
- avg_temperature, max_temperature, min_temperature
- avg_humidity, avg_wind_speed, avg_uv_index
- dominant_condition (most frequent)
```

**Formato**: Delta Lake
**Percorso**: `s3a://gold/weather_ml_features/`

**Disponibilità**:
- **ML Models**: Join con sensor features per correlazioni meteo-sensori
- **Dashboard**: Visualizzazione trend meteo per area

---

## 3. IMMAGINI SATELLITARI

### 🥉 BRONZE ZONE - Satellitari

**Fonte**: Kafka topic `satellite_data` (Copernicus Sentinel-2)

**Struttura Dati Originali**:
```json
{
  "timestamp": "2024-01-15T14:30:00Z",
  "image_base64": "iVBORw0KGgoAAAANSUhEUgAA...",
  "location": {
    "bbox": [10.894444, 45.266667, 10.909444, 45.281667]
  }
}
```

**Trasformazioni Bronze**:
- ✅ Estrazione immagine da base64
- ✅ Salvataggio immagine su MinIO bucket separato
- ✅ Creazione metadata record con path immagine
- ✅ Estrazione coordinate bounding box

**Formato Immagini**: PNG su MinIO bucket `satellite-images`
**Formato Metadata**: JSON
**Percorso**: `s3a://bronze/satellite_data/`
**Schema Metadata**:
```python
{
  "timestamp": "2024-01-15T14:30:00Z",
  "image_path": "s3a://satellite-images/satellite_2024-01-15T14-30-00Z.png",
  "bbox_min_lon": 10.894444,
  "bbox_min_lat": 45.266667,
  "bbox_max_lon": 10.909444,
  "bbox_max_lat": 45.281667,
  "ingestion_timestamp": "2024-01-15T14:31:15+01:00",
  "source_type": "satellite"
}
```

### 🥈 SILVER ZONE - Satellitari

**Trasformazioni Silver**:
- ✅ Validazione metadata immagine
- ✅ Controllo integrità file immagine
- ✅ Validazione coordinate geografiche
- ✅ Calcolo area coperta (km²)
- ✅ Assessment qualità immagine (cloud coverage, resolution)

**Formato**: Parquet (metadata) + PNG (immagini validate)
**Percorso**: `s3a://silver/satellite_data/`
**Partitioning**: Per `date`

### 🥇 GOLD ZONE - Satellitari

**Trasformazioni Gold**:

#### A) Vegetation Indices
```python
- NDVI (Normalized Difference Vegetation Index)
- EVI (Enhanced Vegetation Index)  
- SAVI (Soil-Adjusted Vegetation Index)
```

#### B) Crop Health Indicators
```python
- vegetation_health_score (0-100)
- stress_indicators (drought, disease, pest)
- change_detection (comparison with previous images)
```

#### C) Geospatial Features
```python
- field_coverage_percentage
- vegetation_density_map
- anomaly_regions (coordinate clusters)
```

**Formato**: Delta Lake (metadata + indices) + GeoTIFF (processed images)
**Percorsi**:
- `s3a://gold/satellite_vegetation_indices/`
- `s3a://gold/satellite_health_indicators/`

**Disponibilità**:
- **ML Models**: Features di vegetazione per correlazione con sensori
- **Dashboard**: Mappe di calore, zone a rischio, trend temporali

---

## 4. FEATURE INTEGRATE (Cross-Source)

### 🥇 GOLD ZONE - Integrated Features

**Combinazione delle 3 fonti dati**:

```python
integrated_features = sensor_gold 
    .join(weather_gold, on="date") 
    .join(satellite_gold, on=["date", "field_coordinates"])
```

**Feature Derivate**:
```python
- temp_differential (sensor vs weather)
- humidity_differential (sensor vs weather)
- vegetation_sensor_correlation (NDVI vs soil conditions)
- weather_stress_factor (combination of weather extremes)
- multi_source_risk_score (weighted combination)
```

**Formato**: Delta Lake
**Percorso**: `s3a://gold/integrated_ml_features/`

---

## 5. PIPELINE PROCESSING

### Streaming Processing (Bronze Zone)
- **Frequenza**: Real-time (30 seconds micro-batches)
- **Tecnologia**: Spark Structured Streaming
- **Checkpointing**: Fault-tolerance con recovery automatico

### Batch Processing (Silver + Gold Zone)  
- **Frequenza**: Ogni 5 minuti
- **Tecnologia**: Spark Batch Jobs
- **Orchestrazione**: Schedulazione interna con retry logic

### Schema Evolution
- **Bronze**: Schema flessibile (JSON)
- **Silver**: Schema validato con evolution support
- **Gold**: Schema ottimizzato per query performance

---

## 6. ACCESSO AI DATI

### Per Modelli ML
```python
# Spark SQL
spark.sql("SELECT * FROM delta.`s3a://gold/integrated_ml_features/`")

# Direct Delta table access
from delta.tables import DeltaTable
ml_features = DeltaTable.forPath(spark, "s3a://gold/integrated_ml_features/")
```

### Per Dashboard
```python
# Real-time KPIs
dashboard_data = spark.read.format("delta").load("s3a://gold/sensor_dashboard_kpis/")

# Time-series analysis
historical_data = spark.read.format("delta").load("s3a://gold/weather_ml_features/") \
    .filter(col("date") >= "2024-01-01")
```

### Query Performance
- **Partitioning**: Ottimizzato per query temporali e geografiche
- **Indexing**: Z-ordering su Delta tables per query multi-dimensionali
- **Caching**: Spark cache per tabelle frequentemente accedute

---

## 7. MONITORING E QUALITÀ DATI

### Data Quality Checks
- **Bronze**: Monitoring volume dati e lag
- **Silver**: Validation rules e data profiling
- **Gold**: Business rules validation e anomaly detection

### Metrics
- **Throughput**: Records/second per source
- **Latency**: End-to-end processing time
- **Quality**: % records passed validation
- **Availability**: Uptime servizi e data freshness

---

## 8. SICUREZZA E GOVERNANCE

### Access Control
- **Bronze**: Read-only per data engineers
- **Silver**: Read-write per data scientists  
- **Gold**: Read access per ML models e dashboard

### Data Lineage
- **Tracking**: Spark history server + Delta Lake transaction logs
- **Auditing**: Full traceability da source a consumption

### Backup & Recovery
- **Versioning**: Delta Lake time travel capabilities
- **Disaster Recovery**: MinIO replication + Spark checkpointing

Questa architettura garantisce **scalabilità**, **affidabilità** e **performance** per il sistema di monitoraggio agricolo, supportando sia analisi real-time che batch processing per la prevenzione delle malattie delle colture. 