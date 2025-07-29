# Satellite Service

This component is a satellite image acquisition service designed to fetch high-resolution RGB images from Copernicus Sentinel-2 and stream them to the Kafka messaging system.

## Overview

The Satellite Service operates as a critical data source acquiring satellite images every minute (to simulate images drone frequency) from Copernicus Sentinel-2 through OAuth2 authentication, this service streams base64-encoded images to the `satellite_data` Kafka topic, which then flows through our Spark processing pipeline to be stored in the MinIO object storage database.

## Components

- **producer.py**: Main satellite image acquisition and streaming logic
- **entrypoint.sh**: Service startup with Kafka dependency check
- **wait_for_kafka.py**: Ensures Kafka availability before startup


## Key Features

- **Real-time Image Simulation**: Fetches satellite images from Copernicus Sentinel-2 every minute
- **OAuth2 Authentication**: Secure authentication with Copernicus Data Space Ecosystem
- **RGB Image Processing**: Generates true-color RGB images using B04, B03, B02 bands
- **Geographic Configuration**: Configurable bounding box for area of interest (default: Verona)
- **Cloud Coverage Filtering**: Filters images with maximum 10% cloud coverage
- **Base64 Encoding**: Converts images to base64 for Kafka streaming
- **Timezone Awareness**: Proper timestamp handling for accurate temporal analysis

### Data Flow

![Satellite Service Data Flow](services/Images/satellite_data_flow.png)

## Data Schema

The service produces satellite image data with the following schema:

```json
{
  "timestamp": "ISO-8601",
  "image_base64": "base64-encoded-png-image",
  "location": {
    "bbox": [min_lon, min_lat, max_lon, max_lat]
  }
}
```

## Geographic Configuration

The service is configured for the Verona agricultural area:

| Parameter | Value | Description |
|-----------|-------|-------------|
| **BBOX_MIN_LON** | 10.894444 | Minimum longitude |
| **BBOX_MIN_LAT** | 45.266667 | Minimum latitude |
| **BBOX_MAX_LON** | 10.909444 | Maximum longitude |
| **BBOX_MAX_LAT** | 45.281667 | Maximum latitude |
| **Image Resolution** | 512x512 pixels | Output image dimensions |
| **Cloud Coverage** | ≤10% | Maximum allowed cloud coverage |

## Image Processing

The service uses a custom evaluation script for RGB image generation:

- **Input Bands**: B04 (Red), B03 (Green), B02 (Blue)
- **Processing**: Clamped RGB values with 3x multiplication factor
- **Output Format**: PNG image with 512x512 resolution
- **Color Enhancement**: Optimized for agricultural monitoring





 
