import base64
import json
import time
import schedule
import os
from datetime import datetime
from io import BytesIO
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from kafka import KafkaProducer
from PIL import Image
from zoneinfo import ZoneInfo

# === CONFIG ===
client_id = os.getenv('COPERNICUS_CLIENT_ID')
client_secret = os.getenv('COPERNICUS_CLIENT_SECRET')

# Check if the client id and client secret are set
if not client_id or not client_secret:
    raise ValueError("COPERNICUS_CLIENT_ID and COPERNICUS_CLIENT_SECRET must be set in environment variables")

kafka_topic = 'satellite_data'
kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Area of interest configuration (default: Verona area)
bbox = [
    float(os.getenv('BBOX_MIN_LON', '10.894444')),
    float(os.getenv('BBOX_MIN_LAT', '45.266667')),
    float(os.getenv('BBOX_MAX_LON', '10.909444')),
    float(os.getenv('BBOX_MAX_LAT', '45.281667'))
]

# === AUTENTICAZIONE ===
def get_oauth_session():
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(
        token_url='https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token',
        client_secret=client_secret,
        include_client_id=True
    )
    return oauth

# === EVALSCRIPT RGB ===
evalscript_rgb = """
//VERSION=3
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}
function setup() {
  return {
    input: ["B04", "B03", "B02"],
    output: { bands: 3 }
  };
}
function evaluatePixel(sample) {
  return [
    clamp(sample.B04 * 3, 0, 1),
    clamp(sample.B03 * 3, 0, 1),
    clamp(sample.B02 * 3, 0, 1)
  ];
}
"""

# === FUNZIONE PRINCIPALE ===
def fetch_and_send():
    rome_tz = ZoneInfo("Europe/Rome")
    current_time = datetime.now(rome_tz)
    print("📡 Acquisizione immagine:", current_time, flush=True)
    oauth = get_oauth_session()

    request = {
        "input": {
            "bounds": {
                "bbox": bbox,
                "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}
            },
            "data": [{
                "type": "sentinel-2-l2a",
                "dataFilter": {
                    "timeRange": {
                        "from": "2023-07-01T00:00:00Z",
                        "to": "2025-04-10T23:59:59Z"
                    },
                    "maxCloudCoverage": 10
                }
            }]
        },
        "output": {
            "width": 512,
            "height": 512,
            "responses": [{
                "identifier": "default",
                "format": {"type": "image/png"}
            }]
        },
        "evalscript": evalscript_rgb
    }

    process_url = "https://sh.dataspace.copernicus.eu/api/v1/process"
    response_rgb = oauth.post(process_url, json=request)

    if response_rgb.status_code != 200:
        print(f"❌ Error immagine RGB {response_rgb.status_code}", flush=True)
        print(response_rgb.text)
        return

    # Converti immagine RGB in base64
    img_rgb = Image.open(BytesIO(response_rgb.content))
    buffered = BytesIO()
    img_rgb.save(buffered, format="PNG")
    img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")

    # Message to send
    message = {
        "timestamp": current_time.isoformat(),
        "image_base64": img_base64,
        "location": {
            "bbox": bbox
        }
    }

    # Send to Kafka
    producer = KafkaProducer(
        bootstrap_servers=[kafka_server],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(kafka_topic, message)
    producer.flush()

    print("✅ Image sent to Kafka.")

# === EXECUTE IMMEDIATELY ===
fetch_and_send()

# === OGNI MINUTO === 
# Because we are supposed to use drone to get the image, but it is costly so we use satellite
schedule.every(1).minutes.do(fetch_and_send)

print("⏱ In ascolto ogni minuto...", flush=True)
while True:
    schedule.run_pending()
    time.sleep(1)