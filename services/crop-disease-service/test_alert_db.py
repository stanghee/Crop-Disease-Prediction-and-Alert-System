from database.alert_repository import AlertRepository
from datetime import datetime

# Crea un alert di esempio
alert = {
    "zone_id": "test_field_001",
    "alert_timestamp": datetime.now().isoformat(),
    "alert_type": "SENSOR_ANOMALY",
    "severity": "HIGH",
    "message": "Test alert manuale",
    "status": "ACTIVE"
}

repo = AlertRepository()
success = repo.save_single_alert(alert)

if success:
    print("✅ Alert scritto correttamente nel database!")
else:
    print("❌ Errore nel salvataggio dell'alert.") 