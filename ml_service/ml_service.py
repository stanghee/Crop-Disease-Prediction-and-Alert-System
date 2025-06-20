import os
import logging
import time
import schedule
import socket
from model_trainer import SensorAnomalyTrainer
from anomaly_detector import RealtimeAnomalyDetector
from datetime import datetime

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class MLService:
    def __init__(self):
        self.trainer = SensorAnomalyTrainer()
        self.detector = RealtimeAnomalyDetector()
        self.model_trained = False
    
    def wait_for_services(self):
        """Aspetta che PostgreSQL e Kafka siano pronti"""
        logger.info("🚀 Starting ML Service for Anomaly Detection...")
        
        # Attendi PostgreSQL
        postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        logger.info("⏳ Waiting for PostgreSQL...")
        while True:
            try:
                socket.create_connection((postgres_host, 5432), timeout=5)
                logger.info("✅ PostgreSQL is ready!")
                break
            except OSError:
                logger.info("PostgreSQL is unavailable - sleeping")
                time.sleep(2)
        
        # Attendi Kafka
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        kafka_host, kafka_port = kafka_servers.split(":")
        logger.info("⏳ Waiting for Kafka...")
        while True:
            try:
                socket.create_connection((kafka_host, int(kafka_port)), timeout=5)
                logger.info("✅ Kafka is ready!")
                break
            except OSError:
                logger.info("Kafka is unavailable - sleeping")
                time.sleep(2)
        
    def initial_training(self):
        """Esegue il training iniziale del modello"""
        logger.info("🎯 Avvio training iniziale del modello...")
        
        # Aspetta che ci siano dati sufficienti
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                success = self.trainer.run_training()
                if success:
                    self.model_trained = True
                    logger.info("✅ Training iniziale completato con successo!")
                    return True
                else:
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"⚠️ Training fallito, tentativo {retry_count}/{max_retries}. Riprovo tra 60 secondi...")
                        time.sleep(60)
                    else:
                        logger.error("❌ Training iniziale fallito dopo tutti i tentativi")
                        return False
                        
            except Exception as e:
                logger.error(f"❌ Errore durante training: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(60)
                else:
                    return False
        
        return False
    
    def scheduled_retraining(self):
        """Retraining periodico del modello"""
        logger.info("🔄 Avvio retraining programmato...")
        try:
            success = self.trainer.run_training()
            if success:
                logger.info("✅ Retraining completato con successo!")
                # Ricarica il modello nel detector
                self.detector.load_model()
            else:
                logger.error("❌ Retraining fallito")
        except Exception as e:
            logger.error(f"❌ Errore durante retraining: {e}")
    
    def run_detection(self):
        """Avvia il detector in tempo reale"""
        if not self.model_trained:
            logger.error("❌ Modello non trainato. Impossibile avviare detection.")
            return False
        
        logger.info("🚀 Avvio anomaly detection in tempo reale...")
        self.detector.run()
        return True
    
    def setup_scheduled_retraining(self):
        """Configura il retraining automatico"""
        # Retraining ogni giorno alle 02:00
        schedule.every().day.at("02:00").do(self.scheduled_retraining)
        logger.info("⏰ Retraining programmato ogni giorno alle 02:00")
    
    def run_service(self):
        """Avvia il servizio ML completo"""
        try:
            # 1. Aspetta che l'infrastruttura sia pronta
            self.wait_for_services()
            
            # 2. Training iniziale
            if not self.initial_training():
                logger.error("💥 Impossibile completare il training iniziale. Terminating.")
                return False
            
            # 3. Setup retraining programmato
            self.setup_scheduled_retraining()
            
            # 4. Avvia detection in tempo reale in un thread separato
            import threading
            def run_scheduled_tasks():
                while True:
                    schedule.run_pending()
                    time.sleep(60)  # Controlla ogni minuto
            
            # Avvia scheduler in background
            scheduler_thread = threading.Thread(target=run_scheduled_tasks, daemon=True)
            scheduler_thread.start()
            logger.info("⏰ Scheduler avviato in background")
            
            # 5. Avvia detection (processo principale)
            self.run_detection()
            
        except KeyboardInterrupt:
            logger.info("🛑 Servizio fermato dall'utente")
        except Exception as e:
            logger.error(f"❌ Errore fatale nel servizio: {e}")
        finally:
            logger.info("🔌 ML Service terminato")

def main():
    service = MLService()
    service.run_service()

if __name__ == "__main__":
    main() 