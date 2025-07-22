#!/usr/bin/env python3
"""
Redis Cache Service - Main Entry Point
High-performance Kafka to Redis streaming cache service for crop disease monitoring system
"""

import os
import sys
import time
import logging
from datetime import datetime
from loguru import logger

from config import get_service_config
from redis_stream_processor import RedisStreamProcessor


def setup_logging():
    """Configure loguru logging with proper formatting and levels"""
    service_config = get_service_config()
    
    # Remove default logger
    logger.remove()
    
    # Console logging with colors
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        level=service_config.log_level,
        colorize=True,
        backtrace=True,
        diagnose=True
    )
    
    # File logging (optional, for production)
    log_file = os.getenv("LOG_FILE")
    if log_file:
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
            level=service_config.log_level,
            rotation="100 MB",
            retention="7 days",
            compression="gzip",
            backtrace=True,
            diagnose=True
        )
    
    # Suppress noisy third-party loggers
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("kafka.conn").setLevel(logging.WARNING)
    logging.getLogger("kafka.coordinator.consumer").setLevel(logging.WARNING)
    logging.getLogger("kafka.cluster").setLevel(logging.WARNING)
    
    logger.info("🔧 Logging configured successfully")


def print_startup_banner():
    """Print service startup banner"""
    banner = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                         🌾 CROP DISEASE MONITORING SYSTEM                    ║
║                              Redis Cache Service                              ║
║                                                                              ║
║  📡 High-performance Kafka → Redis streaming cache                          ║
║  🚀 Real-time data processing for dashboard acceleration                    ║
║  ⚡ Sub-millisecond response times for sensor & weather data               ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_system_info():
    """Print system and configuration information"""
    service_config = get_service_config()
    
    logger.info("🌾 Redis Cache Service starting up...")
    logger.info(f"📋 Service Name: {service_config.service_name}")
    logger.info(f"🕐 Timezone: {service_config.timezone}")
    logger.info(f"📊 Log Level: {service_config.log_level}")
    logger.info(f"⚙️ Batch Size: {service_config.batch_size}")
    logger.info(f"⏱️ Processing Interval: {service_config.processing_interval}s")
    logger.info(f"🔍 Health Check Interval: {service_config.health_check_interval}s")
    
    # Environment info
    logger.info(f"🐍 Python: {sys.version}")
    logger.info(f"🖥️ Platform: {sys.platform}")
    logger.info(f"📁 Working Directory: {os.getcwd()}")
    
    # Service configuration
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = os.getenv("REDIS_PORT", "6379")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    logger.info(f"🔴 Redis: {redis_host}:{redis_port}")
    logger.info(f"📨 Kafka: {kafka_servers}")


def wait_for_dependencies():
    """Wait for Redis and Kafka to be available before starting"""
    logger.info("⏳ Checking dependencies...")
    
    # Import here to avoid circular imports
    from redis_client import RedisClient
    from kafka import KafkaProducer
    
    max_wait_time = 120  # 2 minutes
    check_interval = 10  # 10 seconds
    start_time = time.time()
    
    # Check Redis
    logger.info("🔴 Checking Redis connectivity...")
    redis_ready = False
    while not redis_ready and (time.time() - start_time) < max_wait_time:
        try:
            redis_client = RedisClient()
            if redis_client.connect():
                redis_ready = True
                logger.success("✅ Redis is ready!")
                redis_client.disconnect()
            else:
                raise Exception("Redis connection failed")
        except Exception as e:
            logger.warning(f"⏳ Redis not ready: {e}")
            time.sleep(check_interval)
    
    if not redis_ready:
        logger.error("❌ Redis is not available after waiting. Exiting...")
        sys.exit(1)
    
    # Check Kafka
    logger.info("📨 Checking Kafka connectivity...")
    kafka_ready = False
    while not kafka_ready and (time.time() - start_time) < max_wait_time:
        try:
            kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
            producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                request_timeout_ms=5000,
                api_version=(0, 10, 1)
            )
            # Test connection by getting metadata
            metadata = producer.partitions_for("test-topic")  # This will trigger connection
            kafka_ready = True
            producer.close()
            logger.success("✅ Kafka is ready!")
        except Exception as e:
            logger.warning(f"⏳ Kafka not ready: {e}")
            time.sleep(check_interval)
    
    if not kafka_ready:
        logger.error("❌ Kafka is not available after waiting. Exiting...")
        sys.exit(1)
    
    total_wait_time = time.time() - start_time
    logger.success(f"🎯 All dependencies ready after {total_wait_time:.1f} seconds!")


def main():
    """Main service entry point"""
    try:
        # Setup logging first
        setup_logging()
        
        # Print startup information
        print_startup_banner()
        print_system_info()
        
        # Wait for dependencies
        wait_for_dependencies()
        
        # Initialize and start the stream processor
        logger.info("🚀 Initializing Redis Stream Processor...")
        processor = RedisStreamProcessor()
        
        # Start processing
        logger.info("🎯 Starting stream processing...")
        success = processor.start()
        
        if not success:
            logger.error("❌ Failed to start stream processor")
            sys.exit(1)
        
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
    except Exception as e:
        logger.error(f"❌ Fatal error in main service: {e}")
        sys.exit(1)
    finally:
        logger.info("👋 Redis Cache Service shutdown complete")


if __name__ == "__main__":
    main() 