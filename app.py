#!/usr/bin/env python3
import os
import signal
import sys
import time
import threading
from config import load_config
from utils.logging import get_logger
from services.db_service import PostgresClient
from services.kafka_service import KafkaConsumer
from services.healthcheck import HealthCheckServer
from consumer import TraceConsumer


def main():
    """Main entry point for the trace consumer service"""
    # Initialize logger
    logger = get_logger()
    logger.info("Starting trace consumer service...")

    # Load configuration
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.fatal("Failed to load configuration", e)

    # Start health check server
    try:
        server_port = int(config.server_port)
        health_server = HealthCheckServer(server_port, logger)
        health_server.start()
        # Initially set to not ready
        health_server.set_ready(False)
        health_server.set_db_health(False)
        health_server.set_kafka_health(False)
        logger.info(f"Health check server started on port {server_port}")
    except Exception as e:
        logger.error(f"Failed to start health check server: {str(e)}", e)
        # Continue without health checks - won't affect core functionality

    # Initialize PostgreSQL client
    try:
        db_client = PostgresClient(
            host=config.db_host,
            port=config.db_port,
            dbname=config.db_name,
            user=config.db_user,
            password=config.db_password,
            logger=logger,
            schema=config.db_schema,
        )

        # Test database connection
        if db_client.test_connection():
            logger.info("Database connection successful")
            health_server.set_db_health(True)
        else:
            logger.error("Database connection test failed")
            health_server.set_db_health(False)
    except Exception as e:
        logger.fatal("Failed to connect to database", e)

    # Get already processed trace IDs from the database
    try:
        processed_trace_ids = db_client.get_processed_trace_ids()
        logger.info(f"Loaded {len(processed_trace_ids)} previously processed trace IDs")
    except Exception as e:
        logger.error("Failed to load processed trace IDs", e)
        processed_trace_ids = []

    # Initialize consumer
    try:
        trace_consumer = TraceConsumer(
            db_client=db_client,
            logger=logger,
            health_check_interval=config.health_check_interval,
        )
        logger.info("Trace consumer initialized successfully")
    except Exception as e:
        logger.fatal("Failed to initialize trace consumer", e)

    # Start health check thread
    try:
        trace_consumer.start_health_check_thread(health_server)
    except Exception as e:
        logger.error(f"Failed to start health check thread: {str(e)}", e)

    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            config.kafka_brokers,
            config.kafka_topic,
            config.consumer_group,
            config.kafka_username,
            config.kafka_password,
            config.kafka_auth,
            trace_consumer.process_message,
            logger,
            config.max_retries,
            config.retry_backoff_ms,
            processed_trace_ids,
        )
        logger.info("Kafka consumer initialized successfully")
        health_server.set_kafka_health(True)
    except Exception as e:
        logger.fatal("Failed to initialize Kafka consumer", e)

    # Setup signal handling for graceful shutdown
    shutdown_event = threading.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start consuming messages in a separate thread
    consumer_thread = threading.Thread(target=consumer.consume)
    consumer_thread.daemon = True
    consumer_thread.start()
    logger.info("Kafka consumer started")

    # Now that everything is initialized, mark as ready for K8s probes
    try:
        health_server.set_ready(True)
    except Exception:
        pass

    # Wait for shutdown signal
    shutdown_event.wait()

    # Graceful shutdown
    logger.info("Starting graceful shutdown...")

    try:
        # Stop health check thread
        trace_consumer.stop_health_check_thread()

        # Close the consumer
        logger.info("Closing Kafka consumer...")
        consumer.close()

        # Wait for consumer thread to finish
        consumer_thread.join(timeout=30)

        # Close database connection
        logger.info("Closing database connection...")
        db_client.close()

        # Stop health check server
        try:
            logger.info("Stopping health check server...")
            health_server.stop()
        except Exception as e:
            logger.error(f"Error stopping health check server: {str(e)}", e)

        logger.info("Shutdown complete")
    except Exception as e:
        logger.error("Error during shutdown", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
