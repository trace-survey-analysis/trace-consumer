from typing import List, Dict, Any, Optional
import threading
import time

from services.db_service import PostgresClient
from services.kafka_service import KafkaConsumer
from models.data_models import TraceProcessedMessage
from utils.logging import Logger


class TraceConsumer:
    """
    Main consumer class that processes trace survey data from Kafka
    and stores it in the PostgreSQL database
    """

    def __init__(
        self, db_client: PostgresClient, logger: Logger, health_check_interval: int = 60
    ):
        """Initialize the trace consumer"""
        self.db_client = db_client
        self.logger = logger
        self.health_check_interval = health_check_interval
        self.health_check_thread = None
        self.running = False

    def process_message(self, message: TraceProcessedMessage) -> bool:
        """
        Process a trace processed message and store it in the database

        Args:
            message: The message containing processed trace data

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info("Processing trace message", "traceId", message.traceId)

        try:
            # Save the message to database
            success = self.db_client.save_processed_message(message)

            if success:
                self.logger.info(
                    "Successfully processed trace", "traceId", message.traceId
                )
            else:
                self.logger.error(
                    "Failed to save trace to database", None, "traceId", message.traceId
                )

            return success
        except Exception as e:
            self.logger.error(
                f"Error processing trace message: {str(e)}",
                e,
                "traceId",
                message.traceId,
            )
            return False

    def start_health_check_thread(self, health_server):
        """Start a thread to periodically check health of external services"""
        self.running = True

        def health_check_worker():
            while self.running:
                try:
                    # Check database health
                    db_healthy = self.db_client.test_connection()
                    health_server.set_db_health(db_healthy)

                    # Sleep for interval
                    time.sleep(self.health_check_interval)
                except Exception as e:
                    self.logger.error(f"Health check error: {str(e)}", e)
                    # Set unhealthy
                    health_server.set_db_health(False)
                    # Still sleep to avoid tight loop
                    time.sleep(max(5, self.health_check_interval // 2))

        self.health_check_thread = threading.Thread(target=health_check_worker)
        self.health_check_thread.daemon = True
        self.health_check_thread.start()
        self.logger.info(
            f"Health check thread started, interval: {self.health_check_interval}s"
        )

    def stop_health_check_thread(self):
        """Stop the health check thread"""
        self.running = False
        if self.health_check_thread:
            self.health_check_thread.join(timeout=5)
            self.logger.info("Health check thread stopped")
