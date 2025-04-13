import json
import time
from typing import Callable, Dict, Any, Optional, List
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException
from models.data_models import TraceProcessedMessage
from utils.logging import Logger


class KafkaConsumer:
    """Kafka consumer for receiving trace processed messages"""

    def __init__(
        self,
        brokers: list,
        topic: str,
        group_id: str,
        username: str,
        password: str,
        enable_auth: bool,
        handler: Callable,
        logger: Logger,
        max_retries: int = 3,
        retry_backoff_ms: int = 1000,
        processed_trace_ids: List[str] = None,
    ):
        """Initialize the Kafka consumer"""
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.username = username
        self.password = password
        self.enable_auth = enable_auth
        self.handler = handler
        self.logger = logger
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.consumer = self._create_consumer()
        self.running = False

        # Set of trace IDs that have already been processed
        # This helps avoid duplicate processing if the consumer restarts
        self.processed_trace_ids = set(processed_trace_ids or [])

    def _create_consumer(self) -> Consumer:
        """Create a Kafka consumer with the appropriate configuration"""
        config = {
            "bootstrap.servers": ",".join(self.brokers),
            "auto.offset.reset": "latest" if not self.group_id else "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 180000,  # 3 minutes
            "max.poll.interval.ms": 300000,  # 5 minutes
        }

        if self.group_id:
            config["group.id"] = self.group_id

        if self.enable_auth:
            if not self.username or not self.password:
                raise ValueError("Kafka authentication enabled but missing credentials")

            self.logger.info(
                "Setting up SASL PLAIN authentication", "username", self.username
            )

            config.update(
                {
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": self.username,
                    "sasl.password": self.password,
                }
            )

        self.logger.info(
            "Creating Kafka consumer",
            "brokers",
            self.brokers,
            "topic",
            self.topic,
            "groupID",
            self.group_id,
        )

        consumer = Consumer(config)
        consumer.subscribe([self.topic])

        return consumer

    def _reconnect(self) -> None:
        """Attempt to reconnect the consumer"""
        self.logger.info("Beginning reconnection process...")

        if self.consumer:
            self.logger.info("Closing existing consumer")
            self.consumer.close()

        self.logger.info("Creating new consumer connection...")
        self.consumer = self._create_consumer()
        self.logger.info("Successfully reconnected to Kafka with new consumer")

    def add_processed_trace_id(self, trace_id: str) -> None:
        """Add a trace ID to the set of processed trace IDs"""
        self.processed_trace_ids.add(trace_id)

    def is_trace_processed(self, trace_id: str) -> bool:
        """Check if a trace ID has already been processed"""
        return trace_id in self.processed_trace_ids

    def consume(self) -> None:
        """Consume messages from Kafka"""
        self.logger.info(
            "Starting to consume messages",
            "topic",
            self.topic,
            "groupID",
            self.group_id,
            "brokers",
            self.brokers,
        )

        reconnect_attempts = 0
        max_reconnect_attempts = 15
        reconnect_backoff = 0.5  # seconds

        self.running = True

        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.logger.info(f"Reached end of partition {msg.partition()}")
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")

                        # Attempt reconnect
                        if reconnect_attempts < max_reconnect_attempts:
                            reconnect_attempts += 1
                            time.sleep(reconnect_backoff)
                            reconnect_backoff *= 2
                            if reconnect_backoff > 10:
                                reconnect_backoff = 10

                            self._reconnect()
                        else:
                            self.logger.error(
                                "Maximum reconnect attempts reached, will retry in 30 seconds"
                            )
                            time.sleep(30)
                            reconnect_attempts = 0
                            reconnect_backoff = 0.5

                        continue

                # Reset reconnect attempts on successful message
                reconnect_attempts = 0
                reconnect_backoff = 0.5

                # Process message
                value = msg.value()
                self.logger.info(
                    "Received message",
                    "topic",
                    msg.topic(),
                    "partition",
                    msg.partition(),
                    "offset",
                    msg.offset(),
                    "length",
                    len(value) if value else 0,
                )

                if value:
                    try:
                        # Parse message
                        message_dict = json.loads(value)

                        # Parse ISO format dates
                        self._parse_dates_in_dict(message_dict)

                        # Create TraceProcessedMessage from dict
                        trace_msg = TraceProcessedMessage(**message_dict)

                        # Check if this trace has already been processed
                        if self.is_trace_processed(trace_msg.traceId):
                            self.logger.info(
                                f"Trace {trace_msg.traceId} already processed, skipping"
                            )
                            # Still commit the message to avoid getting it again
                            self.consumer.commit(message=msg, asynchronous=False)
                            continue

                        # Process message with retries
                        process_error = None
                        for retry in range(self.max_retries + 1):
                            if retry > 0:
                                sleep_time = self.retry_backoff_ms / 1000.0
                                self.logger.info(
                                    f"Retrying message processing in {sleep_time}s",
                                    "attempt",
                                    retry,
                                    "traceId",
                                    trace_msg.traceId,
                                )
                                time.sleep(sleep_time)

                            try:
                                success = self.handler(trace_msg)
                                if success:
                                    # Add to processed set
                                    self.add_processed_trace_id(trace_msg.traceId)
                                    process_error = None
                                    break
                                else:
                                    process_error = Exception("Handler returned False")
                            except Exception as e:
                                process_error = e
                                self.logger.error(
                                    f"Error processing message: {str(e)}",
                                    e,
                                    "attempt",
                                    retry,
                                    "traceId",
                                    trace_msg.traceId,
                                )

                        # Commit the message
                        self.logger.info("Committing message", "offset", msg.offset())
                        self.consumer.commit(message=msg, asynchronous=False)

                        if process_error:
                            self.logger.error(
                                "Failed to process message after max retries",
                                process_error,
                                "traceId",
                                trace_msg.traceId,
                            )
                        else:
                            self.logger.info(
                                "Successfully processed message",
                                "traceId",
                                trace_msg.traceId,
                            )
                    except json.JSONDecodeError as e:
                        self.logger.error("Error decoding message JSON", e)
                        # Commit invalid message to avoid getting stuck
                        self.consumer.commit(message=msg, asynchronous=False)
                    except Exception as e:
                        self.logger.error("Error processing message", e)
                        # Commit the message even if processing failed
                        self.consumer.commit(message=msg, asynchronous=False)
            except KafkaException as e:
                self.logger.error("Kafka exception", e)
                time.sleep(1)  # Avoid tight loop
            except Exception as e:
                self.logger.error("Unexpected exception", e)
                time.sleep(1)  # Avoid tight loop

    def _parse_dates_in_dict(self, data: Dict) -> None:
        """
        Recursively parse ISO format date strings to datetime objects in a dictionary
        Modifies the dictionary in place
        """
        if not isinstance(data, dict):
            return

        for key, value in data.items():
            if isinstance(value, str) and self._looks_like_iso_date(value):
                try:
                    # Try to use dateutil parser which is more flexible with formats
                    from dateutil import parser

                    data[key] = parser.parse(value)
                except (ImportError, Exception):
                    # Fallback method if dateutil is not available
                    try:
                        # Handle Z or timezone info
                        if value.endswith("Z"):
                            value = value[:-1] + "+00:00"
                        data[key] = datetime.fromisoformat(value)
                    except ValueError:
                        # If still failing, leave as string
                        pass
            elif isinstance(value, dict):
                self._parse_dates_in_dict(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._parse_dates_in_dict(item)

    def _looks_like_iso_date(self, value: str) -> bool:
        """Check if a string looks like an ISO format date"""
        if not isinstance(value, str):
            return False

        # Simple heuristic: contains T and either Z or +/-
        return ("T" in value) and (
            value.endswith("Z") or "+" in value or "-" in value[10:]
        )

    def close(self) -> None:
        """Close the Kafka consumer"""
        self.logger.info("Closing Kafka consumer")
        self.running = False
        if self.consumer:
            self.consumer.close()
