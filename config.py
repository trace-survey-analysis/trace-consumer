import os
from typing import List, Dict, Any


class Config:
    """Configuration class that loads values from environment variables"""

    def __init__(self):
        # Server configuration
        self.server_port = self._get_env("SERVER_PORT", "8082")

        # Kafka configuration
        kafka_brokers_str = self._get_env(
            "KAFKA_BROKERS",
            "kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092,"
            "kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092,"
            "kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092",
        )
        self.kafka_brokers = kafka_brokers_str.split(",")
        self.kafka_topic = self._get_env("KAFKA_TOPIC", "trace-survey-processed")
        self.consumer_group = self._get_env("KAFKA_CONSUMER_GROUP", "trace-consumer")
        self.kafka_username = self._get_env("KAFKA_USERNAME", "")
        self.kafka_password = self._get_env("KAFKA_PASSWORD", "")
        self.kafka_auth = bool(self.kafka_username and self.kafka_password)

        # Database configuration
        self.db_host = self._get_env(
            "DB_HOST", "postgres-postgresql.postgres.svc.cluster.local"
        )
        self.db_port = int(self._get_env("DB_PORT", "5432"))
        self.db_name = self._get_env("DB_NAME", "trace_db")
        self.db_user = self._get_env("DB_USER", "postgres")
        self.db_password = self._get_env("DB_PASSWORD", "")
        self.db_schema = self._get_env("DB_SCHEMA", "trace")

        # Retry configuration
        self.max_retries = int(self._get_env("MAX_RETRIES", "3"))
        self.retry_backoff_ms = int(self._get_env("RETRY_BACKOFF_MS", "1000"))

        # Health check configuration
        self.health_check_interval = int(self._get_env("HEALTH_CHECK_INTERVAL", "60"))

    def _get_env(self, key: str, default: str) -> str:
        """Get an environment variable or return a default value"""
        return os.environ.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to a dictionary"""
        return {
            "server_port": self.server_port,
            "kafka_brokers": self.kafka_brokers,
            "kafka_topic": self.kafka_topic,
            "consumer_group": self.consumer_group,
            "kafka_auth": self.kafka_auth,
            "db_host": self.db_host,
            "db_port": self.db_port,
            "db_name": self.db_name,
            "db_user": self.db_user,
            "db_schema": self.db_schema,
            "max_retries": self.max_retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "health_check_interval": self.health_check_interval,
        }


def load_config() -> Config:
    """Load and return configuration"""
    return Config()
