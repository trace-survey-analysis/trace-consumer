import logging
import os
import sys
from typing import Any, Dict, List, Tuple, Union


class Logger:
    """Custom logger implementation that mimics the behavior of the Go logger"""

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARN = logging.WARNING
    ERROR = logging.ERROR
    FATAL = logging.CRITICAL

    def __init__(self, name: str = "trace-consumer"):
        self.logger = logging.getLogger(name)

        # Get log level from environment
        log_level_str = os.getenv("LOG_LEVEL", "info").lower()
        log_level = logging.INFO

        if log_level_str == "debug":
            log_level = logging.DEBUG
        elif log_level_str == "info":
            log_level = logging.INFO
        elif log_level_str in ("warn", "warning"):
            log_level = logging.WARNING
        elif log_level_str == "error":
            log_level = logging.ERROR
        elif log_level_str in ("fatal", "critical"):
            log_level = logging.CRITICAL

        self.logger.setLevel(log_level)

        # Create handler if none exists
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("[%(levelname)s] %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _format_keyvals(self, keyvals: List[Any]) -> str:
        """Format key-value pairs for logging, similar to Go's logger"""
        if not keyvals:
            return ""

        result = ""
        for i in range(0, len(keyvals), 2):
            key = keyvals[i]
            value = "missing" if i + 1 >= len(keyvals) else keyvals[i + 1]
            result += f" {key}={value}"

        return result

    def debug(self, msg: str, *keyvals: Any) -> None:
        """Log a debug message"""
        self.logger.debug(f"{msg}{self._format_keyvals(keyvals)}")

    def info(self, msg: str, *keyvals: Any) -> None:
        """Log an info message"""
        self.logger.info(f"{msg}{self._format_keyvals(keyvals)}")

    def warn(self, msg: str, *keyvals: Any) -> None:
        """Log a warning message"""
        self.logger.warning(f"{msg}{self._format_keyvals(keyvals)}")

    def error(self, msg: str, err: Exception = None, *keyvals: Any) -> None:
        """Log an error message"""
        err_msg = f" error={err}" if err else ""
        self.logger.error(f"{msg}{err_msg}{self._format_keyvals(keyvals)}")

    def fatal(self, msg: str, err: Exception = None, *keyvals: Any) -> None:
        """Log a fatal message and exit"""
        err_msg = f" error={err}" if err else ""
        self.logger.critical(f"{msg}{err_msg}{self._format_keyvals(keyvals)}")
        sys.exit(1)


def get_logger(name: str = "trace-consumer") -> Logger:
    """Get a logger instance"""
    return Logger(name)
