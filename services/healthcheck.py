"""
HTTP Server for health checks to support Kubernetes liveness and readiness probes.
"""

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from utils.logging import Logger


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health check endpoints"""

    # Class-level variables to track application status
    is_ready = False
    is_db_healthy = False
    is_kafka_healthy = False
    logger = None

    def _send_response(self, status_code, content):
        """Helper method to send HTTP response"""
        self.send_response(status_code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def log_message(self, format, *args):
        """Override to use application logger instead of default handler"""
        if self.logger:
            self.logger.debug(f"Health check: {format % args}")

    def do_GET(self):
        """Handle GET requests"""
        if self.path == "/healthz/live":
            # Liveness probe - always return 200 if the server is running
            self._send_response(200, "OK")
        elif self.path == "/healthz/ready":
            # Readiness probe - return 200 only if the application is fully initialized and dependencies are healthy
            if (
                HealthCheckHandler.is_ready
                and HealthCheckHandler.is_db_healthy
                and HealthCheckHandler.is_kafka_healthy
            ):
                self._send_response(200, "Ready")
            else:
                status = []
                if not HealthCheckHandler.is_ready:
                    status.append("Application not ready")
                if not HealthCheckHandler.is_db_healthy:
                    status.append("Database not healthy")
                if not HealthCheckHandler.is_kafka_healthy:
                    status.append("Kafka not healthy")
                self._send_response(503, f"Not Ready: {', '.join(status)}")
        else:
            self._send_response(404, "Not Found")


class HealthCheckServer:
    """Server for handling health check requests"""

    def __init__(self, port: int, logger: Logger):
        """Initialize the health check server"""
        self.port = port
        self.logger = logger
        self.server = None
        self.thread = None
        self.is_running = False

        # Set the logger in the handler class
        HealthCheckHandler.logger = logger

    def start(self):
        """Start the health check server in a separate thread"""
        if self.is_running:
            return

        self.logger.info(f"Starting health check server on port {self.port}")

        try:
            self.server = HTTPServer(("0.0.0.0", self.port), HealthCheckHandler)
            self.thread = threading.Thread(target=self.server.serve_forever)
            self.thread.daemon = True
            self.thread.start()
            self.is_running = True
            self.logger.info("Health check server started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start health check server: {str(e)}", e)

    def set_ready(self, is_ready: bool):
        """Set the readiness state of the application"""
        HealthCheckHandler.is_ready = is_ready
        self.logger.info(f"Application readiness set to: {is_ready}")

    def set_db_health(self, is_healthy: bool):
        """Set the health state of the database connection"""
        HealthCheckHandler.is_db_healthy = is_healthy
        self.logger.info(f"Database health set to: {is_healthy}")

    def set_kafka_health(self, is_healthy: bool):
        """Set the health state of the Kafka connection"""
        HealthCheckHandler.is_kafka_healthy = is_healthy
        self.logger.info(f"Kafka health set to: {is_healthy}")

    def stop(self):
        """Stop the health check server"""
        if self.server and self.is_running:
            self.logger.info("Stopping health check server")
            self.server.shutdown()
            self.server.server_close()
            self.is_running = False
            self.logger.info("Health check server stopped")
