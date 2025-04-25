# TRACE Survey Consumer

![Python](https://img.shields.io/badge/Python-3776AB.svg?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20.svg?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED.svg?style=for-the-badge&logo=docker&logoColor=white)
![Jenkins](https://img.shields.io/badge/Jenkins-D24939.svg?style=for-the-badge&logo=jenkins&logoColor=white)
![Semantic Release](https://img.shields.io/badge/Semantic_Release-494949.svg?style=for-the-badge&logo=semantic-release&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1.svg?style=for-the-badge&logo=postgresql&logoColor=white)

This service consumes processed trace survey data from Kafka and stores it in a PostgreSQL database.

## Overview

The TRACE Survey Consumer application is responsible for:

1. Consuming processed trace survey data from the `trace-survey-processed` Kafka topic (published by [trace-processor](https://github.com/cyse7125-sp25-team03/trace-processor.git))
2. Validating and transforming the data into appropriate database models
3. Storing the data in a PostgreSQL database in a dedicated `trace` schema
4. Tracking processed messages to ensure idempotency (no duplicate processing)

## Database Schema

The application stores data in the following tables (all in the `trace` schema):

- `courses`: Information about each course
- `instructors`: Information about instructors
- `course_instructors`: Join table linking courses to instructors
- `ratings`: Rating questions and responses
- `comments`: Student comments on courses and instructors
- `processed_traces`: Track which traces have been processed

## Architecture

The service consists of the following components:

- **Kafka Consumer**: Consumes messages from the `trace-survey-processed` topic
- **PostgreSQL Client**: Manages database connections and operations
- **Data Models**: Pydantic models for validating and converting data
- **Health Check Server**: Provides endpoints for Kubernetes liveness and readiness probes

## Data Flow

1. A message is received from the `trace-survey-processed` Kafka topic (from [trace-processor](https://github.com/cyse7125-sp25-team03/trace-processor.git)) containing structured trace data
2. The data is validated and converted to database models
3. A transaction is started and the data is stored in the database:
   - Store instructor information (or retrieve existing)
   - Store course information
   - Link course and instructor
   - Store ratings
   - Store comments
   - Record the trace ID as processed
4. The transaction is committed
5. After data is stored, it becomes available for analysis by the [embedding-service](https://github.com/cyse7125-sp25-team03/embedding-service.git)

## Configuration

The service is configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SERVER_PORT` | HTTP server port | `8082` |
| `KAFKA_BROKERS` | Comma-separated list of Kafka brokers | `kafka-controller-0...` |
| `KAFKA_TOPIC` | Topic to consume messages from | `trace-survey-processed` |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `trace-consumer` |
| `KAFKA_USERNAME` | Kafka username for SASL auth | `""` |
| `KAFKA_PASSWORD` | Kafka password for SASL auth | `""` |
| `DB_HOST` | PostgreSQL database host | `pg-postgresql...` |
| `DB_PORT` | PostgreSQL database port | `5432` |
| `DB_NAME` | PostgreSQL database name | `trace` |
| `DB_SCHEMA` | PostgreSQL schema name | `trace` |
| `DB_USER` | PostgreSQL username | `""` |
| `DB_PASSWORD` | PostgreSQL password | `""` |
| `MAX_RETRIES` | Maximum number of processing retries | `3` |
| `RETRY_BACKOFF_MS` | Backoff time in ms between retries | `1000` |
| `HEALTH_CHECK_INTERVAL` | Interval in seconds to check service health | `60` |
| `LOG_LEVEL` | Logging level (debug, info, warn, error, fatal) | `info` |

## Database Setup

The application expects the database schema to be created before running. The database migrations are managed by the [db-trace-processor](https://github.com/cyse7125-sp25-team03/db-trace-processor.git) repository.

The migrations create the `trace` schema and all required tables.

## Health Checks

The application provides the following health check endpoints:

- `/healthz/live`: Liveness probe to check if the application is running
- `/healthz/ready`: Readiness probe to check if the application is ready to process requests, including connectivity to Kafka and the database

## Development

### Prerequisites

- Python 3.10+
- PostgreSQL database
- Kafka

### Setup

1. Clone the repository
   ```
   git clone https://github.com/cyse7125-sp25-team03/trace-consumer.git
   cd trace-consumer
   ```
   
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

### Running Locally

```
python app.py
```

## Deployment

### Building the Docker Image

```
docker build -t trace-consumer:latest .
```

### Running with Docker

```
docker run -p 8082:8082 \
  -e KAFKA_BROKERS=kafka:9092 \
  -e DB_HOST=postgres \
  -e DB_PASSWORD=postgres \
  trace-consumer:latest
```

### Kubernetes Deployment

The service can be deployed to Kubernetes using Helm charts from the [helm-charts](https://github.com/cyse7125-sp25-team03/helm-charts.git) repository:

```bash
# Clone the helm-charts repository
git clone https://github.com/cyse7125-sp25-team03/helm-charts.git

# Install the trace-consumer chart
helm install trace-consumer ./trace-consumer -n trace-consumer
```

Alternatively, you can add the Helm repository and install directly:

```bash
# Add the Helm repository
helm repo add team03 https://github.com/cyse7125-sp25-team03/helm-charts

# Install the trace-consumer chart
helm install trace-consumer team03/trace-consumer -n trace-consumer
```

## Input Message Format

The consumer expects messages from the `trace-survey-processed` topic in the following format:

```json
{
  "traceId": "string",
  "course": {
    "courseId": "string",
    "courseName": "string",
    "subject": "string",
    "catalogSection": "string",
    "semester": "string",
    "year": 2023,
    "enrollment": 25,
    "responses": 20,
    "declines": 1,
    "processedAt": "2023-01-01T00:00:00Z",
    "originalFileName": "string",
    "gcsBucket": "string",
    "gcsPath": "string"
  },
  "instructor": {
    "name": "string"
  },
  "ratings": [
    {
      "questionText": "string",
      "category": "string",
      "responses": 20,
      "responseRate": 0.8,
      "courseMean": 4.5,
      "deptMean": 4.2,
      "univMean": 4.0,
      "courseMedian": 5.0,
      "deptMedian": 4.0,
      "univMedian": 4.0
    }
  ],
  "comments": [
    {
      "category": "string",
      "questionText": "string",
      "responseNumber": 1,
      "commentText": "string"
    }
  ],
  "processedAt": "2023-01-01T00:00:00Z",
  "error": "string"
}
```

## CI/CD and Releases

This project uses Jenkins for continuous integration and Semantic Release for versioning:

- When a pull request is successfully merged, a Docker image is built
- The Semantic Versioning bot creates a release on GitHub with a tag
- The tagged release is used for the Docker image, which is then pushed to Docker Hub

## License

This project is licensed under the GNU General Public License v3.0. See the [LICENSE](LICENSE) file for details.