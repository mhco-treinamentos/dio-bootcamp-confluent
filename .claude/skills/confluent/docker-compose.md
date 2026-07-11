# Confluent Platform Docker Compose Generator

## Skill: `/confluent-docker-compose`

Generate a complete docker-compose.yml for Confluent Platform Community Edition.

## Usage

```
/confluent-docker-compose [options]
```

### Options
- `--minimal` - Only Zookeeper, Kafka, Schema Registry
- `--full` - All components including ksqlDB and REST Proxy
- `--connect-only` - Add Kafka Connect to existing setup
- `--with-postgres` - Include PostgreSQL for testing connectors
- `--with-monitoring` - Add Prometheus and Grafana

## Generated Configuration

### Minimal Setup (Default)

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  broker:
    image: confluentinc/cp-kafka:7.5.0
    hostname: broker
    container_name: broker
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_RETENTION_BYTES: 1073741824
    volumes:
      - broker-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 5

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      broker:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: BACKWARD
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  zookeeper-data:
  zookeeper-logs:
  broker-data:

networks:
  default:
    name: confluent-network
```

### Full Setup (with Connect, Control Center, ksqlDB)

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  broker:
    image: confluentinc/cp-kafka:7.5.0
    hostname: broker
    container_name: broker
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 168
    volumes:
      - broker-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 5

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      broker:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 5

  connect:
    image: confluentinc/cp-kafka-connect:7.5.0
    hostname: connect
    container_name: connect
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: broker:29092
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: connect-cluster
      CONNECT_CONFIG_STORAGE_TOPIC: _connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: _connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: _connect-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
      CONNECT_LOG4J_LOGGERS: org.apache.kafka.connect=INFO
    volumes:
      - connect-plugins:/usr/share/confluent-hub-components
    command:
      - bash
      - -c
      - |
        echo "Installing connectors..."
        confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
        confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.4.0
        confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:14.0.12
        confluent-hub install --no-prompt confluentinc/kafka-connect-json-schema-converter:7.5.0
        echo "Starting Connect..."
        /etc/confluent/docker/run
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8083/connectors"]
      interval: 30s
      timeout: 10s
      retries: 10
      start_period: 90s

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.5.0
    hostname: control-center
    container_name: control-center
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
      connect:
        condition: service_healthy
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: broker:29092
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER: http://connect:8083
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
      PORT: 9021

  ksqldb-server:
    image: confluentinc/cp-ksqldb-server:7.5.0
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "8088:8088"
    environment:
      KSQL_BOOTSTRAP_SERVERS: broker:29092
      KSQL_LISTENERS: http://0.0.0.0:8088
      KSQL_KSQL_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      KSQL_KSQL_SERVICE_ID: ksqldb-cluster
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8088/info"]
      interval: 10s
      timeout: 5s
      retries: 5

  rest-proxy:
    image: confluentinc/cp-kafka-rest:7.5.0
    hostname: rest-proxy
    container_name: rest-proxy
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "8082:8082"
    environment:
      KAFKA_REST_HOST_NAME: rest-proxy
      KAFKA_REST_BOOTSTRAP_SERVERS: broker:29092
      KAFKA_REST_LISTENERS: http://0.0.0.0:8082
      KAFKA_REST_SCHEMA_REGISTRY_URL: http://schema-registry:8081

volumes:
  zookeeper-data:
  zookeeper-logs:
  broker-data:
  connect-plugins:

networks:
  default:
    name: confluent-network
```

## Commands After Deployment

```bash
# Start services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f broker
docker-compose logs -f connect

# Stop services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Useful API Endpoints

| Service | Endpoint | Description |
|---------|----------|-------------|
| Schema Registry | http://localhost:8081/subjects | List schemas |
| Connect | http://localhost:8083/connectors | List connectors |
| Connect | http://localhost:8083/connector-plugins | List plugins |
| Control Center | http://localhost:9021 | Web UI |
| ksqlDB | http://localhost:8088/info | Server info |
| REST Proxy | http://localhost:8082/topics | List topics |
