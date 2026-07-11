# Kafka Producer Guide

> **Best practices for producing synthetic payments data to Kafka**

## Producer Configuration

### Basic Configuration
```python
from confluent_kafka import Producer

producer_config = {
    # Connection
    "bootstrap.servers": "localhost:9092",
    "client.id": "data-gen-producer",

    # Reliability
    "acks": "all",
    "retries": 3,
    "retry.backoff.ms": 100,

    # Performance
    "linger.ms": 5,
    "batch.size": 16384,
    "compression.type": "snappy",

    # Ordering
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
}

producer = Producer(producer_config)
```

### Confluent Cloud Configuration
```python
confluent_cloud_config = {
    # Connection
    "bootstrap.servers": "<confluent-cloud-bootstrap>.confluent.cloud:9092",
    "client.id": "workshop-data-gen",

    # Authentication
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "<API_KEY>",
    "sasl.password": "<API_SECRET>",

    # Reliability
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 200,

    # Performance
    "linger.ms": 10,
    "batch.size": 32768,
    "compression.type": "lz4",

    # Ordering
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
}
```

### High-Throughput Configuration
```python
high_throughput_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "data-gen-high-throughput",

    # Maximize throughput
    "acks": "1",  # Trade reliability for speed
    "linger.ms": 50,  # Wait longer to batch
    "batch.size": 65536,  # Larger batches
    "compression.type": "lz4",  # Faster compression

    # Buffer management
    "buffer.memory": 67108864,  # 64MB
    "queue.buffering.max.messages": 100000,
}
```

### Demo/Development Configuration
```python
demo_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "data-gen-demo",

    # Simple, reliable
    "acks": "all",
    "retries": 3,

    # Small batches for visibility
    "linger.ms": 0,
    "batch.size": 1024,
}
```

## Schema Registry Integration

### Avro Serialization
```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Schema Registry client
schema_registry = SchemaRegistryClient({
    "url": "http://localhost:8081",
})

# Avro schema — Guia TISS
avro_schema = """
{
    "type": "record",
    "name": "Guia",
    "namespace": "com.workshop.payments",
    "fields": [
        {"name": "guia_id", "type": "string"},
        {"name": "numero_guia", "type": "string"},
        {"name": "membro_id", "type": "string"},
        {"name": "tipo_guia", "type": "string"},
        {"name": "valor_autorizado", "type": "double"},
        {"name": "dt_solicitacao", "type": "long", "logicalType": "timestamp-millis"}
    ]
}
"""

# Create serializer
avro_serializer = AvroSerializer(
    schema_registry,
    avro_schema,
    to_dict=lambda obj, ctx: obj,  # If already dict
)

# Produce with schema
producer.produce(
    topic="dev.saude.oracle-autorizacoes.guias.cdc.v1",
    key=str(record["guia_id"]),
    value=avro_serializer(record, None),
)
```

### JSON Schema Serialization
```python
from confluent_kafka.schema_registry.json_schema import JSONSerializer

json_schema = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "membro_id": {"type": "string"},
        "cpf": {"type": "string"},
        "nome": {"type": "string"},
        "plano_id": {"type": "string"},
        "status": {"type": "string"}
    }
}
"""

json_serializer = JSONSerializer(
    json_schema,
    schema_registry,
)
```

## Rate Limiting

### Token Bucket Rate Limiter
```python
import time
from threading import Lock

class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate_per_second: float):
        self.rate = rate_per_second
        self.tokens = rate_per_second
        self.last_update = time.monotonic()
        self.lock = Lock()

    def acquire(self) -> None:
        """Wait until a token is available."""
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(
                self.rate,
                self.tokens + elapsed * self.rate,
            )
            self.last_update = now

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                time.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1
```

### Rate-Limited Producer
```python
class RateLimitedProducer:
    """Kafka producer with rate limiting."""

    def __init__(
        self,
        config: dict,
        rate_per_second: float = 1000,
    ):
        self.producer = Producer(config)
        self.rate_limiter = RateLimiter(rate_per_second)

    def produce(self, topic: str, value: dict, key: str = None) -> None:
        self.rate_limiter.acquire()
        self.producer.produce(
            topic=topic,
            key=key,
            value=value,
        )
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()
```

## Delivery Callbacks

### Basic Callback
```python
def delivery_callback(err, msg):
    """Handle delivery reports."""
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}")

producer.produce(
    topic="dev.saude.oracle-beneficiarios.membros.cdc.v1",
    value=record,
    callback=delivery_callback,
)
```

### Metrics Collection
```python
from dataclasses import dataclass, field
from threading import Lock

@dataclass
class ProducerMetrics:
    """Track producer delivery metrics."""

    delivered: int = 0
    failed: int = 0
    _lock: Lock = field(default_factory=Lock)

    def on_delivery(self, err, msg) -> None:
        with self._lock:
            if err:
                self.failed += 1
            else:
                self.delivered += 1

    @property
    def success_rate(self) -> float:
        total = self.delivered + self.failed
        return self.delivered / total if total > 0 else 0.0

metrics = ProducerMetrics()
producer.produce(
    topic="dev.saude.oracle-autorizacoes.guias.cdc.v1",
    value=record,
    callback=metrics.on_delivery,
)
```

## Error Handling

### Retry Logic
```python
import time
from confluent_kafka import KafkaException

def produce_with_retry(
    producer: Producer,
    topic: str,
    value: dict,
    max_retries: int = 3,
    backoff: float = 0.1,
) -> bool:
    """Produce message with retry logic."""
    for attempt in range(max_retries):
        try:
            producer.produce(topic=topic, value=value)
            producer.poll(0)
            return True
        except BufferError:
            # Buffer full, wait and retry
            producer.poll(1)
            time.sleep(backoff * (2 ** attempt))
        except KafkaException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff * (2 ** attempt))

    return False
```

## Topic Management

### Topic Creation
```python
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

def create_topic(
    name: str,
    partitions: int = 6,
    replication: int = 1,
    config: dict = None,
) -> None:
    """Create Kafka topic."""
    topic = NewTopic(
        topic=name,
        num_partitions=partitions,
        replication_factor=replication,
        config=config or {},
    )

    futures = admin.create_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic_name} created")
        except Exception as e:
            print(f"Failed to create {topic_name}: {e}")

# Create topics for payments CDC
for topic_name, parts in [
    ("dev.saude.oracle-beneficiarios.membros.cdc.v1", 6),
    ("dev.saude.oracle-autorizacoes.guias.cdc.v1", 12),
    ("dev.saude.oracle-prestadores.prestadores.cdc.v1", 3),
    ("dev.saude.oracle-faturamento.mensalidades.cdc.v1", 6),
]:
    create_topic(
        topic_name,
        partitions=parts,
        config={
            "retention.ms": "86400000",  # 1 day for dev
            "cleanup.policy": "delete",
        },
    )
```

## Docker Compose Setup

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
```
