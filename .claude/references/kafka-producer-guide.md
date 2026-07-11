# Kafka Producer Guide

> **Best practices for producing synthetic payments data to Confluent Cloud (with the slim local fallback)**

Delivery semantics are a bootcamp anchor: topics use **RF=3**, **`min.insync.replicas=2`**, and producers use **`acks=all`** + idempotence. Every config below follows those defaults unless explicitly trading them away.

## Producer Configuration

### Profile Selector (`KAFKA_PROFILE=cloud|local`)

Credentials always come from env vars (`CC_*`) — never hardcoded, never committed (see [rules.md](../rules.md)). The cloud/local switch is a single configuration point:

```python
import os

from confluent_kafka import Producer

CLOUD_PRODUCER_CONFIG = {
    # Connection — Confluent Cloud
    "bootstrap.servers": os.environ["CC_BOOTSTRAP"],
    "client.id": "bootcamp-gen-payments",

    # Authentication
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CC_API_KEY"],
    "sasl.password": os.environ["CC_API_SECRET"],

    # Reliability (bootcamp anchor: acks=all + idempotence)
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,

    # Performance
    "linger.ms": 5,
    "batch.size": 16384,
    "compression.type": "snappy",
}

LOCAL_PRODUCER_CONFIG = {
    # Connection — local fallback (single KRaft broker, PLAINTEXT)
    "bootstrap.servers": "localhost:9092",
    "client.id": "bootcamp-gen-payments",

    # Same reliability settings (but RF=1 locally — see note below)
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
    "compression.type": "snappy",
}


def build_producer() -> Producer:
    """Build a producer for the active profile (KAFKA_PROFILE=cloud|local)."""
    profile = os.environ.get("KAFKA_PROFILE", "cloud")
    config = CLOUD_PRODUCER_CONFIG if profile == "cloud" else LOCAL_PRODUCER_CONFIG
    return Producer(config)


producer = build_producer()
```

> **Local fallback caveat**: with a single broker, RF=1 — the reliability defaults (RF=3, `min.insync.replicas=2`) cannot be demonstrated locally. Say so explicitly when recording local demos.

### High-Throughput Configuration
```python
high_throughput_overrides = {
    # Apply on top of the active profile config
    "client.id": "bootcamp-gen-high-throughput",

    # Maximize throughput
    "acks": "1",  # Trades the reliability anchor for speed — lab use only
    "linger.ms": 50,  # Wait longer to batch
    "batch.size": 65536,  # Larger batches
    "compression.type": "lz4",  # Faster compression

    # Buffer management
    "queue.buffering.max.messages": 100000,
}
```

### Demo/Development Configuration
```python
demo_overrides = {
    # Apply on top of the active profile config
    "client.id": "bootcamp-gen-demo",

    # Small batches for visibility in live demos
    "linger.ms": 0,
    "batch.size": 1024,
}
```

## Schema Registry Integration

### Avro Serialization
```python
import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Schema Registry client — Confluent Cloud (SR_* env vars per rules.md)
schema_registry = SchemaRegistryClient({
    "url": os.environ["SR_URL"],
    "basic.auth.user.info": f"{os.environ['SR_API_KEY']}:{os.environ['SR_API_SECRET']}",
})
# Local fallback: {"url": "http://localhost:8081"} — no auth

# Avro schema — payments transaction
avro_schema = """
{
    "type": "record",
    "name": "Transaction",
    "namespace": "com.bootcamp.payments",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "account_id", "type": "string"},
        {"name": "card_id", "type": ["null", "string"], "default": null},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
        {"name": "status", "type": "string"},
        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
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
    topic="dev.payments.gen.transactions.created.v1",
    key=str(record["transaction_id"]),
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
        "account_id": {"type": "string"},
        "cpf": {"type": "string"},
        "name": {"type": "string"},
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
    topic="dev.payments.gen.accounts.snapshot.v1",
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
    topic="dev.payments.gen.transactions.created.v1",
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
import os

from confluent_kafka.admin import AdminClient, NewTopic

# Reuses the connection/auth keys from the active profile config
admin = AdminClient({
    "bootstrap.servers": os.environ["CC_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CC_API_KEY"],
    "sasl.password": os.environ["CC_API_SECRET"],
})

def create_topic(
    name: str,
    partitions: int = 6,
    replication: int = 3,  # Bootcamp anchor: RF=3 (Confluent Cloud enforces this)
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

# Create topics for the payments labs (naming convention: rules.md)
# min.insync.replicas=2 is the cluster default in Confluent Cloud
for topic_name, parts in [
    ("dev.payments.pg-payments.accounts.cdc.v1", 6),
    ("dev.payments.pg-payments.cards.cdc.v1", 6),
    ("dev.payments.pg-payments.transactions.cdc.v1", 12),
    ("dev.payments.gen.transactions.created.v1", 6),
]:
    create_topic(
        topic_name,
        partitions=parts,
        config={
            "retention.ms": "86400000",  # 1 day for lab topics (credit discipline)
            "cleanup.policy": "delete",
        },
    )
```

## Local Fallback (Docker Compose)

Slim KRaft single-broker + Schema Registry stack for offline demos/recording without spending credits — see [skills/confluent/local-dev-fallback.md](../skills/confluent/local-dev-fallback.md) for the canonical compose file and its limits. Use `KAFKA_PROFILE=local` so producers point at `localhost:9092` (PLAINTEXT) and `http://localhost:8081` without touching code.
