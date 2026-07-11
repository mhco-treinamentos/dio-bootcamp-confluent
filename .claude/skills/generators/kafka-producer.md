# Skill: Kafka Producer

> **Setup and configure Kafka producers for streaming synthetic data**

## Purpose

Configure Kafka producers for:
- High-throughput streaming
- Schema Registry integration
- Rate-limited production
- Error handling and retries

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Target Kafka topic |
| `schema` | No | Avro schema for serialization |
| `throughput` | No | Target messages/second |
| `config` | No | Custom producer configuration |

## Process

1. **Design Configuration**: Producer settings for use case
2. **Setup Schema Registry**: Avro serializer if needed
3. **Implement Producer**: Rate limiting, batching
4. **Add Monitoring**: Delivery callbacks, metrics
5. **Test**: Verify throughput and reliability

## Output Example

```python
"""Kafka producer for synthetic data streaming."""

import time
from dataclasses import dataclass
from typing import Callable

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


@dataclass
class KafkaProducerConfig:
    """Configuration for Kafka producer."""

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    client_id: str = "data-gen"
    acks: str = "all"
    retries: int = 3
    linger_ms: int = 5
    batch_size: int = 16384
    compression_type: str = "snappy"


class DataGenProducer:
    """Produce synthetic data to Kafka topics."""

    def __init__(
        self,
        topic: str,
        config: KafkaProducerConfig | None = None,
        avro_schema: str | None = None,
    ) -> None:
        self.topic = topic
        self.config = config or KafkaProducerConfig()
        self.producer = self._create_producer()
        self.serializer = self._create_serializer(avro_schema) if avro_schema else None
        self._delivered = 0
        self._failed = 0

    def _create_producer(self) -> Producer:
        """Create Kafka producer with configuration."""
        return Producer({
            "bootstrap.servers": self.config.bootstrap_servers,
            "client.id": self.config.client_id,
            "acks": self.config.acks,
            "retries": self.config.retries,
            "linger.ms": self.config.linger_ms,
            "batch.size": self.config.batch_size,
            "compression.type": self.config.compression_type,
        })

    def _create_serializer(self, schema: str) -> AvroSerializer:
        """Create Avro serializer with Schema Registry."""
        schema_registry = SchemaRegistryClient({
            "url": self.config.schema_registry_url,
        })
        return AvroSerializer(schema_registry, schema)

    def _delivery_callback(self, err, msg) -> None:
        """Handle delivery reports."""
        if err:
            self._failed += 1
        else:
            self._delivered += 1

    def produce(self, record: dict, key: str | None = None) -> None:
        """Produce a single record to Kafka."""
        value = self.serializer(record, None) if self.serializer else record
        self.producer.produce(
            topic=self.topic,
            key=key,
            value=value,
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def produce_batch(
        self,
        records: list[dict],
        rate_per_second: float = 0,
        key_func: Callable[[dict], str] | None = None,
    ) -> None:
        """Produce batch of records with optional rate limiting."""
        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0

        for record in records:
            key = key_func(record) if key_func else None
            self.produce(record, key)

            if interval > 0:
                time.sleep(interval)

        self.flush()

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending messages."""
        self.producer.flush(timeout)

    @property
    def stats(self) -> dict:
        """Return delivery statistics."""
        return {
            "delivered": self._delivered,
            "failed": self._failed,
        }
```

## Example Usage

```
"Configure a Kafka producer for 1000 guias/second"
"Setup Avro serialization with Schema Registry"
"Create a rate-limited producer for demo streaming"
```
