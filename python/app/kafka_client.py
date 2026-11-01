"""Kafka producer wrapper. Key every message by VIN so one vehicle's events stay ordered in one partition."""
import json
import logging

from confluent_kafka import Producer

from app import config

log = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, bootstrap: str = config.KAFKA_BOOTSTRAP):
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
        })

    def publish(self, topic: str, key: str, value: dict, headers: dict | None = None) -> None:
        self._producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(value).encode(),
            headers=[(k, str(v).encode()) for k, v in (headers or {}).items()],
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> int:
        return self._producer.flush(timeout)

    @staticmethod
    def _on_delivery(err, msg):
        if err is not None:
            log.error("kafka delivery failed topic=%s key=%s err=%s", msg.topic(), msg.key(), err)


_publisher = None


def get_publisher() -> EventPublisher:
    """FastAPI dependency. Tests override this with a fake."""
    global _publisher
    if _publisher is None:
        _publisher = EventPublisher()
    return _publisher
