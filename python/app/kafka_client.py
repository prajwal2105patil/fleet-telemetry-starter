"""Kafka producer wrapper. Key every message by VIN so one vehicle's events stay ordered in one partition."""
import json
import logging

from confluent_kafka import KafkaException, Producer

from app import config

log = logging.getLogger(__name__)


class PublishError(Exception):
    """A message could not be delivered to Kafka (timeout, broker error, or local queue full)."""


class EventPublisher:
    def __init__(self, bootstrap: str = config.KAFKA_BOOTSTRAP):
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
        })

    def publish(self, topic: str, key: str, value: dict, headers: dict | None = None) -> None:
        """Fire and forget: queues the message and returns. Delivery failures are only logged."""
        self._producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(value).encode(),
            headers=[(k, str(v).encode()) for k, v in (headers or {}).items()],
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def publish_and_wait(self, topic: str, key: str | bytes | None, value: bytes, headers: dict | None = None,
                         timeout: float = 10.0) -> None:
        """Send and block until the broker acknowledges (acks=all). Raises PublishError on any failure.

        Used where losing the message is not acceptable: the ingest API (202 means Kafka has it) and the DLQ
        (the source offset is committed only after this returns). `key` may be raw bytes so a DLQ copy keeps
        the original key even when it is not valid UTF-8.
        """
        outcome: dict = {}

        def on_delivery(err, _msg):
            outcome["err"] = err
            outcome["done"] = True

        try:
            self._producer.produce(
                topic,
                key=key.encode() if isinstance(key, str) else key,
                value=value,
                headers=[(k, str(v).encode()) for k, v in (headers or {}).items()],
                on_delivery=on_delivery,
            )
        except (BufferError, KafkaException) as e:
            raise PublishError(f"could not queue message for {topic}: {e}") from e
        self._producer.flush(timeout)
        if not outcome.get("done"):
            raise PublishError(f"no acknowledgement from Kafka for {topic} within {timeout}s")
        if outcome["err"] is not None:
            raise PublishError(f"Kafka rejected the message for {topic}: {outcome['err']}")

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


def flush_publisher(timeout: float = 5.0) -> None:
    """Called on API shutdown so queued messages are not dropped. Does nothing if no producer was ever created."""
    if _publisher is not None:
        _publisher.flush(timeout)
