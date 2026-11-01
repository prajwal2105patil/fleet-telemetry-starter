"""M2: Kafka consumer -> Postgres.   Run:  python -m app.consumer   (from the python/ folder)

The wiring is done for you: it subscribes to telemetry.raw and prints each message.
Auto-commit is OFF on purpose — you decide when an offset is committed.
"""
import logging
import signal

from confluent_kafka import Consumer, KafkaError

from app import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("consumer")


def run() -> None:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.TOPIC_RAW])
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log.info("consuming %s as group %s", config.TOPIC_RAW, config.CONSUMER_GROUP)
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            # Log the raw bytes: parsing here would crash the consumer on a non-JSON (or empty) message
            # before the DLQ handling below exists. Parsing is step 1 of the M2 TODO.
            log.info("partition=%s offset=%s key=%s value=%s",
                     msg.partition(), msg.offset(), msg.key(), msg.value())

            # TODO M2:
            #  1. Parse msg.value() into TelemetryEvent (app.models).
            #  2. If it can't be parsed (bad JSON / wrong schema): publish it to config.TOPIC_DLQ with the error,
            #     then move on — one bad message must never block the partition.
            #  3. Insert into Postgres in ONE transaction (repository.insert_telemetry) — idempotent.
            #  4. Commit the Kafka offset only AFTER the DB commit:  consumer.commit(message=msg, asynchronous=False)
            #     Q: what happens on a crash between step 3 and 4? Why is that OK here?
            # TODO M3: evaluate the overspeed rule for this vehicle and insert an alert.
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
