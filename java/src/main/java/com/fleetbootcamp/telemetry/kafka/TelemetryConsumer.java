package com.fleetbootcamp.telemetry.kafka;

import com.fleetbootcamp.telemetry.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * M2: telemetry.raw -> Postgres. The wiring is done: this already receives every message (group telemetry-writer).
 * application.yml sets enable-auto-commit=false and ack-mode=record: the offset is committed only after this
 * method returns without an exception.
 *
 * TODO M2:
 *  1. Parse record.value() (JSON) into TelemetryEvent with the Spring ObjectMapper (inject it).
 *  2. Bad JSON / schema != "v1" / missing fields -> must NOT block the partition: send it to Topics.DLQ.
 *     Recommended: throw a custom exception here and configure a DefaultErrorHandler +
 *     DeadLetterPublishingRecoverer bean in KafkaConfig.
 *  3. Insert into Postgres idempotently (TelemetryRepository) inside a @Transactional service method.
 *     Q: if the app crashes after the DB commit but before the offset commit, what happens? Why is it OK?
 * TODO M3: evaluate the overspeed rule (see README) and insert into alerts.
 */
@Component
@ConditionalOnProperty(name = "app.consumer-enabled", havingValue = "true", matchIfMissing = true)
public class TelemetryConsumer {
    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    @KafkaListener(topics = Topics.RAW)
    public void onMessage(ConsumerRecord<String, String> record) {
        log.info("partition={} offset={} key={} value={}", record.partition(), record.offset(), record.key(), record.value());
    }
}
