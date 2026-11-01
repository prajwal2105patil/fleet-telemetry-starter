package com.fleetbootcamp.telemetry.kafka;

import org.springframework.context.annotation.Configuration;

/**
 * TODO M2: define a DefaultErrorHandler @Bean here. Spring Boot plugs it into the listener automatically.
 *   - unparseable / wrong-schema messages: no retry, publish to telemetry.dlq (DeadLetterPublishingRecoverer)
 *   - other errors (DB briefly down): a few retries with a back-off, then DLQ
 *   Without this, Spring retries 10 times and then just logs and skips — the message is silently lost.
 */
@Configuration
public class KafkaConfig {
}
