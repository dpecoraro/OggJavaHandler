package com.santander.goldengate.handler;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Applies the serializers required by the Schema Registry Avro wire format.
 */
final class AvroProducerConfiguration {

    static final String KAFKA_AVRO_SERIALIZER =
            "io.confluent.kafka.serializers.KafkaAvroSerializer";

    private AvroProducerConfiguration() {
    }

    static void apply(Properties kafkaProps) {
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_AVRO_SERIALIZER);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_AVRO_SERIALIZER);
        kafkaProps.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProps.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProps.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProps.putIfAbsent(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        validateDeliveryGuarantees(kafkaProps);
    }

    static boolean requiresExplicitSchemaRegistration(Properties kafkaProps) {
        return !Boolean.parseBoolean(kafkaProps.getProperty("auto.register.schemas", "true"));
    }

    static void validateDeliveryGuarantees(Properties kafkaProps) {
        String idempotence = kafkaProps.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        if (!Boolean.parseBoolean(idempotence)) {
            throw invalid(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence, "true");
        }

        String acknowledgements = kafkaProps.getProperty(ProducerConfig.ACKS_CONFIG);
        if (!("all".equalsIgnoreCase(acknowledgements) || "-1".equals(acknowledgements))) {
            throw invalid(ProducerConfig.ACKS_CONFIG, acknowledgements, "all");
        }

        int maxInFlight = positiveInteger(
                kafkaProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        if (maxInFlight > 5) {
            throw invalid(
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    String.valueOf(maxInFlight),
                    "an integer between 1 and 5");
        }

        positiveInteger(kafkaProps, ProducerConfig.RETRIES_CONFIG);
    }

    private static int positiveInteger(Properties kafkaProps, String propertyName) {
        String configured = kafkaProps.getProperty(propertyName);
        try {
            int parsed = Integer.parseInt(configured);
            if (parsed <= 0) {
                throw invalid(propertyName, configured, "greater than zero");
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw invalid(propertyName, configured, "a valid integer");
        }
    }

    private static IllegalArgumentException invalid(
            String propertyName, String configured, String required) {
        return new IllegalArgumentException(
                "Unsafe Kafka producer configuration: " + propertyName + '=' + configured
                        + "; required " + required + " to preserve ordering and delivery guarantees");
    }
}
