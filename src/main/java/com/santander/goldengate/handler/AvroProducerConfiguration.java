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
    }
}
