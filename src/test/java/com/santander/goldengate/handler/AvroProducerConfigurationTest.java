package com.santander.goldengate.handler;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroProducerConfigurationTest {

    @Test
    void appliesKafkaAvroSerializerToKeyAndValue() {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "incorrect.KeySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "incorrect.ValueSerializer");

        AvroProducerConfiguration.apply(props);

        assertEquals(AvroProducerConfiguration.KAFKA_AVRO_SERIALIZER,
                props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(AvroProducerConfiguration.KAFKA_AVRO_SERIALIZER,
                props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("true", props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", props.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals("5",
                props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        assertEquals(String.valueOf(Integer.MAX_VALUE),
                props.getProperty(ProducerConfig.RETRIES_CONFIG));
    }

    @Test
    void acceptsExplicitOrderingSafeConfiguration() {
        Properties props = safeDeliveryProperties();
        props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");

        AvroProducerConfiguration.apply(props);

        assertEquals("-1", props.getProperty(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    void rejectsDisabledIdempotence() {
        Properties props = safeDeliveryProperties();
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        assertThrows(IllegalArgumentException.class,
                () -> AvroProducerConfiguration.apply(props));
    }

    @Test
    void rejectsAcknowledgementsThatCanLoseMessages() {
        Properties props = safeDeliveryProperties();
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        assertThrows(IllegalArgumentException.class,
                () -> AvroProducerConfiguration.apply(props));
    }

    @Test
    void rejectsTooManyInFlightRequests() {
        Properties props = safeDeliveryProperties();
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");

        assertThrows(IllegalArgumentException.class,
                () -> AvroProducerConfiguration.apply(props));
    }

    @Test
    void rejectsDisabledRetries() {
        Properties props = safeDeliveryProperties();
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "0");

        assertThrows(IllegalArgumentException.class,
                () -> AvroProducerConfiguration.apply(props));
    }

    @Test
    void rejectsMalformedNumericGuarantees() {
        Properties props = safeDeliveryProperties();
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "many");

        assertThrows(IllegalArgumentException.class,
                () -> AvroProducerConfiguration.apply(props));
    }

    @Test
    void reliesOnSerializerRegistrationByDefault() {
        assertEquals(false,
                AvroProducerConfiguration.requiresExplicitSchemaRegistration(new Properties()));
    }

    @Test
    void requiresExplicitRegistrationWhenSerializerAutoRegistrationIsDisabled() {
        Properties props = new Properties();
        props.setProperty("auto.register.schemas", "false");

        assertTrue(AvroProducerConfiguration.requiresExplicitSchemaRegistration(props));
    }

    @Test
    void kafkaAvroSerializerAddsConfluentWireHeaderToGenericRecordKey() {
        Schema schema = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"TestKey\","
                        + "\"namespace\":\"key.test\",\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"long\"}]}" );
        GenericRecord key = new GenericData.Record(schema);
        key.put("id", 42L);

        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        Properties props = new Properties();
        props.put("schema.registry.url", "mock://key-wire-format-test");
        serializer.configure((java.util.Map) props, true);

        byte[] serialized = serializer.serialize("test-topic", key);
        serializer.close();

        assertTrue(serialized.length > 5);
        assertEquals(0, serialized[0], "Confluent magic byte");
        assertTrue(ByteBuffer.wrap(serialized, 1, 4).getInt() > 0, "Schema ID");
    }

    private Properties safeDeliveryProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        return props;
    }
}
