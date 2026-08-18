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
}
