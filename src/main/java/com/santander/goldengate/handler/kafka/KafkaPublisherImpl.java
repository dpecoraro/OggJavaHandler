package com.santander.goldengate.handler.kafka;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Default KafkaPublisher implementation.
 */
public class KafkaPublisherImpl implements KafkaPublisher {

    private KafkaProducer<String, byte[]> producer;
    private String bootstrapServers;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void init(Properties props) throws Exception {
        Objects.requireNonNull(props, "props");
        // Force serializers if missing
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        bootstrapServers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producer = new KafkaProducer<>(props);
        System.out.println(">>> [KafkaPublisherImpl] Initialized producer bootstrap=" + bootstrapServers);
    }

    @Override
    public void send(String topic, String key, byte[] payload) throws Exception {
        if (closed.get()) {
            throw new IllegalStateException("Producer is closed");
        }
        if (producer == null) {
            throw new IllegalStateException("Producer not initialized");
        }
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload);
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                System.err.println("[KafkaPublisherImpl] Send error topic=" + topic + " key=" + key + " msg=" + ex.getMessage());
            } else {
                System.out.println(">>> [KafkaPublisherImpl] Sent topic=" + md.topic() + " partition=" + md.partition() + " offset=" + md.offset());
            }
        });
    }

    @Override
    public void flushClose() {
        if (producer == null || !closed.compareAndSet(false, true)) return;
        try {
            producer.flush();
        } catch (Exception ignore) {}
        try {
            producer.close();
        } catch (Exception ignore) {}
        System.out.println(">>> [KafkaPublisherImpl] Producer closed");
    }
}
