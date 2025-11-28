package com.santander.goldengate.handler.kafka;

import java.util.Properties;

public interface KafkaPublisher {
    void init(Properties props) throws Exception;
    void send(String topic, String key, byte[] payload) throws Exception;
    void flushClose();
}
