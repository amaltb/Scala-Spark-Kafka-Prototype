package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

class KafkaProducerWrapper {

    private Producer producer;
    private Properties producerParams;
    private boolean isActive;

    KafkaProducerWrapper(Producer producer, Properties properties) {
        this.producer = producer;
        this.producerParams = properties;
        this.isActive = true;
    }

    Producer getProducer() {
        return producer;
    }

    Properties getProducerParams() {
        return producerParams;
    }

    boolean isActive() {
        return isActive;
    }
}
