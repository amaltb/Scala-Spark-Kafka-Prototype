package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

import org.apache.kafka.clients.producer.KafkaProducer;

public class MetricsProducerReporter implements Runnable {
    private final KafkaProducer producer;

    public MetricsProducerReporter(KafkaProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        // TODO implement metrics reporter.
    }
}
