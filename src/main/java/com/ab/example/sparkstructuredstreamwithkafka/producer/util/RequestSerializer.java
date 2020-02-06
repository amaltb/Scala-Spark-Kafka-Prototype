package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RequestSerializer implements Serializer<Request>{
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Request request) {
        return request.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
}
