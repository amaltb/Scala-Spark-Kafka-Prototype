package com.ab.example.sparkstructuredstreamwithkafka.producer.util;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigException;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static com.ab.example.sparkstructuredstreamwithkafka.producer.util.ApplicationUtil.getKafkaSerializer;

public class ObjectFactory {

    private static List<KafkaProducerWrapper> kafkaProducerList = new LinkedList<>();

    public static synchronized <K, V> Producer getORCreateKafkaProducer(Properties kafkaParams, Class<K> keyClass, Class<V> valueClass) {
        kafkaParams.put("key.serializer", getKafkaSerializer(keyClass.getSimpleName()));
        kafkaParams.put("value.serializer", getKafkaSerializer(valueClass.getSimpleName()));

        for (KafkaProducerWrapper producerWrapper : kafkaProducerList) {
            if (producerWrapper.getProducerParams().equals(kafkaParams))
            {
                if(producerWrapper.isActive())
                {
                    return producerWrapper.getProducer();
                }
            }
        }

        try {
            Producer kafkaProducer = new KafkaProducer<K, V>(kafkaParams);
            kafkaProducerList.add(new KafkaProducerWrapper(kafkaProducer, kafkaParams));
            return kafkaProducer;
        } catch (ConfigException e) {
            throw new RuntimeException("Unable to create kafka producer.", e);
        }
    }

    public static void closeKafkaProducers() {
        kafkaProducerList.forEach(kafkaProducerWrapper -> kafkaProducerWrapper.getProducer().close());
    }
}
