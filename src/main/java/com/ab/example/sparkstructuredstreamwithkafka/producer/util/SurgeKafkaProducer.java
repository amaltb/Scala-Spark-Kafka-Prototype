package com.ab.example.sparkstructuredstreamwithkafka.producer.util;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SurgeKafkaProducer {

    public static KafkaProducer<String, Request> getKafkaProducer(Properties kafkaParams) {

        final Properties props = new Properties();

        setUpBootStrapAndSerializers(props, kafkaParams);
        setUpBatchingAndCompression(props, kafkaParams);
        setUpRetriesInFlightTimeout(props, kafkaParams);

        /* Setting up number of acknowledgments to all (default value).
        *  This controls the number of in-sync replicas required for
        *  marking a write request as completed. Other options are 'Leader' and 'None'*/
        props.put(ProducerConfig.ACKS_CONFIG, kafkaParams.getOrDefault("acks", "all"));

        return new KafkaProducer<String, Request>(props);

    }

    private static void setUpRetriesInFlightTimeout(Properties props, Properties kafkaParams) {
        /*
            Maximum time the broker waits for confirmation from followers to send ack to the producer. (default 30)
         */
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_0000);

        /*
            Number of times the producer retries to send a message in case of failure. This also requires to set
            number of concurrent in_flight requests to 1 to maintain message order.
         */
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        /*
            For enabling retries.
         */
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);


        /*
            Retry only after one second in case of failure.
         */
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);
    }

    private static void setUpBatchingAndCompression(Properties props, Properties kafkaParams) {
        /*
            Linger up to 100 ms before sending batch if size is not met.
         */
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        /*
            By default batch up to 64K buffer size.
         */
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaParams.getOrDefault("batch.size", 16_384 * 4));

        /*
            Use snappy compression for batch compression.
         */
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    }

    private static void setUpBootStrapAndSerializers(Properties props, Properties kafkaParams) {

        /*
            Kafka broker details.
         */
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParams.get("bootstrap.servers"));

        /*
            Kafka client id.
         */
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaParams.getOrDefault("clinet.id", "KafkaClientProducer"));

        /*
            Setting key serializer as StringSerializer unless specified otherwise.
         */
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaParams.getOrDefault("key.class", StringSerializer.class.getName()));

        /*
            Setting value serializer as StringSerializer unless specified otherwise.
         */
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaParams.getOrDefault("value.class", StringSerializer.class.getName()));
    }

    public static void closeKafkaProducer(KafkaProducer producer) {
        producer.close(5, TimeUnit.MINUTES);
    }
}
