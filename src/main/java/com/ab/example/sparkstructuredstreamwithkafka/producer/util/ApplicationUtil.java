package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Locale;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class ApplicationUtil {

    public static String generateCarSupplyMessage() {
        // generating random driver location inside BLR city
        Double randomLatValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LAT, Constants.GEO_LOC_END_LAT);
        Double randomLongValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LONG, Constants.GEO_LOC_END_LONG);
        int carId = ThreadLocalRandom.current().nextInt(1, 101);

        return String.valueOf(carId) + "," + randomLatValue + "," + randomLongValue;
    }

    public static String generateCarDemandMessage() {
        // generating random customer request inside BLR city
        Double randomLatValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LAT, Constants.GEO_LOC_END_LAT);
        Double randomLongValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LONG, Constants.GEO_LOC_END_LONG);
        int customerId = ThreadLocalRandom.current().nextInt(1, 1001);

        return String.valueOf(customerId) + "," + randomLatValue + "," + randomLongValue;
    }

    public static void sendToKafkaTopic(Producer prod, String key, String value, String topic) {
        sendWithRetries(prod, key, value, topic, 0);
    }

    private static void sendWithRetries(Producer prod, String key, String value, String topic, int attempt_no)
    {
        Future stat = prod.send(new ProducerRecord<>(topic, key, value), (metadata, e) -> {
            if (e != null)
            {
                int attempt = attempt_no + 1;
                if(attempt < Constants.MAX_KAFKA_PRODUCER_RETRIES)
                    sendWithRetries(prod, key, value, topic, attempt);
                else
                    throw new RuntimeException("Failed to send the message to kafka due to " + e);
            }
            else {
                System.out.println("The offset of the record we just sent is: " + metadata.offset());
            }
        });
    }
}
