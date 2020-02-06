package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class RequestSender implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestSender.class);

    private KafkaProducer producer;
    private String topic;

    public RequestSender(KafkaProducer producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void run() {
        int sentCount = 0;
        while (true)
        {
            sentCount ++;
            ProducerRecord<String, Request> msgToSend = null;
            if(topic.equals(Constants.DEMAND_TOPIC))
            {
                msgToSend = createRandomCarDemandMessage();
            }
            else if(topic.equals(Constants.SUPPLY_TOPIC))
            {
                msgToSend = createRandomCarSupplyMessage();
            }
            try{
                final Future<RecordMetadata> future = producer.send(msgToSend);
                if (sentCount % 100 == 0) {
                    displayRecordMetaData(msgToSend, future);
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                if(Thread.interrupted())
                {
                    break;
                }
            } catch (ExecutionException e) {
                LOGGER.warn("Unable to send record to broker.", e);
            }
        }
    }

    private ProducerRecord<String,Request> createRandomCarSupplyMessage() {
        Double randomLatValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LAT, Constants.GEO_LOC_END_LAT);
        Double randomLongValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LONG, Constants.GEO_LOC_END_LONG);
        int carId = ThreadLocalRandom.current().nextInt(1, 101);

        final Request val =  new Request(carId, randomLatValue, randomLongValue);
        final String key = new Timestamp(System.currentTimeMillis()).toString();

        return new ProducerRecord<>(topic, key, val);
    }

    private ProducerRecord<String, Request> createRandomCarDemandMessage() {
        Double randomLatValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LAT, Constants.GEO_LOC_END_LAT);
        Double randomLongValue = ThreadLocalRandom.current()
                .nextDouble(Constants.GEO_LOC_START_LONG, Constants.GEO_LOC_END_LONG);
        int customerId = ThreadLocalRandom.current().nextInt(1, 1001);

        final Request val =  new Request(customerId, randomLatValue, randomLongValue);
        final String key = new Timestamp(System.currentTimeMillis()).toString();

        return new ProducerRecord<>(topic, key, val);
    }

    private void displayRecordMetaData(ProducerRecord<String,Request> record, Future<RecordMetadata> future)
            throws ExecutionException, InterruptedException {
        final RecordMetadata recordMetadata = future.get();
        LOGGER.info(String.format("\n\t\t\tkey=%s, value=%s " +
                        "\n\t\t\tsent to topic=%s part=%d off=%d at time=%s",
                record.key(),
                record.value().toString(),
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                new Date(recordMetadata.timestamp())
        ));
    }
}
