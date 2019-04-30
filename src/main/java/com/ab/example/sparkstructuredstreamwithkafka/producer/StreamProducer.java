package com.ab.example.sparkstructuredstreamwithkafka.producer;

import com.ab.example.sparkstructuredstreamwithkafka.producer.util.ApplicationUtil;
import com.ab.example.sparkstructuredstreamwithkafka.producer.util.Constants;
import com.ab.example.sparkstructuredstreamwithkafka.producer.util.LogFactory;
import com.ab.example.sparkstructuredstreamwithkafka.producer.util.ObjectFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Properties;

import static com.ab.example.sparkstructuredstreamwithkafka.producer.util.ApplicationUtil.sendToKafkaTopic;

/**
 * Class implementing runnable interface to create a thread generating random demand requests to kafka in every second.
 */
class CarDemandThread implements Runnable {

    private static final Logger LOGGER = LogFactory.getLogger(Level.DEBUG);

    private Properties kafkaParams;
    private String kafkaTopic;

    CarDemandThread(Properties kafkaParams, String kafkaTopic) {
        this.kafkaParams = kafkaParams;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void run() {
        try {
            final Producer producer = ObjectFactory.getORCreateKafkaProducer(kafkaParams, String.class, String.class);

            while (true) {
                String msgToSend = ApplicationUtil.generateCarDemandMessage();
                String key = new Timestamp(System.currentTimeMillis()).toString();


                sendToKafkaTopic(producer, key, msgToSend, kafkaTopic);
                Thread.sleep(1000);

                if (Thread.interrupted()) {
                    producer.close();
                    LOGGER.info("Stopping car supply generator due to interrupt...");
                    break;
                }
            }
        } catch (RuntimeException e) {
            LOGGER.error(String.format("Failed to send message to kafka topic %s. \nDetails :%s", kafkaTopic, e));
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while sleeping...");
        }
    }
}

/**
 * Class implementing runnable interface to create a thread generating random supply requests to kafka in every second.
 *
 */
class CarSupplyThread implements Runnable {

    private static final Logger LOGGER = LogFactory.getLogger(Level.DEBUG);

    private Properties kafkaParams;
    private String kafkaTopic;

    CarSupplyThread(Properties kafkaParams, String kafkaTopic) {
        this.kafkaParams = kafkaParams;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void run() {
        try {
            final Producer producer = ObjectFactory.getORCreateKafkaProducer(kafkaParams, String.class, String.class);
            while (true) {
                String msgToSend = ApplicationUtil.generateCarSupplyMessage();
                String key = new Timestamp(System.currentTimeMillis()).toString();


                sendToKafkaTopic(producer, key, msgToSend, kafkaTopic);
                Thread.sleep(1000);

                if (Thread.interrupted()) {
                    producer.close();
                    LOGGER.info("Stopping car supply generator due to interrupt...");
                    break;
                }
            }
        } catch (RuntimeException e) {
            LOGGER.error(String.format("Failed to send message to kafka topic %s. \nDetails :%s", kafkaTopic, e));
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while sleeping...");
        }
    }
}

/**
 * StreamProducer application generates supply & demand requests to kafka
 */
public class StreamProducer {

    private static final Logger LOGGER = LogFactory.getLogger(Level.DEBUG);

    /**
     * application run method.
     *
     * @param args
     */
    private static void run(String[] args) {
        LOGGER.info("Starting application... running stream producer...");
        final Properties props = new Properties();
        try {
            props.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            LOGGER.error("Could not load kafka parameters.. Check kafka parameters file. \nDetails: " + e);
        }

        Thread t1 = new Thread(new CarDemandThread(props, Constants.DEMAND_TOPIC));
        Thread t2 = new Thread(new CarSupplyThread(props, Constants.SUPPLY_TOPIC));

        try {
            t1.start();
            t2.start();

            t1.join();
            t2.join();
        } catch (Exception e) {
            LOGGER.error("Terminating execution due to exception. \nDetails: " + e);
        } finally {
            t1.interrupt();
            t2.interrupt();
            ObjectFactory.closeKafkaProducers();
        }
    }

    /**
     * entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            LOGGER.error("StreamProducer is expecting following arguments\n" +
                    "1. Kafka configuration properties file");
            return;
        }

        run(args);
    }
}
