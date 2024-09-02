package org.example;


import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Main {
    static final int THREAD_COUNT = 5;
    static final String TOPIC = "hadoop_data";

    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(Main.class);
        log.info("Logger initialized");

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Thread[] threads = new Thread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i] = new Thread(new ProducerUtil(prop,TOPIC, i));
            log.info("[ ] Thread " + i + " Created.");
            threads[i].start();
            log.info("[ ] Thread " + i + " Started.");
        }

        try {
            for (Thread t : threads)
                t.join();
        } catch (InterruptedException e) {
            log.error("Exception : ", e);
        } finally {
            log.info("Closing Producer");
        }
    }
}