package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.awt.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

public class ConsumerWorker {
    private static final int THREAD_COUNT = 5;
    private static final String TOPIC = "hadoop_data";

    public static void main(String[] args){

        final Logger log = LoggerFactory.getLogger(Consumer.class);

        Thread[] threads = new Thread[THREAD_COUNT];

        for(int i = 0;i<THREAD_COUNT;i++){
            threads[i] = new Thread(new ConsumerUtil(log, i, TOPIC));
            threads[i].start();
        }

        try {
            for (Thread t : threads)
                t.join();
        } catch (InterruptedException e) {
        }
    }
}

class ConsumerUtil implements Runnable{

    private static final String GROUP_ID = "hadoop_data_group_1";

    final KafkaConsumer<Integer, String> consumer;
    private final int partition;
    private final Logger log;
    ConsumerUtil(Logger log, int partition, String topic){

        // Property Initialization
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);
        this.log = log;
        this.partition = partition;
        this.consumer.subscribe(Arrays.asList(topic));
    }

    public void run(){
        while (true){
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));

            for(ConsumerRecord<Integer, String> record : records){
                log.info("[+] Received new record " +
                        "Key "+ record.key() +
                        " Value "+ record.value() +
                        " Topic " + record.topic() +
                        "Partition "+record.partition() +
                        " Offset " + record.offset()
                );
            }

            //TODO redis processing
        }
    }
}
