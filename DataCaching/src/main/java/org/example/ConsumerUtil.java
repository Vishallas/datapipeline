package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.json.JSONObject;
import org.json.JSONTokener;
import redis.clients.jedis.Jedis;

class ConsumerUtil implements Runnable{

    private final KafkaConsumer<Integer, String> consumer;


    ConsumerUtil(int partition, String topic){

        final String GROUP_ID = "hadoop_data_group_1";
        final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092";

        // Property Initialization
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);

        this.consumer.subscribe(Collections.singletonList(topic)); // Using immutable array
    }

    public void run(){

        final String REDIS_ADDR = "localhost";
        final int REDIS_PORT = 6379;

        try (Jedis jedis = new Jedis(REDIS_ADDR, REDIS_PORT)) {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<Integer, String> record : records) {
                    JSONObject json = new JSONObject(new JSONTokener(record.toString()));

                }
//                break;
            }
        }
    }
}
