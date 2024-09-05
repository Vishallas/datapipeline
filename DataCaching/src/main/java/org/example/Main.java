package org.example;


import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

//import redis.clients.jedis.Jedis;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;


public class Main {

    private static final String TOPIC = "hadoop_data";

    private static String md5UUID(MessageDigest md, String ip, String userAgent){

        String input = ip+userAgent;
        byte[] hashBytes = md.digest(input.getBytes());

        // Create a UUID from the first 16 bytes of the MD5 hash
        long mostSigBits = 0;
        long leastSigBits = 0;

        for (int i = 0; i < 8; i++) {
            mostSigBits |= ((long) (hashBytes[i] & 0xff)) << (8 * (7 - i));
        }
        for (int i = 8; i < 16; i++) {
            leastSigBits |= ((long) (hashBytes[i] & 0xff)) << (8 * (15 - i));
        }

        return (new UUID(mostSigBits, leastSigBits)).toString();
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {

        final String GROUP_ID = "hadoop_data_group_1";
        final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092";
        final String AVRO_SCHEMA_PATH = "/home/vishal-pt7653/Documents/Project-assignment/datapipeline/DataCaching/src/main/avro/EventSchema.avsc";

        // Property Initialization
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomAvroDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty("avro.schema.path", AVRO_SCHEMA_PATH);

        MessageDigest md = MessageDigest.getInstance("md5");

        try( KafkaConsumer<Void, EventSchema> consumer = new KafkaConsumer<>(props)) {

            while (true) {
                consumer.subscribe(Collections.singletonList(TOPIC));
                Thread.sleep(1000);
                ConsumerRecords<Void, EventSchema> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<Void, EventSchema> record : records) {
                    GenericRecord event = record.value();
                    System.out.println("UUID : " + md5UUID(md,event.get("ip").toString(), event.get("user_agent").toString())+ "IP : " +  event.get("ip") + " Event Time : "+ event.get("event_time").toString());
                }
            }

//            final String REDIS_ADDR = "localhost";
//            final int REDIS_PORT = 6379;
//
//            try (Jedis jedis = new Jedis(REDIS_ADDR, REDIS_PORT)) {
//                while (true) {
//                    ConsumerRecords<Void, EventSchema> records = consumer.poll(Duration.ofMillis(200));
//                    for (ConsumerRecord<Void, EventSchema> record : records) {
//                        String uuid =
//                    }
//                }
//            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}






