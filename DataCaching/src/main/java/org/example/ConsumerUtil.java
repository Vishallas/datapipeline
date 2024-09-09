package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;

import org.json.JSONObject;
import org.json.JSONTokener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

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

    public void run(){
        final String GROUP_ID = "hadoop_data_group_1";
        final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092";
        final String AVRO_SCHEMA_PATH = "/home/vishal-pt7653/Documents/Project-assignment/datapipeline/DataCaching/src/main/avro/EventSchema.avsc";

        // Property Initialization
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty("avro.schema.path", AVRO_SCHEMA_PATH);

        try {
            MessageDigest md = MessageDigest.getInstance("md5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        final String REDIS_ADDR = "localhost";
        final int REDIS_PORT = 6379;
        final String TOPIC = "hadoop_data";

        try( KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             Jedis jedis = new Jedis(REDIS_ADDR, REDIS_PORT);) {


//            while (true) {
//                consumer.subscribe(Collections.singletonList(TOPIC));
//                Thread.sleep(1000);
//                ConsumerRecords<String, String > records = consumer.poll(Duration.ofMillis(200));
//
//                for (ConsumerRecord<String , String> record : records) {
//                    GenericRecord event = record.value();
//                    String uuid = md5UUID(md,event.get("ip").toString(), event.get("user_agent").toString());
//                    jedis.watch(uuid);
//                    Map<String,String> userSession = jedis.hgetAll(uuid);
//
//                    if(userSession != null){
//                        if(sessionTimeOut(userSession.get("last_event"), event.get("event_time"))){
//                            userSession.put("visit_count", incr(userSession.get("visit_count")));
//                            userSession.put("last_event", event.get("event_time").toString());
//                        }else {
//                            userSession = new HashMap<String, String>() {{
//                                put("uuid", uuid);
//                                put("visit_count", "1");
//                                put("last_visit", event.get("event_time").toString());
//                                put("first_event", event.get("event_time").toString());
//                                put("url", event.get("url").toString());
//                            }};
//                            Transaction transc = jedis.multi();
//                            transc.hset(uuid, userSession);
//                            List<Object> success = transc.exec();
//                        }
//                    }else{
//                        jedis.unwatch();
//                    }
//                    jedis.lpush(avroEncode(event));
//                }
//                consumer.commitSync();
//            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
