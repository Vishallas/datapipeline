package org.example;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.*;

public class DataProcessor {
    private static final Logger log = LoggerFactory.getLogger(DataProcessor.class);
    final String GROUP_ID = "hadoop_data_group_2";
    final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092";

    private KafkaConsumer<String, String> initializeKafka(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        return new KafkaConsumer<>(properties);
    }
    private class Session{
        public String getUuid() {
            return uuid;
        }
        public void setUuid(String uuid) {
            this.uuid = uuid;
        }
        public JSONObject getBatch() {
            return batch;
        }
        public void setBatch(JSONObject batch) {
            this.batch = batch;
        }

        private String uuid;
        private JSONObject batch;

    }
    private List<Session> processToJson(ConsumerRecords<String, String> records){
        List<Session> batch = new ArrayList<>();
        for(ConsumerRecord record : records) {
            JSONObject jsonBatch = new JSONObject(new JSONTokener(record.value().toString()));
            Session session = new Session();
            session.setUuid((String) jsonBatch.get("uuid"));
            jsonBatch.put("offset", record.offset());
            jsonBatch.remove("uuid");
            session.setBatch(jsonBatch);
            batch.add(session);
        }
        return batch;
    }


    public void processTopic(String topic) throws InterruptedException {
        final String REDIS_ADDR = "localhost";
        final int REDIS_PORT = 6379;

        try (KafkaConsumer<String, String> consumer = initializeKafka();
             Jedis jedis = new Jedis(REDIS_ADDR, REDIS_PORT);) {

            while (true) {
                consumer.subscribe(Collections.singletonList(topic));
                Thread.sleep(5000);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                String uuid = null;
                List<Session> sessions = processToJson(records);
                for ( Session session : sessions) {
                    if (uuid == null){
                        uuid = session.getUuid();
                    }else if(!uuid.equals(session.getUuid())){
                        jedis.lpush("uuids", "uuid:" + uuid);
                        log.info("[+] Processed uuid {}",uuid);
                        uuid = session.getUuid();
                    }else{
                        jedis.lpush("uuid:"+session.getUuid(), session.getBatch().toString());
                    }
                }
            }
        }
    }
}

