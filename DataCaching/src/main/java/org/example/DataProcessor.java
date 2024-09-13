package org.example;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
        long offset = -1;
        for(ConsumerRecord record : records) {
            JSONObject jsonBatch = new JSONObject(new JSONTokener(record.value().toString()));
            Session session = new Session();
            session.setUuid((String) jsonBatch.get("uuid"));
            jsonBatch.put("offset", record.offset());
            jsonBatch.remove("uuid");
            session.setBatch(jsonBatch);
            batch.add(session);
            offset = record.offset();
        }
        if(offset!=-1)
            log.info("[*] End offset {}",offset);
        return batch;
    }


    public void processTopic(String topic) throws InterruptedException {
        final String REDIS_ADDR = "localhost";
        final int REDIS_PORT = 6379;

        try (KafkaConsumer<String, String> consumer = initializeKafka();
             Jedis jedis = new Jedis(REDIS_ADDR, REDIS_PORT);) {
            consumer.assign(Collections.singletonList(new TopicPartition(topic, 0)));
            ConsumerRecords<String, String> records = null;

            long userCount = 0;
            String prevUuid = null;

            while(true){
                records = consumer.poll(Duration.ofMillis(500));

                Iterator<Session> sessions = processToJson(records).iterator();
                while (sessions.hasNext()){
                    Session session = sessions.next();
                    if(prevUuid == null){ // first session of the poll
                        prevUuid = session.getUuid(); // Assigning value to uuid
                    } else if(!session.getUuid().equals(prevUuid)) { // not a previous user
                        jedis.rpush("uuids", "uuid:" + prevUuid);
//                        jedis.sadd("test", "uuid:"+prevUuid);
                        prevUuid = session.getUuid();
                        userCount++;
                    }
                    jedis.rpush("uuid:" + session.getUuid(), session.getBatch().toString());

                }
                if(records.count() == 0 && prevUuid != null) {
                    jedis.rpush("uuids", "uuid:" + prevUuid);
                    prevUuid = null;
                    userCount++;
                }
                log.info("Total user counts {}", userCount);
                log.info("[-] Waiting for 5 seconds");
//                log.info("[0] Record Count {}.", records.count());
//                Thread.sleep(5000);
            }//while (!records.isEmpty() && records != null);
//
        }
    }
}

