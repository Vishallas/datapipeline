package org.example;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.json.JSONObject;

import java.util.*;

class RandomStringGenerator {
    public static String getData(){

        // Data Formate and order

        //    uuid | visits | sid | eventtime | uvid | ip | user_agent | pageurl_trim


        JSONObject data = new JSONObject();

        data.put("uuid","000028e1-3e58-498a-8f4e-67a3fa7dbe89_d587"); // UUID
        data.put("visits",1); // Visit Count
        data.put("sid","-8.96494E+018");
        data.put("eventtime","2024-08-31 00:42:48"); // Event Time
        data.put("uvid","cb07f9bd-8dc1-4096-a657-6f19008f2174-2"); //UVID
        data.put("ip","182.65.217.47"); //IP
        data.put("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36");
        data.put("pageurl_trim","zoho.com/mail/"); //pageurl_trim

        return data.toString();

    }
}

public class ProducerUtil implements Runnable {
    final String topicName = "hadoop_data";
    final int key;
    ProducerUtil (int key){
        this.key = key;
    }

    @Override
    public void run() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        Logger log = LoggerFactory.getLogger(ProducerUtil.class);
        try(KafkaProducer<Void, String> producer = new KafkaProducer<>(prop)) {
            while (true) {
                Thread.sleep(2000);
                String data = RandomStringGenerator.getData();

                ProducerRecord<Void, String> producerRecord = new ProducerRecord<>(topicName, key, null,data);
                producer.send(producerRecord, (recordMetadata, e) ->
                        log.info("Published to " + recordMetadata.topic()
                                + ", Key " + key
                                + ", Partition " + recordMetadata.partition()
                                + ", timestamp " + recordMetadata.timestamp()
                        )
                );
            }
        } catch (Exception e) {
            log.error("Error occured ", e);
            throw new RuntimeException(e);
        }
    }
}