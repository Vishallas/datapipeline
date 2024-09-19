package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;


public class DataProcessor {
    private static final Logger log = LoggerFactory.getLogger(DataProcessor.class);
    private final MessageDigest md;
    private final SimpleDateFormat sdf1;
    private final long TIMEOUT = 5 * 60 * 1000;

    DataProcessor() throws NoSuchAlgorithmException {
        this.md = MessageDigest.getInstance("md5");
        this.sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 2011-02-03 04:05:00
    }

    private class Event {
        private String uuid;
        private String url;
        private String time;

        public String getUuid() {
            return uuid;
        }
        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getUrl() {
            return url;
        }
        public void setUrl(String url) {
            this.url = url;
        }

        public String getTime() {
            return time;
        }
        public void setTime(String time) {
            this.time = time;
        }

        Event(String uuid, String url, String time){
            setUuid(uuid);
            setUrl(url);
            setTime(time);
        }
    }

    private class Session {
        private String uuid;
        private String firstEventTime;
        private JSONArray session = null;

        Session(String uuid){
            this.setUuid(uuid);
        }

        public void addMeta(Event event){
            if (session == null){
                this.session = new JSONArray();
                setFirstEventTime(event.getTime());
            }
            JSONObject meta = new JSONObject();
            meta.put("url", event.getUrl());
            meta.put("time", event.getTime());
            this.session.put(meta);
        }

        public String getJsonString(){
            JSONObject batch = new JSONObject();
            batch.put("uuid", getUuid());
            batch.put("first_event_time", getFirstEventTime());
            batch.put("events", getSession());
            return batch.toString();
        }

        private String getFirstEventTime() {
            return firstEventTime;
        }

        private void setFirstEventTime(String firstEventTime) {
            this.firstEventTime = firstEventTime;
        }

        private void setUuid(String uuid){
            this.uuid = uuid;
        }
        private String getUuid(){
            return this.uuid;
        }
        private JSONArray getSession(){
            return this.session;
        }

    }

    private String md5UUID(String ip, String userAgent){

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
    private void sendToKafka(KafkaProducer<String, String> kafkaProducer, String TOPIC, String data){
        kafkaProducer.send(new ProducerRecord<>(TOPIC, data)
        , (recordMetadata, e) -> {
                    if (e == null) {
                        log.info(" [+] RECORD SENT TO TOPIC {}, PARTITION {}, OFFSET {}, TIMESTAMP {}",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        log.error(" [x] ERROR OCCURRED TOPIC {}, PARTITION {}, OFFSET {}, TIMESTAMP {}",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp());
                    }
                }
        );
    }
    private int processUsers(KafkaProducer<String, String> kafkaProducer, String TOPIC, Iterator<Event> users, long userCount) throws ParseException {
        Date previousEventTime = null;

        Session batch = null;
        int session = 0;
        while(users.hasNext()){
            Event event = users.next();
            Date currentEventTime = sdf1.parse(event.getTime());
            if(previousEventTime == null){
                batch = new Session(event.getUuid());
            }else if(currentEventTime.getTime() - previousEventTime.getTime() > TIMEOUT){
                session++;
                sendToKafka(kafkaProducer, TOPIC, batch.getJsonString());
                batch = new Session(event.getUuid());
            }
            batch.addMeta(event);
            previousEventTime = currentEventTime;
        }
        if (batch!=null){
            session++;
            sendToKafka(kafkaProducer, TOPIC, batch.getJsonString());
        }
//        log.info("[{}] Processing user {} with session {}.", userCount,batch.getUuid(), session);
        return session;
    }

    private Event getRecordFromLine(String line) {
        Event values;
        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter("\\|"); // Pipe deliminator
            String ip = rowScanner.next();
            String userAgent = rowScanner.next();
            String url = rowScanner.next();
            String time = rowScanner.next();
//            System.out.println(ip + " " + url + " " + time);
            values = new Event(md5UUID(ip, userAgent), url, time);

        }
        return values;
    }
    private KafkaProducer<String, String> initializeKafka(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public void processFile(String fileName){
        final String TOPIC = "hadoop_data_1";
        long totalUsers = 0l;
        long offset = 0l;
        try (Scanner scanner = new Scanner(new File(fileName))) {
            String uuid = null;
            List<Event> users = null;
            try(KafkaProducer<String, String> kafkaProducer = initializeKafka()) {
                while (scanner.hasNextLine()) {

                    Event data = getRecordFromLine(scanner.nextLine());
                    if (!data.getUuid().equals(uuid)) { // new user

                        // For first line
                        if (users != null) {
                            totalUsers++;
                            offset += processUsers(kafkaProducer, TOPIC, users.iterator(), totalUsers); //processing users
                        }
                        // Common
                        uuid = data.getUuid();
                        users = new ArrayList<>();
                    }
                    users.add(data);
                }
                // remaining users
                if(!users.isEmpty()){
                    totalUsers++;
                    offset += processUsers(kafkaProducer, TOPIC, users.iterator(), totalUsers); // Processign users
                }
                log.info("[total] Users count = {} with practical offset {}", totalUsers, offset);
            }
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }
}
