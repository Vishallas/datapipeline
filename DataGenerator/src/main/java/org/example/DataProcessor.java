package org.example;

import org.apache.avro.JsonProperties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.awt.*;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
        this.sdf1 = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
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

    private class Batch{
        private String uuid;
        private JSONArray session;

        Batch(String uuid){
            this.setUuid(uuid);
            this.session = new JSONArray();
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

        public void addMeta(Event event){
            JSONObject meta = new JSONObject();
            meta.put("url", event.getUrl());
            meta.put("time", event.getTime());
            this.session.put(meta);
        }

        public String getJsonString(){
            JSONObject batch = new JSONObject();
            batch.put("uuid", getUuid());
            batch.put("events", getSession());
            return batch.toString();
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

    private void processBatch(KafkaProducer<String, String> kafkaProducer, String TOPIC, Iterator<Event> records) throws ParseException {
        Date previousEventTime = null;

        Batch batch = null;

        while(records.hasNext()){
            Event event = records.next();
            Date currentEventTime = sdf1.parse(event.getTime());

            // First event or No timeout
            if(previousEventTime == null || currentEventTime.getTime() - previousEventTime.getTime() <= TIMEOUT){

                // Creating Batch Object for first event
                if (previousEventTime == null)
                    batch = new Batch(event.getUuid());

                // Adding Event to the session
                batch.addMeta(event);

            }else {
                // Session before timeout
                kafkaProducer.send(new ProducerRecord<>(TOPIC, batch.getJsonString()), (recordMetadata, e) -> {
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

                // Initializing for new batch
                batch = new Batch(event.getUuid());
                // Adding first meta
                batch.addMeta(event);
            }

            // Updating to new time
            previousEventTime = currentEventTime;
        }
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
        final String TOPIC = "hadoop_data";
        try (Scanner scanner = new Scanner(new File(fileName))) {
            String uuid = null;
            List<Event> records = null;
            try(KafkaProducer<String, String> kafkaProducer = initializeKafka()) {
                while (scanner.hasNextLine()) {
                    Event data = getRecordFromLine(scanner.nextLine());
                    if (uuid == null || data.getUuid().equals(uuid)) {
                        if (uuid == null) {
                            records = new ArrayList<>();
                            uuid = data.getUuid();
                        }
                        records.add(data);
                    } else { // first line or new user data

                        // New User
                        if (records != null)
                            processBatch(kafkaProducer, TOPIC, records.iterator());

                        // Common
                        uuid = data.getUuid();
                        records = new ArrayList<>();
                    }
                }
            }
        } catch (FileNotFoundException | ParseException e) {

            throw new RuntimeException(e);
        }
    }
}
