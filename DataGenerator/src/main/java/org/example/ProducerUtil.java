package org.example;

import com.opencsv.CSVWriter;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.text.SimpleDateFormat;

import java.io.IOException;
import java.io.FileWriter;



class RandomStringGenerator {
    private final MessageDigest md;
    private final DateTimeFormatter formatter;
    private final String[] userAgentList;
    private final String[] urlList;
    private final Random random;
    private Instant currentTime= null;
    private final ZoneId zoneId;

    RandomStringGenerator() throws NoSuchAlgorithmException {
        this.md = MessageDigest.getInstance("md5");
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        this.userAgentList = assignUserAgent();
        this.urlList = assignUrl();
        this.random =new Random();
        this.zoneId = ZoneId.of("UTC");
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

    private String[] assignUserAgent(){
        return new String[] {
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.94 Chrome/37.0.2062.94 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko"
                ,"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9"
                ,"Mozilla/5.0 (iPad; CPU OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H321 Safari/600.1.4"
                ,"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.10240"
                ,"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko"
                ,"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko"
                ,"Mozilla/5.0 (Windows NT 10.0; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/7.1.8 Safari/537.85.17"
                ,"Mozilla/5.0 (iPad; CPU OS 8_4 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H143 Safari/600.1.4"
                ,"Mozilla/5.0 (iPad; CPU OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12F69 Safari/600.1.4"
            };
    }

    private String[] assignUrl(){
        return new String[] {
                "zoho.com/phonebridge/developer/v3/call-control.html",
                "zoho.com/in/inventory/inventory-dictionary/unit-of-measure.html",
                "zoho.com/expense/#/trips/3260118000001599155",
                "help.zoho.com/portal/en/kb/people/search/leave carry",
                "help.zoho.com/portal/en/kb/people/zoho-people-4-0/employee-handbook-zoho-people-4-0/employee-leave/articles/zoho-people-employee-leave",
                "help.zoho.com/portal/en/kb/people/administrator-guide/leave/overview/articles/leave-service-overview#What_is_Leave_Service_in_Zoho_People",
                "help.zoho.com/portal/en/kb/people/administrator-guide/introduction/articles/what-is-zoho-people#Services_and_features_of_Zoho_People",
                "zoho.com/in/books/help/transaction-approval/#approving",
                "zoho.com/sheet/analyze.html",
                "zoho.com/ipr-complaints.html",
                "zoho.com/abuse-policy/",
                "zoho.com/commerce/index1.html",
                "store.zoho.in/html/store/index.html#subscription",
                "zoho.com/id/forms/",
                "help.zoho.com/portal/en/kb/vault/user-guide/sharing-passwords/articles/vault-add-passwords-notes-documents#Creating_custom_categories",
                "zoho.com/ae/books/pricing/pricing-comparison.html",
                "help.zoho.com/portal/en/kb/people/administrator-guide/attendance-management/settings/articles/breaks#Configuring_breaks",
                "zoho.com/in/expense/#/expensereports/1471609000000640099",
                "zoho.com/projects/project-milestones.html",
                "help.zoho.com/portal/en/kb/workdrive/migrations/google/articles/migrate-from-g-suite-drive-to-zoho-workdrive#i_Create_your_own_Google_Cloud_app"
        };

    }
    private String randUserAgent(){
        int randomNumber= random.nextInt(userAgentList.length);
        return userAgentList[randomNumber];
    }

    private String randUrl(){
        int randomNumber = random.nextInt(urlList.length);
        return urlList[randomNumber];
    }

    private String ipGenerator() {

        Random random = new Random();

        // Generate each of the 4 octets
        int firstOctet = 180;
        int secondOctet = 0;
        int thirdOctet = 1; //random.nextInt(256);
        int fourthOctet = random.nextInt(256); // 0 - 255

        return String.format("%d.%d.%d.%d", firstOctet, secondOctet, thirdOctet, fourthOctet);
    }

    private String getTime(){
        if(this.currentTime == null){
            this.currentTime = Instant.now();
        }else {
            long maxIntervalMillis = 7 * 60 * 1000;
            // Minimum interval in milliseconds
            long minIntervalMillis = 1000;
            long intervalMillis = minIntervalMillis + (long) (random.nextDouble() * (maxIntervalMillis - minIntervalMillis));
            this.currentTime = this.currentTime.plusMillis(intervalMillis);
        }

        return formatter.format(currentTime.atZone(zoneId));
    }

//    public EventSchema getAvroEvent(){
//
//        EventSchema eventObj = new EventSchema();
//        eventObj.setIp(ipGenerator()); //IP
//        eventObj.setUserAgent(randUserAgent()); // user agent
//        eventObj.setUrl(randUrl()); // event url
//        eventObj.setEventTime(getTime()); // sequence time
//
//        return eventObj;
//    }
//
//    public EventSchemaV1 getAvroEventV1(int n){
//        EventSchemaV1 eventObj = new EventSchemaV1();
//        String ip = ipGenerator();
//        String userAgent = randUserAgent();
//        eventObj.setUuid(md5UUID(ip,userAgent));
//        eventObj.setIp(ip);
//        eventObj.setUserAgent(userAgent);
//
//        List<event> events = getRandUrlTime(n);
//        eventObj.setEvents(events);
//        return eventObj;
//    }

    private List<event> getRandUrlTime(int n) {
        List<event> events = new ArrayList<>();
        StringBuilder salt = new StringBuilder();
        for(int i = 0; i< 4;i++){
            salt.append("e");
        }
        String url = salt.toString();
        for(int i = 0;i<n;i++){
            event e = new event();
            e.setTime(getTime());
//            e.setUrl(randUrl());

            e.setUrl(url);
            events.add(e);
        }
        this.currentTime = null;
        return events;
    }

//    public JSONObject getJsonEvent(EventSchema eventSchema){
//        JSONObject json = new JSONObject();
//        json.put("ip", eventSchema.getIp());
//        json.put("user_agent", eventSchema.getUserAgent());
//        json.put("event_time", eventSchema.getEventTime());
//        json.put("url", eventSchema.getUrl());
//        return json;
//    }
//
//    public JSONObject getJsonEvent(EventSchemaV1 eventSchema){
//        JSONObject json = new JSONObject();
//        json.put("ip", eventSchema.getIp());
//        json.put("user_agent", eventSchema.getUserAgent());
//        json.put("uuid", eventSchema.getUuid());
//        JSONArray jsonArray = new JSONArray();
//        for(event e : eventSchema.getEvents()){
//            JSONObject event = new JSONObject();
//            event.put("url", e.getUrl());
//            event.put("time", e.getTime());
//            jsonArray.put(event);
//        }
//        json.put("events", jsonArray);
//        return json;
//    }

    public void createFile() throws IOException {

        String fileName = "output1.txt";
        List<String[]> data = new ArrayList<>();
        Instant Time = Instant.now();
        for(int i = 0; i<1000;i++) {
            String ip = ipGenerator();
            String userAgent = randUserAgent();
            currentTime = Time;
            for (int j = 0; j < 50; j++) {
                data.add(new String[]{ip, userAgent, randUrl(), getTime()});
            }

        }

        char customDelimiter = '|';
            try (CSVWriter writer = new CSVWriter(new FileWriter(fileName),
                    customDelimiter,
                    '\0',
                    '\0',
                    CSVWriter.DEFAULT_LINE_END)) {
                writer.writeAll(data);
            }


    }

}

public class ProducerUtil implements Runnable {
    final String topicName = "hadoop_data";
    final int partition;
    ProducerUtil (int key){
        this.partition = key;
    }

    @Override
    public void run(){
        Logger log = LoggerFactory.getLogger(ProducerUtil.class);
        try {
            RandomStringGenerator randGent = new RandomStringGenerator();
            Properties prop = new Properties();
            prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
            prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomAvroSerializer.class.getName());
//            try (KafkaProducer<Void, EventSchema> producer = new KafkaProducer<>(prop)) {
//                while (true) {
//                    Thread.sleep(2000);
//                    EventSchema eventData = randGent.getAvroEvent();
//
//                    ProducerRecord<Void, EventSchema> producerRecord = new ProducerRecord<>(topicName, partition, null, eventData);
//                    producer.send(producerRecord, (recordMetadata, e) ->
//                            log.info("Published to " + recordMetadata.topic()
//                                    + ", Key " + null
//                                    + ", Partition " + recordMetadata.partition()
//                                    + ", timestamp " + recordMetadata.timestamp()
//                            )
//                    );
//                }
//            } catch (Exception e) {
//                log.error("Error occured ", e);
//            }
        }catch (Exception e){
            log.error("Error Generated", e);
        }

    }
}