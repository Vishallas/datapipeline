package org.example;
import org.apache.kafka.clients.producer.*;

import java.util.Random;

class RandomStringGenerator {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int STRING_LENGTH = 10; // Length of the generated string

    public static String generateRandomString() {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(STRING_LENGTH);

        for (int i = 0; i < STRING_LENGTH; i++) {
            int index = random.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }

        return sb.toString();
    }
}

public class ProducerUtil implements Runnable {

    private final KafkaProducer<Integer, String> producer;
    private final String topicName;
    private int key;

//    private static final Logger logger = LogManager.getLogger(Dispatcher.class);
    ProducerUtil(KafkaProducer<Integer, String> producer, String topicName, int key) {
        this.producer = producer;
        this.topicName = topicName;
        this.key = key;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(2000);
                String data = RandomStringGenerator.generateRandomString();
                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>(topicName, key, data+" " +key);
                producer.send(producerRecord);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

