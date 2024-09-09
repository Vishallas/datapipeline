package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

public class Main {
    static final int THREAD_COUNT = 5;

    //static final String TOPIC = "hadoop_data";

    private static void testAvro() throws IOException, NoSuchAlgorithmException {
        // Load the Avro schema from a local file
        RandomStringGenerator rd = new RandomStringGenerator();

//        EventSchema user = rd.getAvroEvent();
        EventSchemaV1 user = rd.getAvroEventV1(10000);


        // Serialize the record to a byte array
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

//        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(EventSchema.getClassSchema());
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(EventSchemaV1.getClassSchema());

        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(user, encoder);
        encoder.flush();
        outputStream.close();

        // Get serialized data
        byte[] serializedData = outputStream.toByteArray();

        // Deserialize the record from a byte array
        InputStream inputStream = new ByteArrayInputStream(serializedData);

//        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(EventSchema.getClassSchema());
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(EventSchemaV1.getClassSchema());

        Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord users = datumReader.read(null, decoder);
        inputStream.close();

        // Print the deserialized record
//        System.out.println("Serialized data: " + serializedData );
        System.out.println("Avro data Length : " + serializedData.length);
//        System.out.println("Deserialized record: " + users);
        JSONObject json = rd.getJsonEvent(user);
        System.out.println("JsonObject String : " + json.toString());
        System.out.println("Json string Length : " + json.toString().length());
    }
    private static void createFile() throws NoSuchAlgorithmException, IOException {
        RandomStringGenerator rd = new RandomStringGenerator();
        rd.createFile();
    }

    private static void produceData(){
        final String topicName = "hadoop_data";
        final String AVRO_SCHEMA_PATH = "/home/vishal-pt7653/Documents/Project-assignment/datapipeline/DataGenerator/src/main/avro/EventSchemaV1.avsc";

        final Logger log = LoggerFactory.getLogger(Main.class);
        log.info("Logger initialized");

        try {
            RandomStringGenerator randGent = new RandomStringGenerator();

            Properties prop = new Properties();
            prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
            prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomAvroSerializer.class.getName());
            prop.setProperty("avro.schema.path", AVRO_SCHEMA_PATH);


            try (KafkaProducer<Void, EventSchema> producer = new KafkaProducer<>(prop)) {
                while (true) {
                    Thread.sleep(200);
                    EventSchema eventData = randGent.getAvroEvent();

                    ProducerRecord<Void, EventSchema> producerRecord = new ProducerRecord<>(topicName,null, eventData);
                    producer.send(producerRecord, (recordMetadata, e) ->
                            log.info("Published to " + recordMetadata.topic()
                                    + ", Key " + null
                                    + ", Partition " + recordMetadata.partition()
                                    + ", timestamp " + recordMetadata.timestamp()
                            )
                    );
                }
            } catch (Exception e) {
                log.error("Error occured ", e);
            }
        }catch (Exception e){
            log.error("Error Generated", e);
        }
    }

    public static void main(String[] args) throws  Exception{
        //testAvro();
        createFile();
        //produceData();

    }
}