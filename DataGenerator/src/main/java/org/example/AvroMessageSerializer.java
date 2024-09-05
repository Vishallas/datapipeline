package org.example;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroMessageSerializer implements Serializer<EventSchema> {
    @Override
    public byte[] serialize(String topic, EventSchema data) {

        byte[] arr = null;
        try {
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                GenericDatumWriter<EventSchema> writer = new GenericDatumWriter<>(data.getSchema());
                writer.write(data, binaryEncoder);
                binaryEncoder.flush();
                arr = outputStream.toByteArray();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return arr;
    }
}