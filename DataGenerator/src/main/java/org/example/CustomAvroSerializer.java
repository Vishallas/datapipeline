package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class CustomAvroSerializer implements Serializer<GenericRecord> {

    private Schema schema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String schemaFilePath = (String) configs.get("avro.schema.path");
        if (schemaFilePath != null) {
            try {
                schema = new Schema.Parser().parse(new File(schemaFilePath));
            } catch (IOException e) {
                throw new RuntimeException("Failed to load Avro schema", e);
            }
        } else {
            throw new IllegalArgumentException("Schema path must be provided");
        }
    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        if (data == null) {
            return null;
        }

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(data, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Avro record", e);
        }
    }

    @Override
    public void close() {
        // Cleanup if needed
    }
}
