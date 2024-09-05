package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class CustomAvroDeserializer implements Deserializer<GenericRecord> {

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
    public GenericRecord deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Avro record", e);
        }
    }

    @Override
    public void close() {
        // Cleanup if needed
    }
}
