package com.harmeetsingh13.java.consumers.avrodesrializer;

import com.harmeetsingh13.java.avroserializer.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by harmeet on 11/2/17.
 */
public class AvroSpecificDeserializer {

    private static Properties kafkaProps = new Properties();

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountryGroup1");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
    }

    public static void infiniteConsumer() throws IOException {
        try(KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(kafkaProps)) {
            kafkaConsumer.subscribe(Arrays.asList("CustomerSpecificCountry"));

            while(true) {
                ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(100);

                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(AvroSpecificDeserializer.class
                        .getClassLoader().getResourceAsStream("avro/customer.avsc"));

                records.forEach(record -> {
                    DatumReader<GenericRecord> customerDatumReader = new SpecificDatumReader<>(schema);
                    BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(record.value(), null);
                    try {
                        GenericRecord customer = customerDatumReader.read(null, binaryDecoder);
                        System.out.println(customer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

        }
    }

    public static void main(String[] args) throws IOException {
        infiniteConsumer();
    }
}
