package com.harmeetsingh13.java.producers.avroserializer;

import com.harmeetsingh13.java.avroserializer.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by harmeet on 10/2/17.
 */
public class AvroSpecificProducer {
    private static Properties kafkaProps = new Properties();
    private static KafkaProducer<String, byte[]> kafkaProducer;

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProducer = new KafkaProducer<>(kafkaProps);
    }

    public static void fireAndForget(ProducerRecord<String, byte[]> record) {
        kafkaProducer.send(record);
    }

    public static void asyncSend(ProducerRecord<String, byte[]> record) {
        kafkaProducer.send(record, (recordMetaData, ex) -> {
            System.out.println("Offset: "+ recordMetaData.offset());
            System.out.println("Topic: "+ recordMetaData.topic());
            System.out.println("Partition: "+ recordMetaData.partition());
            System.out.println("Timestamp: "+ recordMetaData.timestamp());
        });
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Customer customer1 = new Customer(1002, "Jimmy");

        Parser parser = new Parser();
        Schema schema = parser.parse(AvroSpecificProducer.class
                .getClassLoader().getResourceAsStream("avro/customer.avsc"));

        SpecificDatumWriter<Customer> writer = new SpecificDatumWriter<>(schema);
        try(ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(customer1, encoder);
            encoder.flush();

            byte[] avroBytes = os.toByteArray();

            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>("CustomerSpecificCountry",
                    "Customer One 11 ", avroBytes
            );

            asyncSend(record1);
        }

        Thread.sleep(10000);
    }
}
