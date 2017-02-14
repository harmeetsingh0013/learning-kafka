package com.harmeetsingh13.java.producers.avroserializer;

import com.harmeetsingh13.java.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by harmeet on 13/2/17.
 */
public class AvroSpecificProducerTwo {

    private static Properties kafkaProps = new Properties();
    private static KafkaProducer<String, byte[]> kafkaProducer;

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProducer = new KafkaProducer<>(kafkaProps);
    }

    public static void fireAndForget(ProducerRecord<String, byte[]> record) {
        kafkaProducer.send(record);
    }

    public static void asyncSend(ProducerRecord<String, byte[]> record) {
        kafkaProducer.send(record, (recordMetaData, ex) -> {
            System.out.println("Offset: " + recordMetaData.offset());
            System.out.println("Topic: " + recordMetaData.topic());
            System.out.println("Partition: " + recordMetaData.partition());
            System.out.println("Timestamp: " + recordMetaData.timestamp());
        });
    }

    private static byte[] convertCustomerToAvroBytes(Customer customer) throws IOException {
        Parser parser = new Parser();
        Schema schema = parser.parse(AvroSpecificProducerOne.class
                .getClassLoader().getResourceAsStream("avro/customer.avsc"));

        SpecificDatumWriter<Customer> writer = new SpecificDatumWriter<>(schema);
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(customer, encoder);
            encoder.flush();

            return os.toByteArray();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Customer customer1 = new Customer(1001, "Jimmy");
        Customer customer2 = new Customer(1002, "James");


        byte[] customer1AvroBytes = convertCustomerToAvroBytes(customer1);
        byte[] customer2AvroBytes = convertCustomerToAvroBytes(customer2);

        ProducerRecord<String, byte[]> record1 = new ProducerRecord<>("AvroSpecificProducerTwoTopic",
                "KeyOne", customer1AvroBytes
        );
        ProducerRecord<String, byte[]> record2 = new ProducerRecord<>("AvroSpecificProducerTwoTopic",
                "KeyOne", customer2AvroBytes
        );

        asyncSend(record1);
        asyncSend(record2);

        Thread.sleep(1000);
    }
}
