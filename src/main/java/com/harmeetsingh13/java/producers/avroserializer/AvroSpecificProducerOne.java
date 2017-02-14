package com.harmeetsingh13.java.producers.avroserializer;

import com.harmeetsingh13.java.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by harmeet on 10/2/17.
 */
public class AvroSpecificProducerOne {
    private static Properties kafkaProps = new Properties();
    private static KafkaProducer<String, Customer> kafkaProducer;

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProducer = new KafkaProducer<>(kafkaProps);
    }

    public static void fireAndForget(ProducerRecord<String, Customer> record) {
        kafkaProducer.send(record);
    }

    public static void asyncSend(ProducerRecord<String, Customer> record) {
        kafkaProducer.send(record, (recordMetaData, ex) -> {
            System.out.println("Offset: " + recordMetaData.offset());
            System.out.println("Topic: " + recordMetaData.topic());
            System.out.println("Partition: " + recordMetaData.partition());
            System.out.println("Timestamp: " + recordMetaData.timestamp());
        });
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Customer customer1 = new Customer(1001, "Jimmy");
        Customer customer2 = new Customer(1002, "James");

        ProducerRecord<String, Customer> record1 = new ProducerRecord<>("AvroSpecificProducerOneTopic",
                "KeyOne", customer1
        );
        ProducerRecord<String, Customer> record2 = new ProducerRecord<>("AvroSpecificProducerOneTopic",
                "KeyOne", customer2
        );

        asyncSend(record1);
        asyncSend(record2);

        Thread.sleep(1000);
    }
}
