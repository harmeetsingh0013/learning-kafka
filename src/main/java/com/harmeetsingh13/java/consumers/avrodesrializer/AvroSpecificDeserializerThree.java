package com.harmeetsingh13.java.consumers.avrodesrializer;

import com.harmeetsingh13.java.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by harmeet on 14/2/17.
 */
public class AvroSpecificDeserializerThree {

    private static Properties kafkaProps = new Properties();

    static {
        // As per my findings 'latest', 'earliest' and 'none' values are used with kafka consumer poll.
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "AvroSpecificDeserializerThree-GroupOne");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    public static void infiniteConsumer() throws IOException {
        try (KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(kafkaProps)) {
            kafkaConsumer.subscribe(Arrays.asList("AvroSpecificProducerOneTopic"));

            while (true) {
                ConsumerRecords<String, Customer> records = kafkaConsumer.poll(100);

                records.forEach(record -> {
                    Customer customer = record.value();
                    System.out.println("Key : " + record.key());
                    System.out.println("Value: " + customer);
                });
            }
        }
    }

    public static void main(String[] args) throws IOException {
        infiniteConsumer();
    }
}
