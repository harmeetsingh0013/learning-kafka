package com.harmeetsingh13.java.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by harmeet on 11/2/17.
 */
public class SimpleConsumer {

    private static Properties kafkaProps = new Properties();

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountryGroup");

    }

    private static void infinitePollLoop() {
        try(KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps)){
            kafkaConsumer.subscribe(Arrays.asList("CustomerCountry"));
            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                records.forEach(record -> {
                    System.out.printf("Topic: %s, Partition: %s, Offset: %s, Key: %s, Value: %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    System.out.println();

                });
            }
        }
    }

    public static void main(String[] args) {
        infinitePollLoop();
    }
}
