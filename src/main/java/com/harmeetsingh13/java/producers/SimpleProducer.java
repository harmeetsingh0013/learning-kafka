package com.harmeetsingh13.java.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by harmeet on 10/2/17.
 */
public class SimpleProducer {

    private static Properties kafkaProps = new Properties();
    private static KafkaProducer<String, String> kafkaProducer;

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducer = new KafkaProducer<>(kafkaProps);
    }


    public static void fireAndForget(ProducerRecord<String, String> record) {
        kafkaProducer.send(record);
    }

    public static void asyncSend(ProducerRecord<String, String> record) {
        kafkaProducer.send(record, (recordMetaData, ex) -> {
            System.out.println("Offset: "+ recordMetaData.offset());
            System.out.println("Topic: "+ recordMetaData.topic());
            System.out.println("Partition: "+ recordMetaData.partition());
            System.out.println("Timestamp: "+ recordMetaData.timestamp());
        });
    }

    public static void main(String[] args) throws InterruptedException {
        ProducerRecord<String, String> record1 = new ProducerRecord<>("CustomerCountry",
        "Record 1", "Japan1"
        );

        ProducerRecord<String, String> record2 = new ProducerRecord<>("CustomerCountry",
                "Record 2", "Punjab1"
        );

        fireAndForget(record1);
        asyncSend(record2);

        Thread.sleep(10000);
    }
}
