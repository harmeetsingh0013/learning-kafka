package com.harmeetsingh13.java.producers.avroserializer;

import com.harmeetsingh13.java.CustomerGeneric;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by harmeet on 10/2/17.
 */
public class AvroGenericProducer {

    private static Properties kafkaProps = new Properties();
    private static KafkaProducer<String, GenericRecord> kafkaProducer;

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProducer = new KafkaProducer<>(kafkaProps);
    }

    public static void fireAndForget(ProducerRecord<String, GenericRecord> record) {
        kafkaProducer.send(record);
    }

    public static void asyncSend(ProducerRecord<String, GenericRecord> record) {
        kafkaProducer.send(record, (recordMetaData, ex) -> {
            System.out.println("Offset: "+ recordMetaData.offset());
            System.out.println("Topic: "+ recordMetaData.topic());
            System.out.println("Partition: "+ recordMetaData.partition());
            System.out.println("Timestamp: "+ recordMetaData.timestamp());
        });
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        GenericRecord  customer1 = new GenericData.Record(CustomerGeneric.SCHEMA$);
        customer1.put("id", 1001);
        customer1.put("name", "Micky");
        customer1.put("salary", 230000.63);

        GenericRecord  customer2 = new GenericData.Record(CustomerGeneric.SCHEMA$);
        customer2.put("id", 1002);
        customer2.put("name", "Gunu");
        customer2.put("salary", 230000.63);

        ProducerRecord<String, GenericRecord> record1 = new ProducerRecord<>("AvroGenericProducerTopics",
                "KeyOne", customer1
        );
        ProducerRecord<String, GenericRecord> record2 = new ProducerRecord<>("AvroGenericProducerTopics",
                "KeyOne", customer2
        );

        fireAndForget(record1);
        asyncSend(record2);

        Thread.sleep(1000);
    }
}
