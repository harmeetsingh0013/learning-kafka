package com.harmeetsingh13.java.producers.avroserializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

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
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
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
        Parser parser = new Parser();
        Schema schema = parser.parse(AvroSpecificProducer.class.getClassLoader().getResourceAsStream("avro/generic.avsc"));

        GenericRecord  customer = new GenericData.Record(schema);
        customer.put("id", 1003);
        customer.put("name", "Micky");
        customer.put("salary", 230000.63);

        ProducerRecord<String, GenericRecord> record1 = new ProducerRecord<>("NewTopic",
                "Customer First", customer
        );

        fireAndForget(record1);

        Thread.sleep(10000);
    }
}
