package com.harmeetsingh13.java.consumers.avrodesrializer;

import com.harmeetsingh13.java.Customer;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static kafka.consumer.Consumer.createJavaConsumerConnector;

/**
 * Created by harmeet on 11/2/17.
 */
public class AvroSpecificDeserializerOne {

    private static Properties kafkaProps = new Properties();

    static {
        // As per my findings 'smallest' and 'largest' values are used with kafka stream.
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "AvroSpecificDeserializerOne-GroupOne");
        kafkaProps.put("zookeeper.connect", "localhost:2181");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    public static void infiniteConsumer() throws IOException {

        VerifiableProperties properties = new VerifiableProperties(kafkaProps);
        StringDecoder keyDecoder = new StringDecoder(properties);
        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(properties);

        final String topic = "AvroSpecificProducerOneTopic";

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);

        while (true) {
            ConsumerConnector consumer = createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(kafkaProps));
            Map<String, List<KafkaStream<String, Object>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);

            KafkaStream stream = consumerMap.get(topic).get(0);
            ConsumerIterator it = stream.iterator();

            while (it.hasNext()) {
                MessageAndMetadata messageAndMetadata = it.next();
                String key = (String) messageAndMetadata.key();
                GenericRecord record = (GenericRecord) messageAndMetadata.message();
                Customer customer = (Customer) SpecificData.get().deepCopy(Customer.SCHEMA$, record);
                System.out.println("Key: " + key);
                System.out.println("Value: " + customer);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        infiniteConsumer();
    }
}
