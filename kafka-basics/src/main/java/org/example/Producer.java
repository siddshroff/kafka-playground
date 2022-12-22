package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        try {
            logger.info("Hello Producer");

            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            ProducerRecord producerRecord = new ProducerRecord<>("demo_topic", "Hello World");
            producer.send(producerRecord);
            producer.flush();
            producer.close();
        } catch (Exception e) {
            logger.error("" + e);
        }
    }
}