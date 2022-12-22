package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    public static final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) {
        try {
            logger.info("Hello Producer");
            String key = "id_";
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            for (int i = 0; i < 10; i++) {
                ProducerRecord producerRecord = new ProducerRecord<>("demo_topic",key+i, "Hello World_"+i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Produced");
                        } else {
                            logger.error("Failed" + e);

                        }
                    }
                });
            }
            producer.flush();
            producer.close();
        } catch (Exception e) {
            logger.error("" + e);
        }
    }
}