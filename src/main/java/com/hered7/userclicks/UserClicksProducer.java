package com.hered7.userclicks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
public class UserClicksProducer {

    public static void main(String[] args) {

        SpringApplication.run(UserClicksProducer.class, args);

        // Initial Logger
        Logger logger = LoggerFactory.getLogger(LoggingController.class);
        logger.atInfo();

        // Kafka producer configuration settings
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Simulate generating user clicks
            while (true) {
                String userId = UUID.randomUUID().toString();
                String timestamp = Instant.now().toString();
                String url = "http://daniheredia.com/page/" + ThreadLocalRandom.current().nextInt(1, 101); // Random URL

                // Construct the click event JSON
                String clickEvent = String.format("{\"userId\":\"%s\",\"timestamp\":\"%s\",\"url\":\"%s\"}", userId, timestamp, url);

                // Send the click event to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>("user-clicks", userId, clickEvent);
                producer.send(record);

                // Simulate a delay between clicks
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 1000)); // Random delay between 100ms and 1s
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
