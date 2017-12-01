package co.nz.solnet.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.Properties;

@Service
public class AutoCommitOffsetConsumer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Consumer<String, String> consumer;

    public Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-normal");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("first.topic", "jsontest");

        return properties;
    }

    public AutoCommitOffsetConsumer() {
        consumer = new KafkaConsumer<>(kafkaProperties());
        consumer.subscribe(Collections.singletonList(kafkaProperties().getProperty("first.topic")));
    }

    public void pollOld() {
        final int giveUp = 10;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    logger.info("consumerRecords.count() == 0 too much, give up");
                    break;
                } else {
                    continue;
                }
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
            });
        }

        consumer.unsubscribe();
        consumer.close();
        logger.info("KafkaStreams closed");
    }

    private void pollNew() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
        }

    }

    public void poll() {
        pollOld();
    }

    @PreDestroy
    public void unSubscribeAndClose() {
        consumer.unsubscribe();
        consumer.close();
        logger.info("KafkaStreams closed");
    }
}
