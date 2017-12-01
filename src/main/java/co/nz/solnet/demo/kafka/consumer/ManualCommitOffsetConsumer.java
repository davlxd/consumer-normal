package co.nz.solnet.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Service
public class ManualCommitOffsetConsumer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Consumer<String, String> consumer;

    public Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-normal");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("first.topic", "jsontest");

        return properties;
    }

    public ManualCommitOffsetConsumer() {
        consumer = new KafkaConsumer<>(kafkaProperties());
        consumer.subscribe(Collections.singletonList(kafkaProperties().getProperty("first.topic")));
    }


    public void poll() {
        final int minBatchSize = 4;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                System.out.println("--------------------------");
                System.out.println(buffer);
                System.out.println("==========================");
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    @PreDestroy
    public void unSubscribeAndClose() {
        consumer.unsubscribe();
        consumer.close();
        logger.info("KafkaStreams closed");
    }

}
