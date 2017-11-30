package co.nz.solnet.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.Properties;

@Service
public class CapitalizeConsumer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Consumer<String, String> consumer;

    public CapitalizeConsumer(@Autowired Properties kafkaProperties) {
        consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singletonList(kafkaProperties.getProperty("first.topic")));
    }

    public Consumer<String, String> getConsumer() {
        return consumer;
    }

    @PreDestroy
    private void kafkaProducerClose() {
        consumer.unsubscribe();
        consumer.close();
        logger.info("KafkaStreams closed");
    }
}
