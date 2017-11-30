package co.nz.solnet.demo.kafka;

import co.nz.solnet.demo.kafka.consumer.CapitalizeConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class ConsumerNormalApplication implements CommandLineRunner {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private CapitalizeConsumer capitalizeConsumer;

    public static void main(String[] args) {
        SpringApplication.run(ConsumerNormalApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                latch.countDown();
            }
        });

        final int giveUp = 100;   int noRecordsCount = 0;
        try {
            while (true) {
                final ConsumerRecords<String, String> consumerRecords = capitalizeConsumer.getConsumer().poll(1000);

                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) break;
                    else continue;
                }

                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
                });

//                capitalizeConsumer.getConsumer().commitAsync();
            }
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }

        System.exit(0);
    }
}
