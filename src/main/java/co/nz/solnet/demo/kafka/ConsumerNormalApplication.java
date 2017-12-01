package co.nz.solnet.demo.kafka;

import co.nz.solnet.demo.kafka.consumer.AutoCommitOffsetConsumer;
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
    private AutoCommitOffsetConsumer autoCommitOffsetConsumer;

    public static void main(String[] args) {
        SpringApplication.run(ConsumerNormalApplication.class, args);
    }


    private void consumerPoll() {
        autoCommitOffsetConsumer.poll();
        autoCommitOffsetConsumer.unSubscribeAndClose();
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

        try {
            consumerPoll();

            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }

        System.exit(0);
    }
}
