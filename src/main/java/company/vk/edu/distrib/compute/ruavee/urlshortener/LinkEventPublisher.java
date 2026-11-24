package company.vk.edu.distrib.compute.ruavee.urlshortener;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class LinkEventPublisher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LinkEventPublisher.class);
    private static final String TOPIC = "links.created";
    private static final int QUEUE_CAPACITY = 1024;

    private final KafkaProducer<String, String> producer;
    private final ThreadPoolExecutor executor;

    LinkEventPublisher(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);

        producer = new KafkaProducer<>(properties);

        executor = new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(QUEUE_CAPACITY),
            task -> {
                Thread thread = new Thread(task, "link-event-publisher");
                thread.setDaemon(true);
                return thread;
            }
        );
    }

    void publish(LinkCreatedEvent event) {
        try {
            executor.execute(() -> send(event));
        } catch (RejectedExecutionException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Cannot enqueue moderation event for {}", event.id(), e);
            }
        }
    }

    private void send(LinkCreatedEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, event.id(), LinkEventCodecUtils.encode(event));
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null && LOG.isWarnEnabled()) {
                    LOG.warn("Failed to publish link {}", event.id(), exception);
                }
            });
        } catch (RuntimeException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Cannot send moderation event for {}", event.id(), e);
            }
        }
    }

    @Override
    public void close() {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        producer.close(Duration.ofSeconds(2));
    }
}
