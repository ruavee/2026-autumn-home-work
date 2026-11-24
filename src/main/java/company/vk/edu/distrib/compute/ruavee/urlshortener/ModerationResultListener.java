package company.vk.edu.distrib.compute.ruavee.urlshortener;

import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.ruavee.moderation.ModerationResultCodecUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static company.vk.edu.distrib.compute.ruavee.moderation.ModerationServiceImpl.getProperties;

final class ModerationResultListener implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ModerationResultListener.class);
    private static final String TOPIC = "links.moderated";

    private final String bootstrapServers;
    private final Consumer<String> rejectLink;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<@Nullable KafkaConsumer<String, String>> consumer = new AtomicReference<>();
    private @Nullable Thread worker;

    ModerationResultListener(String bootstrapServers, Consumer<String> rejectLink) {
        this.bootstrapServers = bootstrapServers;
        this.rejectLink = rejectLink;
    }

    private Properties properties() {
        Properties result = new Properties();
        result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        result.put(ConsumerConfig.GROUP_ID_CONFIG, "ruavee-results-" + UUID.randomUUID());
        return getProperties(result);
    }

    private boolean apply(ConsumerRecord<String, String> record) {
        try {
            ModerationResultEvent event = ModerationResultCodecUtils.decode(record.value());
            if (event.id().equals(record.key()) && Boolean.FALSE.equals(event.passed())) {
                rejectLink.accept(event.id());
            }
            return true;
        } catch (IllegalArgumentException e) {
            LOG.warn("Ignoring malformed moderation result", e);
            return true;
        } catch (RuntimeException e) {
            LOG.warn("Cannot apply moderation result", e);
            return false;
        }
    }

    private void consume() {
        try (KafkaConsumer<String, String> kafka = new KafkaConsumer<>(properties())) {
            consumer.set(kafka);
            kafka.subscribe(List.of(TOPIC));
            while (running.get()) {
                for (ConsumerRecord<String, String> record : kafka.poll(Duration.ofMillis(250))) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    if (apply(record)) {
                        kafka.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)));
                    } else {
                        kafka.seek(partition, record.offset());
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                LOG.error("Unexpected Kafka wakeup", e);
            }
        } catch (RuntimeException e) {
            LOG.error("Moderation result consumer failed", e);
        } finally {
            consumer.set(null);
        }
    }

    void start() {
        running.set(true);
        worker = new Thread(this::consume, "moderation-result-consumer");
        worker.start();
    }

    @Override
    public void close() {
        running.set(false);
        KafkaConsumer<String, String> active = consumer.get();
        if (active != null) {
            active.wakeup();
        }
        Thread activeWorker = worker;
        if (activeWorker != null) {
            try {
                activeWorker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
