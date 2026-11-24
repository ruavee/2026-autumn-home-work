package company.vk.edu.distrib.compute.ruavee.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.ruavee.urlshortener.LinkEventCodecUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class ModerationServiceImpl implements ModerationService {
    private static final Logger LOG = LoggerFactory.getLogger(ModerationServiceImpl.class);
    private static final String CREATED_TOPIC = "links.created";
    private static final String MODERATED_TOPIC = "links.moderated";
    private static final String DEAD_LETTER_TOPIC = "links.created.dlt";
    private static final String GROUP_ID = "ruavee-moderators";

    private final String bootstrapServers;
    private final UrlModerationChecker checker = new UrlModerationChecker();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<@Nullable KafkaConsumer<String, String>> consumer = new AtomicReference<>();
    private @Nullable Thread worker;

    public ModerationServiceImpl(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return getProperties(properties);
    }

    public static Properties getProperties(Properties properties) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        return properties;
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    private ProducerRecord<String, String> deadLetter(ConsumerRecord<String, String> record, String error) {
        ProducerRecord<String, String> result = new ProducerRecord<>(DEAD_LETTER_TOPIC, record.key(), record.value());
        result.headers().add("error", error.getBytes(StandardCharsets.UTF_8));
        return result;
    }

    private ProducerRecord<String, String> prepareResult(ConsumerRecord<String, String> record) {
        try {
            LinkCreatedEvent event = LinkEventCodecUtils.decode(record.value());
            if (!event.id().equals(record.key())) {
                return deadLetter(record, "Link id and Kafka key do not match");
            }
            boolean banned = checker.isBanned(event.url());
            ModerationResultEvent result = new ModerationResultEvent(
                    event.id(), !banned, banned ? "Banned word detected" : "", Instant.now()
            );
            return new ProducerRecord<>(MODERATED_TOPIC, event.id(), ModerationResultCodecUtils.encode(result));
        } catch (IllegalArgumentException e) {
            return deadLetter(record, e.toString());
        }
    }

    private boolean publish(KafkaProducer<String, String> producer, ConsumerRecord<String, String> record) {
        try {
            producer.send(prepareResult(record)).get(5, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException e) {
            running.set(false);
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            LOG.warn("Cannot publish moderation of link {}", record.key(), e);
            return false;
        }
    }

    private void consume() {
        try (KafkaConsumer<String, String> kafka = new KafkaConsumer<>(consumerProperties());
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            consumer.set(kafka);
            kafka.subscribe(List.of(CREATED_TOPIC));
            while (running.get()) {
                for (ConsumerRecord<String, String> record : kafka.poll(Duration.ofMillis(250))) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    if (publish(producer, record)) {
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
            LOG.error("Moderation consumer failed", e);
        } finally {
            consumer.set(null);
        }
    }

    @Override
    public synchronized void start() {
        if (worker != null) {
            throw new IllegalStateException("Already started");
        }
        running.set(true);
        worker = new Thread(this::consume, "moderation-consumer");
        worker.start();
    }

    @Override
    public synchronized void stop() {
        running.set(false);
        KafkaConsumer<String, String> activeConsumer = consumer.get();
        if (activeConsumer != null) {
            activeConsumer.wakeup();
        }
        if (worker != null) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
