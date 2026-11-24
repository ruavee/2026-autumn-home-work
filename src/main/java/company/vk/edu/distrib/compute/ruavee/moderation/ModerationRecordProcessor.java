package company.vk.edu.distrib.compute.ruavee.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.ruavee.urlshortener.LinkEventCodecUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

final class ModerationRecordProcessor {
    private static final String MODERATED_TOPIC = "links.moderated";
    private static final String DEAD_LETTER_TOPIC = "links.created.dlt";

    private final UrlModerationChecker checker = new UrlModerationChecker();

    private ProducerRecord<String, String> deadLetter(ConsumerRecord<String, String> record, String error) {
        ProducerRecord<String, String> result = new ProducerRecord<>(DEAD_LETTER_TOPIC, record.key(), record.value());
        result.headers().add("error", error.getBytes(StandardCharsets.UTF_8));
        return result;
    }

    ProducerRecord<String, String> prepareResult(ConsumerRecord<String, String> record) {
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
}
