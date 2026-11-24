package company.vk.edu.distrib.compute.ruavee.urlshortener;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public final class LinkEventCodecUtils {
    private LinkEventCodecUtils() {
    }

    public static String encode(LinkCreatedEvent event) {
        String encodedUrl = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(event.url().getBytes(StandardCharsets.UTF_8));

        return event.id() + ":" + encodedUrl + ":" + event.createdAt().toEpochMilli();
    }

    public static LinkCreatedEvent decode(@Nullable String payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Null event");
        }

        String[] parts = payload.split(":", -1);

        if (parts.length != 3 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("Invalid event format");
        }

        try {
            String url = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
            Instant createdAt = Instant.ofEpochMilli(Long.parseLong(parts[2]));
            return new LinkCreatedEvent(parts[0], url, createdAt);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid event data", e);
        }
    }
}
