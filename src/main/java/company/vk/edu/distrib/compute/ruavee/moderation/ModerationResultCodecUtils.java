package company.vk.edu.distrib.compute.ruavee.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public final class ModerationResultCodecUtils {
    private ModerationResultCodecUtils() {
    }

    public static String encode(ModerationResultEvent event) {
        String encodedReason = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(event.reason().getBytes(StandardCharsets.UTF_8));
        return event.id() + ":" + event.passed() + ":" + encodedReason + ":" + event.moderatedAt().toEpochMilli();
    }

    public static ModerationResultEvent decode(@Nullable String payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Null result");
        }
        String[] parts = payload.split(":", -1);
        if (parts.length != 4 || parts[0].isBlank() || (!"true".equals(parts[1]) && !"false".equals(parts[1]))) {
            throw new IllegalArgumentException("Invalid result format");
        }
        try {
            String reason = new String(Base64.getUrlDecoder().decode(parts[2]), StandardCharsets.UTF_8);
            Instant moderatedAt = Instant.ofEpochMilli(Long.parseLong(parts[3]));
            return new ModerationResultEvent(parts[0], Boolean.parseBoolean(parts[1]), reason, moderatedAt);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid result data", e);
        }
    }
}
