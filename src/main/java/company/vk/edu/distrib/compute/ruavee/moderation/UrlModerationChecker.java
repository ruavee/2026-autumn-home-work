package company.vk.edu.distrib.compute.ruavee.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import org.jspecify.annotations.Nullable;

public final class UrlModerationChecker {
    private static final String RESOURCE_PATH = "/moderation/banned_words.txt";
    private final List<String> bannedWords;

    public UrlModerationChecker() {
        @Nullable InputStream resource = UrlModerationChecker.class.getResourceAsStream(RESOURCE_PATH);

        if (resource == null) {
            throw new IllegalStateException("Banned words resource not found");
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource, StandardCharsets.UTF_8))) {
            bannedWords = reader.lines()
                    .map(String::trim)
                    .filter(word -> !word.isEmpty())
                    .map(word -> word.toLowerCase(Locale.ROOT))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load banned words", e);
        }
    }

    public boolean isBanned(String url) {
        String decoded = URLDecoder.decode(url, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        return bannedWords.stream().anyMatch(decoded::contains);
    }
}
