package company.vk.edu.distrib.compute.ruavee.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentDao implements Dao<String> {
    private final Map<String, String> storage;
    private final Path file;
    private final ReentrantLock lock = new ReentrantLock();

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private void load() throws IOException {
        for (String line : Files.readAllLines(this.file)) {
            String[] parts = line.split(" ", 2);
            String key = decode(parts[0]);
            String value = decode(parts[1]);
            storage.put(key, value);
        }
    }

    private void save() throws IOException {
        List<String> lines = new ArrayList<>();
        for (Map.Entry<String, String> entry : storage.entrySet()) {
            String key = Base64.getEncoder().encodeToString(entry.getKey().getBytes(StandardCharsets.UTF_8));
            String value = Base64.getEncoder().encodeToString(entry.getValue().getBytes(StandardCharsets.UTF_8));
            String line = key + " " + value;
            lines.add(line);
        }
        Files.write(this.file, lines, StandardCharsets.UTF_8);
    }

    public PersistentDao(Path file) throws IOException {
        this.storage = new ConcurrentHashMap<>();
        this.file = file;
        if (Files.exists(file)) {
            load();
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        String value = storage.get(key);
        if (value != null) {
            return value;
        }
        throw new NoSuchElementException();
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        lock.lock();
        try {
            storage.put(key, value);
            save();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        lock.lock();
        try {
            storage.remove(key);
            save();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // Nothing to do here :)
    }
}
