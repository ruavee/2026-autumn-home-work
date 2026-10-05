package company.vk.edu.distrib.compute.ruavee.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentByteDao implements Dao<byte[]> {
    private static final String PUT = "P";
    private static final String DELETE = "D";

    private final Map<String, byte[]> storage;
    private final Path file;
    private final ReentrantLock lock = new ReentrantLock();

    private static String decodeKey(String key) {
        return new String(Base64.getDecoder().decode(key), StandardCharsets.UTF_8);
    }

    private static byte[] decodeValue(String value) {
        return Base64.getDecoder().decode(value);
    }

    private static String encodeKey(String key) {
        return Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private static String encodeValue(byte[] value) {
        return Base64.getEncoder().encodeToString(value);
    }

    private void append(String line) throws IOException {
        Files.writeString(
                file,
                line + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    private void load() throws IOException {
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String[] parts = line.split(" ", 3);
            if (PUT.equals(parts[0])) {
                storage.put(decodeKey(parts[1]), decodeValue(parts[2]));
            } else if (DELETE.equals(parts[0])) {
                storage.remove(decodeKey(parts[1]));
            }
        }
    }

    public PersistentByteDao(Path file) throws IOException {
        this.storage = new ConcurrentHashMap<>();
        this.file = file;
        if (Files.exists(file)) {
            load();
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        byte[] value = storage.get(key);
        if (value != null) {
            return Arrays.copyOf(value, value.length);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        lock.lock();
        try {
            append(PUT + " " + encodeKey(key) + " " + encodeValue(value));
            storage.put(key, Arrays.copyOf(value, value.length));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        lock.lock();
        try {
            append(DELETE + " " + encodeKey(key));
            storage.remove(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // Nothing to do here :)
    }
}
