package company.vk.edu.distrib.compute.ruavee.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class ClusterRemoteStringDao implements Dao<String> {
    private final List<RemoteStringDao> daos = new ArrayList<>();
    private final int[] ports;

    public ClusterRemoteStringDao(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("No ports provided");
        }
        this.ports = ports.clone();
        for (int port : ports) {
            RemoteStringDao dao = new RemoteStringDao(port);
            daos.add(dao);
        }
    }

    private RemoteStringDao selectDao(String key) {
        int bestIndex = 0;
        long maxHash = Long.MIN_VALUE;
        try {
            MessageDigest hasher = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < ports.length; i++) {
                String input = key + ":" + ports[i];
                byte[] hashBytes = hasher.digest(input.getBytes(StandardCharsets.UTF_8));
                long hash = ByteBuffer.wrap(hashBytes).getLong();
                if (hash > maxHash) {
                    maxHash = hash;
                    bestIndex = i;
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        return daos.get(bestIndex);
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return selectDao(key).get(key);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        selectDao(key).upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        selectDao(key).delete(key);
    }

    @Override
    public void close() throws IOException {
        for (RemoteStringDao dao : daos) {
            dao.close();
        }
    }
}
