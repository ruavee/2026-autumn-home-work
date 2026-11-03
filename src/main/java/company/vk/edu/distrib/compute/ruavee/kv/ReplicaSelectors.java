package company.vk.edu.distrib.compute.ruavee.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public final class ReplicaSelectors {
    private ReplicaSelectors() {
    }

    private record PortScore(int port, long score) {}

    public static List<Integer> selectReplicas(String key, int[] ports, int replicaCount) {
        if (replicaCount < 1 || replicaCount > ports.length) {
            throw new IllegalArgumentException("invalid replica count");
        }
        MessageDigest hasher;
        try {
            hasher = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }

        List<PortScore> scores = new ArrayList<>(ports.length);
        Set<Integer> seen = new HashSet<>();
        for (int port : ports) {
            if (!seen.add(port)) {
                throw new IllegalArgumentException("duplicate port");
            }

            byte[] hash = hasher.digest((key + ":" + port).getBytes(StandardCharsets.UTF_8));
            scores.add(new PortScore(port, ByteBuffer.wrap(hash).getLong()));
        }
        scores.sort((a, b) -> Long.compare(b.score(), a.score()));
        return scores.subList(0, replicaCount).stream().map(PortScore::port).toList();
    }
}
