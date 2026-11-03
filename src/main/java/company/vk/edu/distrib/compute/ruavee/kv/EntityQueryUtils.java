package company.vk.edu.distrib.compute.ruavee.kv;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

final class EntityQueryUtils {
    private EntityQueryUtils() {
    }

    static String parseId(String rawQuery) {
        if (rawQuery == null) {
            throw new IllegalArgumentException("no id");
        }
        String[] parts = rawQuery.split("&");
        String id = null;
        boolean foundId = false;
        for (String part : parts) {
            if ("id".equals(part)) {
                throw new IllegalArgumentException("no value for id");
            }
            if (part.startsWith("id=")) {
                if (foundId) {
                    throw new IllegalArgumentException("duplicate id");
                }
                foundId = true;
                id = URLDecoder.decode(part.substring(3), StandardCharsets.UTF_8);
                if (id.isEmpty()) {
                    throw new IllegalArgumentException("empty id");
                }
            }
        }
        if (!foundId) {
            throw new IllegalArgumentException("no id");
        }
        return id;
    }

    static int parseAck(String rawQuery, int replicaCount) {
        if (rawQuery == null) {
            return 1;
        }
        String[] parts = rawQuery.split("&");
        int ack = 1;
        boolean foundAck = false;
        for (String part : parts) {
            if ("ack".equals(part)) {
                throw new IllegalArgumentException("no value for ack");
            }
            if (part.startsWith("ack=")) {
                if (foundAck) {
                    throw new IllegalArgumentException("duplicate ack");
                }
                foundAck = true;
                ack = Integer.parseInt(part.substring(4));
                if (ack < 1 || ack > replicaCount) {
                    throw new IllegalArgumentException("invalid ack");
                }
            }
        }
        return ack;
    }
}
