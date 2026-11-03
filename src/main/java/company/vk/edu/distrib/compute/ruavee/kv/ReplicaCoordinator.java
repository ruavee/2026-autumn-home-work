package company.vk.edu.distrib.compute.ruavee.kv;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

final class ReplicaCoordinator {
    private static final String INTERNAL_ENTITY_PATH = "/v0/internal/entity";
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    CompletableFuture<QuorumTracker.Result> execute(
            String id, String method, byte[] body, int ack, int... ports) {
        List<Integer> replicas = ReplicaSelectors.selectReplicas(id, ports, ports.length);
        List<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();

        for (int replicaPort : replicas) {
            futures.add(sendToReplica(replicaPort, method, id, body));
        }

        return QuorumTracker.awaitQuorum(futures, method, ack);
    }

    private CompletableFuture<HttpResponse<byte[]>> sendToReplica(
            int replicaPort, String method, String id, byte[] body) {
        String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8);
        URI uri = URI.create("http://localhost:" + replicaPort + INTERNAL_ENTITY_PATH + "?id=" + encodedId);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(2));

        switch (method) {
            case "GET" -> builder.GET();
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            case "DELETE" -> builder.DELETE();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        }

        return HTTP_CLIENT.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }
}
