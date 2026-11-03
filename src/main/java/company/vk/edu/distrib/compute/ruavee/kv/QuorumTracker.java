package company.vk.edu.distrib.compute.ruavee.kv;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

final class QuorumTracker {
    private static final String GET_METHOD = "GET";
    private static final int HTTP_OK = 200;
    private final String method;
    private final int ack;
    private final int replicaCount;
    private final ReentrantLock lock = new ReentrantLock();
    private final CompletableFuture<Result> result = new CompletableFuture<>();

    private int successes;
    private int completed;
    private byte[] foundValue;

    record Result(int status, byte[] body) {
    }

    private QuorumTracker(String method, int ack, int replicaCount) {
        this.method = method;
        this.ack = ack;
        this.replicaCount = replicaCount;
    }

    static CompletableFuture<Result> awaitQuorum(
            List<CompletableFuture<HttpResponse<byte[]>>> futures,
            String method, int ack
    ) {
        if (ack < 1 || ack > futures.size()) {
            throw new IllegalArgumentException("invalid ack");
        }
        QuorumTracker tracker = new QuorumTracker(method, ack, futures.size());
        for (CompletableFuture<HttpResponse<byte[]>> future : futures) {
            future.whenComplete(tracker::accept);
        }
        return tracker.result;
    }

    private void accept(HttpResponse<byte[]> response, Throwable error) {
        lock.lock();
        try {
            if (result.isDone()) {
                return;
            }
            completed++;
            if (error == null && response != null && isSuccessful(response.statusCode())) {
                successes++;
                captureValue(response);
            }
            checkQuorum();
        } finally {
            lock.unlock();
        }
    }

    private boolean isSuccessful(int status) {
        return switch (method) {
            case "GET" -> status == 200 || status == 404;
            case "PUT" -> status == 201;
            case "DELETE" -> status == 202;
            default -> false;
        };
    }

    private void captureValue(HttpResponse<byte[]> response) {
        if (GET_METHOD.equals(method) && response.statusCode() == HTTP_OK && foundValue == null) {
            foundValue = response.body();
        }
    }

    private void checkQuorum() {
        if (successes >= ack) {
            result.complete(successResult());
        } else if (successes + replicaCount - completed < ack) {
            result.complete(new Result(503, null));
        }
    }

    private Result successResult() {
        return switch (method) {
            case "GET" -> foundValue == null ? new Result(404, null) : new Result(200, foundValue);
            case "PUT" -> new Result(201, null);
            case "DELETE" -> new Result(200, null);
            default -> new Result(405, null);
        };
    }
}
