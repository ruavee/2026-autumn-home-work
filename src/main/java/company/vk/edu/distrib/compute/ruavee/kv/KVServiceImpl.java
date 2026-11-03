package company.vk.edu.distrib.compute.ruavee.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final int port;
    private static final int STOP_DELAY_SECONDS = 1;
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String INTERNAL_ENTITY_PATH = "/v0/internal/entity";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final int MAX_VALUE_BYTES = 8 * 1024 * 1024;
    private final ReplicaCoordinator replicaCoordinator = new ReplicaCoordinator();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }

            String method = exchange.getRequestMethod();
            if (!METHOD_GET.equals(method) && !METHOD_PUT.equals(method) && !METHOD_DELETE.equals(method)) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String rawQuery = exchange.getRequestURI().getRawQuery();
            int[] ports = getClusterPorts();
            String id;
            int ack;

            try {
                id = EntityQueryUtils.parseId(rawQuery);
                ack = EntityQueryUtils.parseAck(rawQuery, ports.length);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            if (System.getProperty("CLUSTER") == null) {
                processLocalOperation(exchange, id);
                return;
            }

            byte[] body = null;
            if (METHOD_PUT.equals(method)) {
                body = exchange.getRequestBody().readNBytes(MAX_VALUE_BYTES + 1);
                if (body.length > MAX_VALUE_BYTES) {
                    exchange.sendResponseHeaders(413, -1);
                    return;
                }
            }

            QuorumTracker.Result result = replicaCoordinator.execute(id, method, body, ack, ports).join();
            byte[] responseBody = result.body();
            if (responseBody == null) {
                exchange.sendResponseHeaders(result.status(), -1);
            } else {
                exchange.sendResponseHeaders(result.status(), responseBody.length);
                exchange.getResponseBody().write(responseBody);
            }
        }
    }

    private void processLocalOperation(HttpExchange exchange, String id) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case METHOD_GET -> {
                try {
                    byte[] value = dao.get(id);
                    exchange.sendResponseHeaders(200, value.length);
                    exchange.getResponseBody().write(value);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
                }
            }
            case METHOD_PUT -> {
                byte[] body = exchange.getRequestBody().readNBytes(MAX_VALUE_BYTES + 1);
                if (body.length > MAX_VALUE_BYTES) {
                    exchange.sendResponseHeaders(413, -1);
                } else {
                    dao.upsert(id, body);
                    exchange.sendResponseHeaders(201, -1);
                }
            }
            case METHOD_DELETE -> {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
            }
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleInternalEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String rawQuery = exchange.getRequestURI().getRawQuery();
            String id;

            try {
                id = EntityQueryUtils.parseId(rawQuery);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            processLocalOperation(exchange, id);
        }
    }

    public KVServiceImpl(int port) throws IOException {
        Path path = Path.of(System.getProperty("java.io.tmpdir"));
        path = path.resolve("ruavee-kv");
        Files.createDirectories(path);
        Path file = path.resolve("kv-" + port + ".db");

        this.dao = new PersistentByteDao(file);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(executor);
        this.port = port;

        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
        server.createContext(INTERNAL_ENTITY_PATH, this::handleInternalEntity);
    }

    private int[] getClusterPorts() {
        String cluster = System.getProperty("CLUSTER");
        if (cluster == null) {
            return new int[] {port};
        }
        String[] parts = cluster.split(",");
        int[] ports = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            ports[i] = Integer.parseInt(parts[i].trim());
        }
        return ports;
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(STOP_DELAY_SECONDS);
        executor.shutdown();
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
