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

public class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;
    private static final int STOP_DELAY_SECONDS = 1;

    private void handleStatus(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
            exchange.sendResponseHeaders(200, -1);
        } else {
            exchange.sendResponseHeaders(405, -1);
        }
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith("/v0/entity/")) {
            exchange.close();
            return;
        }
        String id = path.substring("/v0/entity/".length());
        if (id.isEmpty()) {
            exchange.sendResponseHeaders(400,-1);
            exchange.close();
            return;
        }
        String method = exchange.getRequestMethod();
        switch (method) {
            case "GET" -> {
                try{
                    byte[] value = dao.get(id);
                    exchange.sendResponseHeaders(200, value.length);
                    exchange.getResponseBody().write(value);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
                }
            }
            case "PUT" -> {
                dao.upsert(id, exchange.getRequestBody().readAllBytes());
                exchange.sendResponseHeaders(201, -1);
            }
            case "DELETE" -> {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
            }
            default -> exchange.sendResponseHeaders(405, -1);
        }
        exchange.close();
    }

    public KVServiceImpl(int port) throws IOException {
        Path path = Path.of(System.getProperty("java.io.tmpdir"));
        path = path.resolve("ruavee-kv");
        Files.createDirectories(path);
        Path file = path.resolve("kv-" + port + ".db");

        this.dao = new PersistentByteDao(file);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity/", this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(STOP_DELAY_SECONDS);
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
