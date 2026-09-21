package company.vk.edu.distrib.compute.ruavee.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

public class UrlShortenerHandler implements HttpHandler {

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private final SecureRandom random = new SecureRandom();
    private final Dao<String> dao;
    private final BasicAuth auth;
    private final int port;
    private final ReentrantLock lock = new ReentrantLock();

    public UrlShortenerHandler(Dao<String> dao, BasicAuth auth, int port) {
        this.dao = dao;
        this.auth = auth;
        this.port = port;
    }

    private boolean isValidUrl(String url) {
        try {
            URI uri = URI.create(url);
            if (("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) && uri.getHost() != null) {
                return true;
            }
        } catch (IllegalArgumentException e) {
            return false;
        }
        return false;
    }

    private boolean isValidId(String id) {
        if (id.length() == 10) {
            for (int i = 0; i < 10; i++) {
                if (ALPHABET.indexOf(id.charAt(i)) == -1) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private String randomId() throws IOException {
        StringBuilder id;
        boolean exists = true;
        do {
            id = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                id.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
            }
            try {
                dao.get(id.toString());
            } catch (NoSuchElementException e) {
                exists = false;
            }
        } while (exists);
        return id.toString();
    }

    private void handleCreate(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (!isValidUrl(body)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }
        String id;
        lock.lock();
        try {
            id = randomId();
            dao.upsert(id, body);
        } finally {
            lock.unlock();
        }
        byte[] response = ("http://localhost:" + port + "/" + id).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(201, response.length);
        exchange.getResponseBody().write(response);
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        if (!isValidId(id)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }
        try {
            String url = dao.get(id);
            byte[] response = url.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void handleUpdate(HttpExchange exchange, String id) throws IOException {
        if (!isValidId(id)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (!isValidUrl(body)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }
        boolean found;
        lock.lock();
        try {
            dao.get(id);
            dao.upsert(id, body);
            found = true;
        } catch (NoSuchElementException e) {
            found = false;
        } finally {
            lock.unlock();
        }
        if (found) {
            exchange.sendResponseHeaders(200, -1);
        } else {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        if (!isValidId(id)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }
        lock.lock();
        try {
            dao.delete(id);
        } finally {
            lock.unlock();
        }
        exchange.sendResponseHeaders(202, -1);
    }

    private void handleRedirect(HttpExchange exchange, String id) throws IOException {
        if (!isValidId(id)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }
        try {
            String url = dao.get(id);
            exchange.getResponseHeaders().add("Location", url);
            exchange.sendResponseHeaders(301, -1);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void handleLinks(HttpExchange exchange, String method, String path) throws IOException {
        if (!auth.checkAuthorization(exchange)) {
            return;
        }

        if ("POST".equals(method) && "/v0/links".equals(path)) {
            handleCreate(exchange);
            return;
        }

        if (!path.startsWith("/v0/links/")) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        String id = path.substring("/v0/links/".length());

        switch (method) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handleUpdate(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(404, -1);
        }
    }

    private boolean isLinksPath(String path) {
        return "/v0/links".equals(path) || path.startsWith("/v0/links/");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        if ("GET".equals(method) && "/v0/status".equals(path)) {
            exchange.sendResponseHeaders(200, -1);
        } else if (isLinksPath(path)) {
            handleLinks(exchange, method, path);
        } else if ("POST".equals(method) && "/internal/users".equals(path)) {
            auth.handleCreateUser(exchange);
        } else if ("GET".equals(method) && path.startsWith("/")) {
            String id = path.substring(1);
            handleRedirect(exchange, id);
        } else {
            exchange.sendResponseHeaders(404, -1);
        }
        exchange.close();
    }
}
