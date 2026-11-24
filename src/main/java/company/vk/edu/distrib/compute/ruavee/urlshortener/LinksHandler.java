
package company.vk.edu.distrib.compute.ruavee.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

final class LinksHandler {

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final int ID_LENGTH = 10;

    private static final String METHOD_GET = "GET";
    private static final String METHOD_POST = "POST";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String LINKS_PATH = "/v0/links";
    private static final String LINKS_PREFIX = "/v0/links/";

    private final SecureRandom random = new SecureRandom();
    private final BasicAuth auth;
    private final int port;
    private final ReentrantLock lock = new ReentrantLock();

    private Dao<String> dao;
    private @Nullable LinkEventPublisher publisher;

    LinksHandler(Dao<String> dao, BasicAuth auth, int port) {
        this.dao = dao;
        this.auth = auth;
        this.port = port;
    }

    void setLinksDao(Dao<String> dao) {
        this.dao = dao;
    }

    void setPublisher(LinkEventPublisher publisher) {
        this.publisher = publisher;
    }

    void deleteRejectedLink(String id) throws IOException {
        lock.lock();
        try {
            dao.delete(id);
        } catch (NoSuchElementException e) {
            // The link might already have been deleted.
        } finally {
            lock.unlock();
        }
    }

    private boolean isValidUrl(String url) {
        try {
            URI uri = URI.create(url);
            return ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) && uri.getHost() != null;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean isValidId(String id) {
        if (id.length() != ID_LENGTH) {
            return false;
        }

        for (int i = 0; i < ID_LENGTH; i++) {
            if (ALPHABET.indexOf(id.charAt(i)) == -1) {
                return false;
            }
        }

        return true;
    }

    private String randomId() throws IOException {
        StringBuilder id = new StringBuilder(ID_LENGTH);
        boolean exists = true;

        do {
            id.setLength(0);

            for (int i = 0; i < ID_LENGTH; i++) {
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

        if (publisher != null) {
            publisher.publish(new LinkCreatedEvent(id, body, Instant.now()));
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

    void handleRedirect(HttpExchange exchange, String id) throws IOException {
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

    void handleLinks(HttpExchange exchange, String method, String path) throws IOException {

        if (!auth.checkAuthorization(exchange)) {
            return;
        }

        if (METHOD_POST.equals(method) && LINKS_PATH.equals(path)) {
            handleCreate(exchange);
            return;
        }

        if (!path.startsWith(LINKS_PREFIX)) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        String id = path.substring(LINKS_PREFIX.length());

        switch (method) {
            case METHOD_GET -> handleGet(exchange, id);
            case METHOD_PUT -> handleUpdate(exchange, id);
            case METHOD_DELETE -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(404, -1);
        }
    }

    boolean isLinksPath(String path) {
        return LINKS_PATH.equals(path) || path.startsWith(LINKS_PREFIX);
    }
}
