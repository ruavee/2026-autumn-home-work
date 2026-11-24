package company.vk.edu.distrib.compute.ruavee.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;

public class UrlShortenerHandler implements HttpHandler {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_POST = "POST";
    private static final String STATUS_PATH = "/v0/status";
    private static final String USERS_PATH = "/internal/users";
    private static final String ROOT_PATH = "/";

    private final LinksHandler linksHandler;
    private final BasicAuth auth;

    public UrlShortenerHandler(Dao<String> dao, BasicAuth auth, int port) {
        this.auth = auth;
        this.linksHandler = new LinksHandler(dao, auth, port);
    }

    void setLinksDao(Dao<String> dao) {
        linksHandler.setLinksDao(dao);
    }

    void setPublisher(LinkEventPublisher publisher) {
        linksHandler.setPublisher(publisher);
    }

    void deleteRejectedLink(String id) throws IOException {
        linksHandler.deleteRejectedLink(id);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();

            if (METHOD_GET.equals(method) && STATUS_PATH.equals(path)) {
                exchange.sendResponseHeaders(200, -1);
            } else if (linksHandler.isLinksPath(path)) {
                linksHandler.handleLinks(exchange, method, path);
            } else if (METHOD_POST.equals(method) && USERS_PATH.equals(path)) {
                auth.handleCreateUser(exchange);
            } else if (METHOD_GET.equals(method) && path.startsWith(ROOT_PATH)) {
                String id = path.substring(ROOT_PATH.length());
                linksHandler.handleRedirect(exchange, id);
            } else {
                exchange.sendResponseHeaders(404, -1);
            }
        }
    }
}
