package company.vk.edu.distrib.compute.ruavee.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    private final HttpServer server;
    private final Dao<String> dao;
    private final Dao<String> usersDao;

    public UrlShortenerServiceImpl(int port) throws IOException {
        this.dao = new InMemoryDao();
        this.usersDao = new InMemoryDao();

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        BasicAuth auth = new BasicAuth(usersDao);
        server.createContext("/", new UrlShortenerHandler(dao, auth, port));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }
}
