package company.vk.edu.distrib.compute.ruavee.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Files;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    private final HttpServer server;

    public UrlShortenerServiceImpl(int port) throws IOException {
        Path storageDir = Path.of("build", "ruavee-storage");
        Files.createDirectories(storageDir);

        Path linksPath = storageDir.resolve("links" + port + ".db");
        Path usersPath = storageDir.resolve("users" + port + ".db");

        Dao<String> linksDao = new PersistentDao(linksPath);
        Dao<String> usersDao = new PersistentDao(usersPath);

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        BasicAuth auth = new BasicAuth(usersDao);
        server.createContext("/", new UrlShortenerHandler(linksDao, auth, port));
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
