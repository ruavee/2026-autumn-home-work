package company.vk.edu.distrib.compute.ruavee.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    private boolean started;
    private boolean stopped;
    private final UrlShortenerHandler handler;
    private final HttpServer server;
    private @Nullable String kafkaAddress;
    private @Nullable LinkEventPublisher publisher;
    private @Nullable ModerationResultListener resultListener;
    private static final int STOP_DELAY_SECONDS = 1;

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (started || stopped) {
            throw new IllegalStateException();
        }
        handler.setLinksDao(dao);
    }

    public UrlShortenerServiceImpl(int port) throws IOException {
        Path storageDir = Path.of(System.getProperty("java.io.tmpdir"), "ruavee-storage");
        Files.createDirectories(storageDir);

        Path linksPath = storageDir.resolve("links" + port + ".db");
        Path usersPath = storageDir.resolve("users" + port + ".db");

        Dao<String> linksDao = new PersistentDao(linksPath);
        Dao<String> usersDao = new PersistentDao(usersPath);

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        BasicAuth auth = new BasicAuth(usersDao);
        this.handler = new UrlShortenerHandler(linksDao, auth, port);
        server.createContext("/", handler);
    }

    @Override
    public void setKafkaBootstrapServers(String bootstrapServers) {
        if (started || stopped) {
            throw new IllegalStateException();
        }
        this.kafkaAddress = bootstrapServers;
    }

    @Override
    public void start() {
        if (started || stopped) {
            throw new IllegalStateException();
        }
        if (kafkaAddress != null) {
            publisher = new LinkEventPublisher(kafkaAddress);
            handler.setPublisher(publisher);
            resultListener = new ModerationResultListener(kafkaAddress, id -> {
                try {
                    handler.deleteRejectedLink(id);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            resultListener.start();
        }
        server.start();
        started = true;
    }

    @Override
    public void stop() {
        server.stop(STOP_DELAY_SECONDS);
        stopped = true;
        if (resultListener != null) {
            resultListener.close();
        }
        if (publisher != null) {
            publisher.close();
        }
    }
}
