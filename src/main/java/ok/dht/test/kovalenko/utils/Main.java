package ok.dht.test.kovalenko.utils;

import ok.dht.ServiceConfig;
import ok.dht.test.kovalenko.http.MyServiceBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final List<Integer> ports = List.of(19234, 19235, 19236);
    private static final List<String> urls = ports.stream().map(p -> "http://localhost:" + p).toList();
    private static final List<ServiceConfig> configs = new ArrayList<>(urls.size());

    static {
        for (int i = 0; i < urls.size(); ++i) {
            try {
                int port = ports.get(i);
                String url = urls.get(i);
                java.nio.file.Path cfgPath
                        = java.nio.file.Path.of("/home/pavel/IntelliJIdeaProjects/tables/shard" + (i + 1));
                Files.createDirectories(cfgPath);
                ServiceConfig cfg = new ServiceConfig(
                        port,
                        url,
                        urls,
                        cfgPath
                );
                configs.add(cfg);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Main() {
    }

    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(args[0]);
            String url = args[1];
            List<String> clusterUrls = List.of(args[2].split(", "));
            Path workingDir = Path.of(args[3]);
            ServiceConfig cfg = new ServiceConfig(port, url, clusterUrls, workingDir);
            MyServiceBase service = new MyServiceBase(cfg);
            service.start().get(1, TimeUnit.SECONDS);

            log.debug("Socket " + url + " has started successfully");
        } catch (Exception e) {
            log.error("Fatal error", e);
            throw new RuntimeException(e);
        }
    }

    public static void main(int serviceOrdinal) {
        try {
            MyServiceBase service = new MyServiceBase(configs.get(serviceOrdinal - 1));
            service.start().get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Socket wasn't started: {}", urls.get(serviceOrdinal - 1), e);
            throw new RuntimeException(e);
        }
    }
}
