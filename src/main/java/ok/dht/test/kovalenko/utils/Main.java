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

    private Main() {
    }

    public static void main(String[] args) {
        try {
            String url = args[0];
            int port = Integer.parseInt(url.split(":")[1]);
            List<String> clusterUrls = List.of(args[1].split(", "));
            Path workingDir = Path.of(args[2]);
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
            int port = ports.get(serviceOrdinal - 1);
            String url = urls.get(serviceOrdinal - 1);
            java.nio.file.Path cfgPath = Files.createTempDirectory("shards").resolve("shard" + serviceOrdinal);
            ServiceConfig cfg = new ServiceConfig(
                    port,
                    url,
                    urls,
                    cfgPath
            );
            MyServiceBase service = new MyServiceBase(cfg);
            service.start().get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Socket wasn't started: {}", urls.get(serviceOrdinal - 1), e);
            throw new RuntimeException(e);
        }
    }
}
