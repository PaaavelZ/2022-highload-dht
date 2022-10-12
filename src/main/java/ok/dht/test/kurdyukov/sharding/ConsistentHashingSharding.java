package ok.dht.test.kurdyukov.sharding;

import one.nio.util.Hash;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsistentHashingSharding implements Sharding {
    private static final int SALT = 7619;

    private final List<Point> circleHashes;
    private final int size;

    public ConsistentHashingSharding(
            List<String> clusterUrls,
            int countPointsForNode
    ) {
        circleHashes = clusterUrls
                .stream()
                .flatMap(
                        url -> IntStream
                                .range(0, countPointsForNode)
                                .mapToObj(pointIndex ->
                                        new Point(
                                                Hash.murmur3(String.format("%s_node_%d", url, pointIndex)),
                                                url
                                        )
                                )
                )
                .sorted(Comparator.comparingInt(Point::hash))
                .collect(Collectors.toList());
        size = circleHashes.size();
    }

    @Override
    public String getShardUrlByKey(final String key) {
        int l = -1;
        int r = size;

        int hashKey = Hash.murmur3(key);

        while (r - l > 1) {
            int m = (r + l) / 2;

            if (circleHashes.get(m).hash < hashKey) {
                l = m;
            } else {
                r = m;
            }
        }

        return r == size ? circleHashes.get(0).url : circleHashes.get(r).url;
    }

    private record Point(int hash, String url) {
    }
}
