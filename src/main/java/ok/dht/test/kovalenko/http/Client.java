package ok.dht.test.kovalenko.http;

import ok.dht.test.kovalenko.utils.HttpUtils;
import ok.dht.test.kovalenko.utils.MyHttpSession;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class Client {

    public static final Client INSTANSE = new Client();
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private final HttpClient javaNetClient;

    private Client() {
        javaNetClient = HttpClient.newBuilder()
                .build();
    }

    public CompletableFuture<HttpResponse<byte[]>> get(String uri) {
        HttpRequest httpRequest = request(uri, false).GET().build();
        return sendAsync(httpRequest);
    }

    public CompletableFuture<HttpResponse<byte[]>> get(String url, MyHttpSession session,
                                                       boolean isRequestForReplica)
            throws IOException, InterruptedException {
        return get(url + "/v0/entity?id=" + session.getRequestId() + session.getReplicas().toHttpString());
    }

    public CompletableFuture<HttpResponse<byte[]>> put(String uri, byte[] data) {
        HttpRequest httpRequest = request(uri, false).PUT(HttpRequest.BodyPublishers.ofByteArray(data)).build();
        return sendAsync(httpRequest);
    }

    public CompletableFuture<HttpResponse<byte[]>> put(String url, byte[] data, MyHttpSession session,
                                                       boolean isRequestForReplica)
            throws IOException, InterruptedException {
        return put(url + "/v0/entity?id=" + session.getRequestId() + session.getReplicas().toHttpString(), data);
    }

    public CompletableFuture<HttpResponse<byte[]>> delete(String url, MyHttpSession session,
                                                          boolean isRequestForReplica)
            throws IOException, InterruptedException {
       return delete(url + "/v0/entity?id=" + session.getRequestId() + session.getReplicas().toHttpString());
    }

    public CompletableFuture<HttpResponse<byte[]>> delete(String uri) {
        HttpRequest httpRequest = request(uri, false).DELETE().build();
        return sendAsync(httpRequest);
    }

    private HttpRequest.Builder request(String uri, boolean isRequestForReplica) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uri)).timeout(TIMEOUT);
        if (isRequestForReplica) {
            builder.header(HttpUtils.REPLICA_HEADER, "");
        }
        return builder;
    }

    private CompletableFuture<HttpResponse<byte[]>> sendAsync(HttpRequest request) {
        return javaNetClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
    }

}
