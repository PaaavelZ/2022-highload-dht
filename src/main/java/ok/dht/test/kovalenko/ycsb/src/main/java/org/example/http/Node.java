package org.example.http;

import one.nio.http.Request;
import org.example.utils.HttpUtils;
import org.example.utils.MyHttpSession;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Node {

    private final String selfUrl;
    private boolean isIll = false;

    public Node(String selfUrl) {
        this.selfUrl = selfUrl;
    }

    public CompletableFuture<HttpResponse<byte[]>> proxyRequest(Request request, MyHttpSession session)
            throws ExecutionException, InterruptedException, IllegalAccessException, IOException {
        if (isIll()) {
            throw new IllegalAccessException("Node is ill!");
        }

        return switch (request.getMethod()) {
            case Request.METHOD_GET -> HttpUtils.CLIENT.get(selfUrl, session, true);
            case Request.METHOD_PUT -> HttpUtils.CLIENT.put(selfUrl, request.getBody(), session, true);
            case Request.METHOD_DELETE -> HttpUtils.CLIENT.delete(selfUrl, session, true);
            default -> throw new IllegalArgumentException("Unexpected request method to be proxied: "
                    + request.getMethod());
        };
    }

    public boolean isIll() {
        return isIll;
    }

    public void setIll(boolean isIll) {
        this.isIll = isIll;
    }

    public String selfUrl() {
        return this.selfUrl;
    }
}
