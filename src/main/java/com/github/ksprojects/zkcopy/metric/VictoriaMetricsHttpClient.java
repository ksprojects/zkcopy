package com.github.ksprojects.zkcopy.metric;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class VictoriaMetricsHttpClient {
    private static final Logger logger = Logger.getLogger(VictoriaMetricsHttpClient.class);

    private final String url;
    private final int requestTimeoutMs;
    private final HttpClient httpClient;

    public VictoriaMetricsHttpClient(String url, int requestTimeoutMs) {
        this.url = url;
        this.requestTimeoutMs = requestTimeoutMs;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(requestTimeoutMs))
                .build();
    }

    public CompletableFuture<HttpResponse<String>> pushMetrics(PrometheusMeterRegistry registry) {
        String body = registry.scrape();
        return pushMetrics(body);
    }

    public CompletableFuture<HttpResponse<String>> pushMetrics(String metricsBody) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "text/plain; version=0.0.4")
                .POST(HttpRequest.BodyPublishers.ofString(metricsBody))
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .build();

        return sendRequest(request);
    }

    private CompletableFuture<HttpResponse<String>> sendRequest(HttpRequest request) {
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .whenComplete((response, exception) -> {
                if (exception != null) {
                    logger.error("Error while pushing to VictoriaMetrics", exception);
                } else {
                    int statusCode = response.statusCode();
                    if (statusCode < 200 || statusCode >= 300) {
                        logger.error("Error code received [" + statusCode + "] while pushing to VictoriaMetrics");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("status: " + statusCode + " response body: " + response.body());
                    }
                }
            });
    }
}
