package com.github.ksprojects.zkcopy.metric;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MetricsPusher {
    private static final Logger log = Logger.getLogger(MetricsPusher.class);
    private final VictoriaMetricsClient vmClient;

    private final PrometheusMeterRegistry registry;

    private final int pushingRate;

    public MetricsPusher(VictoriaMetricsClient victoriaMetricsClient, PrometheusMeterRegistry registry, int pushingRate) {
        this.vmClient = victoriaMetricsClient;
        this.registry = registry;
        this.pushingRate = pushingRate;
    }

    public void start() {
        Executors.newScheduledThreadPool(1)
            .scheduleAtFixedRate(() -> {
                log.debug("Pushing metrics...");
                vmClient.pushMetrics(registry);
            }, pushingRate, pushingRate, TimeUnit.SECONDS);
    }
}
