package com.github.ksprojects.zkcopy.metric;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MetricsPusher {
    private static final Logger log = Logger.getLogger(MetricsPusher.class);
    private final VictoriaMetricsHttpClient vmClient;

    private final PrometheusMeterRegistry registry;

    private final int pushingRate;

    public MetricsPusher(VictoriaMetricsHttpClient victoriaMetricsHttpClient, PrometheusMeterRegistry registry, int pushingRate) {
        this.vmClient = victoriaMetricsHttpClient;
        this.registry = registry;
        this.pushingRate = pushingRate;
    }

    public void start() {
        Executors.newScheduledThreadPool(1)
            .scheduleAtFixedRate(() -> {
                try {
                    log.debug("Pushing metrics...");
                    vmClient.pushMetrics(registry);
                } catch (Exception ex){
                    log.error("Error while pushing metrics...");
                    log.error(ex.getMessage(), ex);
                }
            }, pushingRate, pushingRate, TimeUnit.SECONDS);
    }
}
