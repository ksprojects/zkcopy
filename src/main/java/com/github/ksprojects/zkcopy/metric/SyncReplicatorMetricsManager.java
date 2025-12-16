package com.github.ksprojects.zkcopy.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SyncReplicatorMetricsManager {
    private static final long INITIALIZED_AT = System.currentTimeMillis();
    public static final String ZK_REPLICATION_METRIC = "ucp_zk_replication_requests";
    public static final String ZK_EVENTS_BROKER_RECEIVED_EVENTS_METRIC = "ucp_zk_replication_received_events";
    public static final String ZK_EVENTS_BROKER_HANDLED_EVENTS_METRIC = "ucp_zk_replication_handled_events";
    public static final String ZK_CONNECTED_GAUGE_METRIC = "ucp_zk_replication_connected_time";
    public static final String ZK_UP_TIME_GAUGE_METRIC = "ucp_zk_replication_up_time";
    public static final String ZK_FREE_MEM_GAUGE_METRIC = "ucp_zk_replication_memory_free";
    public static final String ZK_USED_MEM_GAUGE_METRIC = "ucp_zk_replication_memory_used";
    public static final String ZK_MAX_MEM_GAUGE_METRIC = "ucp_zk_replication_memory_max";
    public static final String DZEN_DATASOURCE_NAME = "dzen";

    private final PrometheusMeterRegistry registry;
    public final String ctype;
    public final String cloud;

    public SyncReplicatorMetricsManager(PrometheusMeterRegistry prometheusMeterRegistry, String ctype, String cloud) {
        this.registry = prometheusMeterRegistry;
        this.ctype = ctype;
        this.cloud = cloud;
    }

    public void countCreation(boolean isSource, String serviceNode){
        Counter counter = Counter.builder(ZK_REPLICATION_METRIC)
            .tag("from", isSource ? "source" : "target")
            .tag("type", "creation")
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("serviceNode", serviceNode)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);

        counter.increment();
    }

    public void countDataChanged(boolean isSource, String serviceNode){
        Counter counter = Counter.builder(ZK_REPLICATION_METRIC)
            .tag("from", isSource ? "source" : "target")
            .tag("type", "dataChanged")
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("serviceNode", serviceNode)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);

        counter.increment();
    }

    public void countDeletion(boolean isSource, String serviceNode){
        Counter counter = Counter.builder(ZK_REPLICATION_METRIC)
            .tag("from", isSource ? "source" : "target")
            .tag("type", "deletion")
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("serviceNode", serviceNode)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);

        counter.increment();
    }

    public void countReceivedEvents(){
        Counter counter = Counter.builder(ZK_EVENTS_BROKER_RECEIVED_EVENTS_METRIC)
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);

        counter.increment();
    }

    public void countHandledEvents(){
        Counter counter = Counter.builder(ZK_EVENTS_BROKER_HANDLED_EVENTS_METRIC)
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);

        counter.increment();
    }

    public void initConnectionGauge(AtomicLong connectedAt, AtomicBoolean isPaused){
        Gauge.builder(ZK_CONNECTED_GAUGE_METRIC, ()
            -> isPaused.get() ? 0 : System.currentTimeMillis() - connectedAt.get())
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);
    }

    public void initUpTimeGauge(){
        Gauge.builder(ZK_UP_TIME_GAUGE_METRIC, ()
            -> System.currentTimeMillis() - INITIALIZED_AT)
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);
    }

    public void initUsedMemoryGauge(){
        Gauge.builder(ZK_USED_MEM_GAUGE_METRIC, ()
            -> Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);
    }

    public void initFreeMemoryGauge(){
        Gauge.builder(ZK_FREE_MEM_GAUGE_METRIC, ()
            -> Runtime.getRuntime().freeMemory())
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);
    }

    public void initMaxMemoryGauge(){
        Gauge.builder(ZK_MAX_MEM_GAUGE_METRIC, ()
            -> Runtime.getRuntime().maxMemory())
            .tag("ctype", ctype)
            .tag("cloud", cloud)
            .tag("cloud_namespace", DZEN_DATASOURCE_NAME)
            .register(registry);
    }
}
