package com.github.ksprojects.zkcopy.replicator;

import com.github.ksprojects.zkcopy.metric.SyncReplicatorMetricsManager;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ZkLocalEventBroker implements ZkEventTemplate, ZkEventProvider{
    private static final Logger log = Logger.getLogger(ZkLocalEventBroker.class);

    private final SyncReplicatorMetricsManager metricsManager;

    private final BlockingQueue<ZkEvent> eventQueue = new LinkedBlockingQueue<>();

    public ZkLocalEventBroker(SyncReplicatorMetricsManager metricsManager) {
        this.metricsManager = metricsManager;
    }

    @Override
    public ZkEvent provide() throws InterruptedException {
        log.debug("Taking next event...");

        var event = eventQueue.take();
        metricsManager.countHandledEvents();

        return event;
    }

    @Override
    public void send(ZkEvent event) {
        log.debug("Received new event: " + event);

        eventQueue.add(event);
        metricsManager.countReceivedEvents();
    }
}
