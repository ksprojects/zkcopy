package com.github.ksprojects.zkcopy.replicator;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ZkEventContainer implements ZkEventTemplate, ZkEventProvider{
    private static final Logger log = Logger.getLogger(ZkEventContainer.class);

    private final BlockingQueue<ZkEvent> eventQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger receivedEvents = new AtomicInteger();
    private final AtomicInteger providedEvents = new AtomicInteger();

    @Override
    public ZkEvent provide() throws InterruptedException {
        log.debug("Taking next event...");
        providedEvents.incrementAndGet();

        return eventQueue.take();
    }

    @Override
    public void send(ZkEvent event) {
        log.debug("Received new event: " + event);
        receivedEvents.incrementAndGet();

        eventQueue.add(event);
    }
}
