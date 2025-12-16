package com.github.ksprojects.zkcopy.replicator;

import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZkEventExecutor implements Runnable {
    private static final Logger log = Logger.getLogger(ZkEventExecutor.class);

    private final ZkEventProvider eventProvider;
    private final ZkEventHandler eventHandler;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isPaused;
    private final ExecutorService executor;

    public ZkEventExecutor(ZkEventProvider eventProvider, ZkEventHandler eventHandler, AtomicBoolean isRunning, AtomicBoolean isPaused) {
        this.eventProvider = eventProvider;
        this.eventHandler = eventHandler;
        this.isRunning = isRunning;
        this.isPaused = isPaused;
        this.executor = Executors.newFixedThreadPool(1);
    }

    public void start(){
        log.info("Starting zookeeper events executor service...");

        executor.submit(this);

        log.info("Zookeeper events executor service started.");
    }

    @Override
    public void run(){
        while (isRunning.get()){
            try {
                if (isPaused.get()) continue;

                var event = eventProvider.provide();

                eventHandler.handle(event);
            } catch (Exception e){
                log.error("Critical error while handling zk event.", e);
            }
        }
    }
}
