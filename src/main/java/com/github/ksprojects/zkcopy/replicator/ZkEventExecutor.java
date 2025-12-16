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
        executor.submit(this);
    }

    @Override
    public void run(){
        while (isRunning.get()){
            try {
                if (isPaused.get()) continue;

                var event = eventProvider.provide();

                eventHandler.handle(event);
            } catch (Exception e){
                log.error("Error while handling zk event.", e);
            }
        }
    }
}
