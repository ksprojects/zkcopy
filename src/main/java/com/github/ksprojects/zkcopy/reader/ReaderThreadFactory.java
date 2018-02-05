package com.github.ksprojects.zkcopy.reader;

import java.util.concurrent.ThreadFactory;


final class ReaderThreadFactory implements ThreadFactory {

    private final String hostPort;
    private int timeout;

    ReaderThreadFactory(String hostPort, int timeout) {
        this.hostPort = hostPort;
        this.timeout = timeout;
    }

    public Thread newThread(Runnable r) {
        return new ReaderThread(r, hostPort, timeout);
    }
}