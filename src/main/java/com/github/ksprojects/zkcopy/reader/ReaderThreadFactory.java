package com.github.ksprojects.zkcopy.reader;

import java.util.concurrent.ThreadFactory;


final class ReaderThreadFactory implements ThreadFactory {

    private final String hostPort;

    ReaderThreadFactory(String hostPort) {
        this.hostPort = hostPort;
    }

    public Thread newThread(Runnable r) {
        return new ReaderThread(r, hostPort);
    }
}