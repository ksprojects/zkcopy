package com.github.ksprojects.zkcopy.reader;

import com.github.ksprojects.zkcopy.LoggingWatcher;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

final class ReaderThreadFactory implements ThreadFactory {
    
    private static final Logger LOG = Logger.getLogger(ReaderThreadFactory.class);

    private final String hostPort;
    private int timeout;

    private Set<ZooKeeper> zkCache = new HashSet<>();

    ReaderThreadFactory(String hostPort, int timeout) {
        this.hostPort = hostPort;
        this.timeout = timeout;
    }

    public Thread newThread(Runnable r) {
        ZooKeeper zooKeeper;
        try {
            zooKeeper = new ZooKeeper(hostPort, timeout, new LoggingWatcher());
        } catch (IOException e) {
            throw new RuntimeException("Cannot connect to source Zookeeper", e);
        }
        zkCache.add(zooKeeper);
        return new ReaderThread(r, zooKeeper);
    }

    public void closeZookeepers() {
        for (ZooKeeper zk : zkCache) {
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    LOG.warn("There was an error closing a source zookeeper connection", e);
                }
            }
        }
    }
}