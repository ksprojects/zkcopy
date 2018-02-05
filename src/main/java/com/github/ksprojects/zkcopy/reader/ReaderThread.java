package com.github.ksprojects.zkcopy.reader;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.log4j.Logger;

final class ReaderThread extends Thread implements Watcher {

    private ZooKeeper zk = null;
    private static Logger logger = Logger.getLogger(ReaderThread.class);

    ReaderThread(Runnable r, String hostPort) {
        super(r);
        try {
            zk = new ZooKeeper(hostPort, 40000, this);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub

    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    @SuppressWarnings("checkstyle:nofinalizer")
    protected void finalize() throws Throwable {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (InterruptedException e) {
            logger.error("Exception caught while closing session", e);
        } finally {
            super.finalize();
        }
    }

}
