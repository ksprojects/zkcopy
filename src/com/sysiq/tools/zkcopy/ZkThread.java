package com.sysiq.tools.zkcopy;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZkThread extends Thread implements Watcher {
    
    private ZooKeeper zk = null;
    
    public ZkThread(Runnable r, String hostPort) {
        super(r);
        try
        {
            zk = new ZooKeeper(hostPort, 3000, this);
        }
        catch(IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @Override
    public void process(WatchedEvent event)
    {
        // TODO Auto-generated method stub
        
    }
    
    public ZooKeeper getZooKeeper() {
        return zk;
    }
}