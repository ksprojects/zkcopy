package com.sysiq.tools.zkcopy;

import java.util.concurrent.ThreadFactory;

public class ZkThreadFactory implements ThreadFactory {
    
    private final String hostPort;
    
    public ZkThreadFactory(String hostPort) {
        this.hostPort = hostPort;
    }
    
    public Thread newThread(Runnable r) {
      return new ZkThread(r, hostPort);
    }    
}