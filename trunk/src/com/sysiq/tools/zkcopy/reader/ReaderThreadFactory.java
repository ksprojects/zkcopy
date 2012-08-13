package com.sysiq.tools.zkcopy.reader;

import java.util.concurrent.ThreadFactory;


public class ReaderThreadFactory implements ThreadFactory {
    
    private final String hostPort;
    
    public ReaderThreadFactory(String hostPort) {
        this.hostPort = hostPort;
    }
    
    public Thread newThread(Runnable r) {
      return new ReaderThread(r, hostPort);
    }    
}