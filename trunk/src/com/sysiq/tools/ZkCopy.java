package com.sysiq.tools;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.sysiq.tools.zkcopy.*;

public class ZkCopy implements Watcher
{
    
    private int threadsNumber = 1;
    
    private String znode;    
    private String source;
    private ZooKeeper zkOut;
    
    /**
     * @param args[0] - source ZooKeeper server
     * @param args[1] - destination ZooKeeper server
     * @param args[2] - path to copy
     * @param args[3] - threads count
     */
    public static void main(String[] args)
    {
        // TODO Auto-generated method stub
        String source = args[0];
        String destination = args[1];
        String znode = args[2];                
        try {
            ZkCopy proc = new ZkCopy(source, destination, znode);
            if (args.length >= 4) {
                proc.setThreadsNumber(args[3]);
            }
            proc.executeThreads();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public ZkCopy(String source, String destination, String znode) throws IOException, KeeperException, InterruptedException {
        this.source = source;
        this.znode = znode;
        zkOut = new ZooKeeper(destination, 3000, this);
        checkCreatePath(zkOut, znode);
    }  
    
    private void setThreadsNumber(String s) {
        Integer n = Integer.valueOf(s);
        if (n > 0 && n <= 100) {
            threadsNumber = n;
        }
    }
    
    public void checkCreatePath(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        String[] l = path.split("/");
        StringBuffer b = new StringBuffer();
        for(int i=1; i<l.length; i++) {            
            b.append('/');
            b.append(l[i]);
            System.out.println("CCP: " + b.toString());
            Stat stat = zk.exists(b.toString(), false);
            if (stat == null) {
                zk.create(b.toString(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }
    
    public void executeThreads() throws KeeperException, InterruptedException {
        checkCreatePath(zkOut, znode);        
        ExecutorService pool = Executors.newFixedThreadPool(threadsNumber, new ZkThreadFactory(source));        
        AtomicInteger totalCounter = new AtomicInteger(0);
        AtomicInteger processedCounter = new AtomicInteger(0);
        pool.execute(new ZNodeWalker(zkOut, znode, pool, totalCounter, processedCounter));
        try
        {
            while(true) {
                if (pool.awaitTermination(1, TimeUnit.SECONDS)) {
                    System.out.println("complete");
                    break;
                } 
                System.out.println("total=" + totalCounter + " processed=" + processedCounter);
                if (totalCounter.get() == processedCounter.get()) {
                    // all work finished
                    pool.shutdown();
                }
            }
        }
        catch(InterruptedException e)
        {
            System.out.println("Await Termination of pool was unsuccessful: " + e.getMessage());
        }
    }

    @Override
    public void process(WatchedEvent event)
    {
        // TODO Auto-generated method stub
        
    }

}


