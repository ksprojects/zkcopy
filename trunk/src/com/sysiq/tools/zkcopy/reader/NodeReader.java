package com.sysiq.tools.zkcopy.reader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.sysiq.tools.zkcopy.Node;

public class NodeReader implements Runnable {
    
    private final Node znode;
    private final ExecutorService pool;
    private final AtomicInteger totalCounter;
    private final AtomicInteger processedCounter;
    
    private static Logger logger = Logger.getLogger(NodeReader.class);
    
    public NodeReader(ExecutorService pool, Node znode, AtomicInteger totalCounter, AtomicInteger processedCounter) {
        this.znode = znode;
        this.pool = pool;
        this.totalCounter = totalCounter;
        this.processedCounter = processedCounter;
        totalCounter.incrementAndGet();
    }

    @Override
    public void run()
    {
        try {
            ReaderThread thread = (ReaderThread)Thread.currentThread();
            ZooKeeper zk = thread.getZooKeeper();
            Stat stat = null;
            String path = znode.getAbsolutePath();
            logger.debug("Reading node " + path);
            byte[] data = zk.getData(path, false, stat);
            znode.setData(data);
            List<String> children = zk.getChildren(path, false);
            for(String child:children) {
                if ("zookeeper".equals(child)) {
                    // reserved
                    continue;
                }
                Node zchild= new Node(znode, child);
                znode.appendChild(zchild);
                pool.execute(new NodeReader(pool, zchild, totalCounter, processedCounter));
            }       
        }
        catch (KeeperException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            processedCounter.incrementAndGet();
        }
    }    
}