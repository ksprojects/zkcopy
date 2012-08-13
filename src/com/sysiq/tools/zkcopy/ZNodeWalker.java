package com.sysiq.tools.zkcopy;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZNodeWalker implements Runnable {
    
    private final String znode;
    private final ExecutorService pool;
    private final AtomicInteger totalCounter;
    private final AtomicInteger processedCounter;
    private final ZooKeeper zkOut;
    
    public ZNodeWalker(ZooKeeper zkOut, String znode, ExecutorService pool, AtomicInteger totalCounter, AtomicInteger processedCounter) {
        this.znode = znode;
        this.pool = pool;
        this.totalCounter = totalCounter;
        this.processedCounter = processedCounter;
        this.zkOut = zkOut;
        totalCounter.incrementAndGet();
    }

    @Override
    public void run()
    {
        try {
            ZkThread thread = (ZkThread)Thread.currentThread();
            ZooKeeper zk = thread.getZooKeeper();
//            Stat stat = null;            
//            stat = zk.exists(znode, false);   
//            if (stat != null) {
                sync(zk, znode);
                List<String> children = null;
                children = zk.getChildren(znode, false);
                for(String child:children) {
                    if ("zookeeper".equals(child)) {
                        // reserved
                        continue;
                    }
                    if ("/".equals(znode)) {
                        push(znode + child);
                    } else {
                        push(znode + "/" + child);
                    }
                }
//            } else {
//                System.out.print("Node " + znode + " doesn't exist");
//            }        
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
    
    private void push(String node) {;
        pool.execute(new ZNodeWalker(zkOut, node, pool, totalCounter, processedCounter));
    }
    
    private void sync(ZooKeeper zk, String node) throws KeeperException, InterruptedException {
        
        Stat stat = zkOut.exists(node, false);
//        List<ACL> acl = zk.getACL(node, stat);
        List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
        byte[] data = zk.getData(node, false, stat);
        if (stat == null) {
            zkOut.create(node, data, acl, CreateMode.PERSISTENT);
        } else {
//            zkOut.setACL(node, acl, stat.getVersion());
            zkOut.setData(node, data, -1);
        }
    }
}