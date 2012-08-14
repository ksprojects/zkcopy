package com.sysiq.tools.zkcopy.writer;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.sysiq.tools.zkcopy.Node;

public class Writer implements Watcher
{
    private static Logger logger = Logger.getLogger(Writer.class);
    private Node sourceRoot;
    private String addr;
    private String server;
    private String path;
    private ZooKeeper zk;
    private boolean removeDeprecated;
    
    public Writer(String addr, Node znode, boolean removeDeprecatedNodes) {
        this.addr = addr;
        sourceRoot = znode;
        this.removeDeprecated = removeDeprecatedNodes;
        parseAddr();        
    }
    
    private final void parseAddr() {
        int p = addr.indexOf('/');
        server = addr.substring(0, p);
        path = addr.substring(p);
    }
    
    public void write() {
        try
        {
            zk = new ZooKeeper(server, 3000, this);
            checkCreatePath(path);
            Node dest = sourceRoot;
            dest.setPath(path);
            logger.info("Writing data...");
            update(dest);
            logger.info("Writing data completed.");
        }
        catch(IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch(KeeperException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch(InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void checkCreatePath(String path) throws KeeperException, InterruptedException {
        logger.info("Checking path " + path);
        String[] l = path.split("/");
        StringBuffer b = new StringBuffer();
        for(int i=1; i<l.length; i++) {            
            b.append('/');
            b.append(l[i]);
            System.out.println("CCP: " + b.toString());
            Stat stat = zk.exists(b.toString(), false);
            if (stat == null) {
                zk.create(b.toString(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created " + b.toString());
            }
        }
    }

    private void update(Node node) throws KeeperException, InterruptedException {
        String path = node.getAbsolutePath();
        
        // 1. Update or create current node
        Stat stat = zk.exists(path, false);
        if (stat != null) {
            zk.setData(path, node.getData(), -1);
        } else {
            zk.create(path, node.getData(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        
        // 2. Recursively update or create children
        for(Node child:node.getChildren()) {
            update(child);
        }
        
        if (removeDeprecated) {
            // 3. Remove deprecated children
            List<String> destChildren = zk.getChildren(path, false);
            for(String child: destChildren) {
                if (!node.getChildrenNamed().contains(child)) {
                    delete(node.getAbsolutePath() + "/" + child);
                }
            }
        }
    }
    
    private void delete(String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, false);
        for(String child:children) {
            delete(path + "/" + child);
        }
        zk.delete(path, -1);
        logger.info("Deleted node " + path);
    }
    
    @Override
    public void process(WatchedEvent event)
    {
        // TODO Auto-generated method stub
        
    }
    
}
