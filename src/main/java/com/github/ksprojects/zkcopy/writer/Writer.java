package com.github.ksprojects.zkcopy.writer;

import com.github.ksprojects.zkcopy.Node;
import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Writer implements Watcher {
    private static Logger logger = Logger.getLogger(Writer.class);
    private Node sourceRoot;
    private String addr;
    private String server;
    private String path;
    private ZooKeeper zk;
    private boolean ignoreEphemeralNodes;
    private boolean removeDeprecated;
    private long ephemeralIgnored = 0;
    private long deletedEphemeral = 0;
    private long nodesUpdated = 0;
    private long nodesCreated = 0;

    /**
     * Create new {@link Writer} instance.
     *
     * @param addr address of a server to write data
     * @param znode root node to copy data from
     * @param removeDeprecatedNodes {@code true} if nodes that does
     * not exist in source should be removed
     * @param ignoreEphemeralNodes {@code true} if ephemeral nodes
     * should not be copied
     */
    public Writer(String addr, Node znode, boolean removeDeprecatedNodes, boolean ignoreEphemeralNodes) {
        this.addr = addr;
        sourceRoot = znode;
        this.removeDeprecated = removeDeprecatedNodes;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        parseAddr();
    }

    private void parseAddr() {
        int p = addr.indexOf('/');
        server = addr.substring(0, p);
        path = addr.substring(p);
    }

    /**
     * Start process of writing data to the target.
     */
    public void write() {
        try {
            zk = new ZooKeeper(server, 40000, this);
            checkCreatePath(path);
            Node dest = sourceRoot;
            dest.setPath(path);
            logger.info("Writing data...");
            update(dest);
            logger.info("Writing data completed.");
            logger.info("Wrote " + (nodesCreated + nodesUpdated) + " nodes");
            logger.info("Created " + nodesCreated + " nodes; Updated " + nodesUpdated + " nodes");
            logger.info("Ignored " + ephemeralIgnored + " ephemeral nodes");
            if (deletedEphemeral > 0) {
                logger.info("Deleted " + deletedEphemeral + " ephemeral nodes");
            }

        } catch (IOException | KeeperException | InterruptedException e) {
            logger.error("Exception caught while writing nodes", e);
        } finally {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (InterruptedException e) {
                logger.error("Exception caught while closing session", e);
            }
        }
    }

    private void checkCreatePath(String path) throws KeeperException, InterruptedException {
        logger.info("Checking path " + path);
        String[] l = path.split("/");
        StringBuffer b = new StringBuffer();
        for (int i = 1; i < l.length; i++) {
            b.append('/');
            b.append(l[i]);
            Stat stat = zk.exists(b.toString(), false);
            if (stat == null) {
                zk.create(b.toString(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created " + b.toString());
            }
        }
    }

    private void update(Node node) throws KeeperException, InterruptedException {
        String path = node.getAbsolutePath();
        if (ignoreEphemeralNodes && node.isEphemeral()) {
            ephemeralIgnored++;
            Stat stat = zk.exists(path, false);
            // only delete ephemeral nodes if they've been copied over persistently before
            if (stat != null && stat.getEphemeralOwner() == 0) {
                zk.delete(path, stat.getVersion());
                deletedEphemeral++;
            }
            return;
        }

        // 1. Update or create current node
        Stat stat = zk.exists(path, false);
        if (stat != null) {
            zk.setData(path, node.getData(), -1);
            nodesUpdated++;
        } else {
            zk.create(path, node.getData(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            nodesCreated++;
        }

        // 2. Recursively update or create children
        for (Node child : node.getChildren()) {
            update(child);
        }

        if (removeDeprecated) {
            // 3. Remove deprecated children
            List<String> destChildren = zk.getChildren(path, false);
            for (String child : destChildren) {
                if (!node.getChildrenNamed().contains(child)) {
                    delete(node.getAbsolutePath() + "/" + child);
                }
            }
        }
    }

    private void delete(String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            delete(path + "/" + child);
        }
        zk.delete(path, -1);
        logger.info("Deleted node " + path);
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub

    }

}
