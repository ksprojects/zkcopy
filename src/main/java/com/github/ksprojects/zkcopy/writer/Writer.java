package com.github.ksprojects.zkcopy.writer;

import com.github.ksprojects.zkcopy.Node;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Writer {
    static Logger logger = Logger.getLogger(Writer.class);
    
    private Node sourceRoot;
    private String destPath;
    private ZooKeeper zk;
    private boolean ignoreEphemeralNodes;
    private boolean removeDeprecated;
    private long ephemeralIgnored = 0;
    private long deletedEphemeral = 0;
    private long nodesUpdated = 0;
    private long nodesCreated = 0;
    private long nodesSkipped = 0;
    private long mtime;
    private long maxMtime;
    private Transaction transaction;
    private int batchSize;

    /**
     * Create new {@link Writer} instance.
     *
     * @param zk
     *            Zookeeper server
     * @param znode
     *            root node to copy data from
     * @param removeDeprecatedNodes
     *            {@code true} if nodes that does not exist in source should be
     *            removed
     * @param ignoreEphemeralNodes
     *            {@code true} if ephemeral nodes should not be copied
     * @param mtime
     *            znodes modified before this timestamp will not be copied.
     */
    public Writer(ZooKeeper zk, String destPath, Node znode, boolean removeDeprecatedNodes, boolean ignoreEphemeralNodes, long mtime, int batchSize) {
        this.zk = zk;
        this.destPath = destPath;
        this.sourceRoot = znode;
        this.removeDeprecated = removeDeprecatedNodes;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.mtime = mtime;
        this.batchSize = batchSize;
    }
    
    /**
     * Start process of writing data to the target.
     */
    public void write() {
        try {
            Node dest = sourceRoot;
            dest.setPath(destPath);
            logger.info("Writing data...");
            transaction = new AutoCommitTransactionWrapper(zk, batchSize);
            update(dest);
            transaction.commit();
            logger.info("Writing data completed.");
            logger.info("Wrote " + (nodesCreated + nodesUpdated) + " nodes");
            logger.info("Created " + nodesCreated + " nodes; Updated " + nodesUpdated + " nodes");
            logger.info("Ignored " + ephemeralIgnored + " ephemeral nodes");
            logger.info("Skipped " + nodesSkipped + " nodes older than " + mtime);
            logger.info("Max mtime of copied nodes: " + maxMtime);
            if (deletedEphemeral > 0) {
                logger.info("Deleted " + deletedEphemeral + " ephemeral nodes");
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Exception caught while writing nodes", e);
        }
    }

    private void update(Node node) throws KeeperException, InterruptedException {
        String path = node.getAbsolutePath();
        if (ignoreEphemeralNodes && node.isEphemeral()) {
            ephemeralIgnored++;
            Stat stat = zk.exists(path, false);
            // only delete ephemeral nodes if they've been copied over persistently before
            if (stat != null && stat.getEphemeralOwner() == 0) {
                transaction.delete(path, stat.getVersion());
                deletedEphemeral++;
            }
            return;
        }

        if (node.getMtime() > mtime) {
            upsertNode(node);
            maxMtime = Math.max(node.getMtime(), maxMtime);
        } else {
            nodesSkipped++;
        }

        // 2. Recursively update or create children
        for (Node child : node.getChildren()) {
            update(child);
        }

        if (removeDeprecated) {
            // 3. Remove deprecated children
            try {
                List<String> destChildren = zk.getChildren(path, false);
                for (String child : destChildren) {
                    if (!node.getChildrenNamed().contains(child)) {
                        delete(node.getAbsolutePath() + "/" + child);
                    }
                }
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NONODE) {
                    // If there was no such node before this transaction started, then it can't have
                    // any children and is therefore safe to ignore
                    return;
                }
                throw e;
            }
        }
    }

    /**
     * Updates or creates the given node.
     * 
     * @param node
     *            The node to copy
     * @throws KeeperException
     *             If the server signals an error
     * @throws InterruptedException
     *             If the server transaction is interrupted
     */
    private void upsertNode(Node node) throws KeeperException, InterruptedException {
        String nodePath = node.getAbsolutePath();
        // 1. Update or create current node
        Stat stat = zk.exists(nodePath, false);
        if (stat != null) {
            logger.debug("Attempting to update " + nodePath);
            transaction.setData(nodePath, node.getData(), -1);
            nodesUpdated++;
        } else {
            logger.debug("Attempting to create " + nodePath);
            transaction.create(nodePath, node.getData(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            nodesCreated++;
        }
        if (nodesUpdated % 100 == 0) {
            logger.debug(String.format("Updated: %s, current node mtime %s", nodesUpdated, node.getMtime()));
        }
    }

    private void delete(String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            delete(path + "/" + child);
        }
        transaction.delete(path, -1);
        logger.info("Deleted node " + path);
    }

}
