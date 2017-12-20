package com.github.ksprojects.zkcopy.writer;

import com.github.ksprojects.zkcopy.Node;
import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class Writer {
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
    private long nodesSkipped = 0;
    private long mtime;
    private long maxMtime;
    private Transaction transaction;
    private int batchSize;

    /**
     * Create new {@link Writer} instance.
     *
     * @param addr
     *            address of a server to write data
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
    public Writer(String addr, Node znode, boolean removeDeprecatedNodes, boolean ignoreEphemeralNodes, long mtime, int batchSize) {
        this.addr = addr;
        sourceRoot = znode;
        this.removeDeprecated = removeDeprecatedNodes;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.mtime = mtime;
        this.batchSize = batchSize;
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
            zk = new ZooKeeper(server, 3000, new LoggingWatcher());
            Node dest = sourceRoot;
            dest.setPath(path);
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

        } catch (IOException | KeeperException | InterruptedException e) {
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

    /**
     * Watcher implementation that simply logs at info.
     */
    private class LoggingWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            logger.info("Ignoring watched event: " + event);
        }
    }

    /**
     * A ZK Transaction Wrapper that automatically commits your transaction and
     * transparently creates the next one every {@link #transactionSize} operations.
     */
    private class AutoCommitTransactionWrapper extends Transaction {

        private Transaction transaction;
        private int transactionSize;
        private int opsSinceCommit = 0;
        private ZooKeeper zk;

        /**
         *
         * @param zk
         *            Zookeeper server to commit transactions to.
         * @param transactionSize
         *            Number of operations to perform before commiting, <em>n.b you will
         *            have to perform you last {@link #commit()} manually </em>
         */
        protected AutoCommitTransactionWrapper(ZooKeeper zk, int transactionSize) {
            super(zk);
            transaction = zk.transaction();
            this.zk = zk;
            this.transactionSize = transactionSize;
        }

        @Override
        public Transaction create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
            maybeCommitTransaction();
            return transaction.create(path, data, acl, createMode);
        }

        @Override
        public Transaction delete(String path, int version) {
            maybeCommitTransaction();
            return transaction.delete(path, version);
        }

        @Override
        public Transaction check(String path, int version) {
            maybeCommitTransaction();
            return transaction.check(path, version);
        }

        @Override
        public Transaction setData(String path, byte[] data, int version) {
            maybeCommitTransaction();
            return transaction.setData(path, data, version);
        }

        @Override
        public List<OpResult> commit() throws InterruptedException, KeeperException {
            return transaction.commit();
        }

        private void maybeCommitTransaction() {
            if (opsSinceCommit >= transactionSize) {
                try {
                    logger.info("Committing transaction");
                    transaction.commit();
                    opsSinceCommit = 0;
                    transaction = zk.transaction();
                } catch (NodeExistsException e) {
                    List<OpResult> results = e.getResults();
                    for (OpResult result : results) {
                        if (result.getType() == ZooDefs.OpCode.error) {
                            Code code = KeeperException.Code.get(((ErrorResult) result).getErr());
                            if (code != Code.RUNTIMEINCONSISTENCY) {
                                logger.warn("Transaction result: " + code);
                            }
                        }
                    }
                    throw new RuntimeException("Node exists: " + e.getPath());
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            } else {
                opsSinceCommit++;
            }

        }

    }

}
