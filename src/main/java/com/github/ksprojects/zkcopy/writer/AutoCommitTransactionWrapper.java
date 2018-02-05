package com.github.ksprojects.zkcopy.writer;

import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * A ZK Transaction Wrapper that automatically commits your transaction and
 * transparently creates the next one every {@link #transactionSize} operations.
 */
class AutoCommitTransactionWrapper extends Transaction {

    private Transaction transaction;
    int transactionSize;
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
        if (opsSinceCommit >= (transactionSize - 1)) {
            try {
                Writer.logger.info("Committing transaction");
                transaction.commit();
                opsSinceCommit = 0;
                transaction = zk.transaction();
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        } else {
            opsSinceCommit++;
        }

    }

}