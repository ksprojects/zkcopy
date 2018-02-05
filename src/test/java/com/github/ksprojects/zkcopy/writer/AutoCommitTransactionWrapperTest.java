package com.github.ksprojects.zkcopy.writer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class AutoCommitTransactionWrapperTest {

    private static final int TRANSACTION_SIZE = 10;

    @Test
    public void testAutoCommit() throws InterruptedException, KeeperException {
        ZooKeeper mockZK = mock(ZooKeeper.class);
        Transaction transaction = mock(Transaction.class);
        when(mockZK.transaction()).thenReturn(transaction);
        AutoCommitTransactionWrapper wrapper = new AutoCommitTransactionWrapper(mockZK, TRANSACTION_SIZE);
        for(int i = 0; i < TRANSACTION_SIZE * 50; i++) {
            wrapper.create("/test/blah", new byte[] {0x0, 0x0}, null, CreateMode.PERSISTENT);
        }
        verify(transaction, times(50)).commit();
    }

    @Test
    public void testManualCommit() throws InterruptedException, KeeperException {
        ZooKeeper mockZK = mock(ZooKeeper.class);
        Transaction transaction = mock(Transaction.class);
        when(mockZK.transaction()).thenReturn(transaction);
        AutoCommitTransactionWrapper wrapper = new AutoCommitTransactionWrapper(mockZK, TRANSACTION_SIZE);
        for(int i = 0; i < TRANSACTION_SIZE - 1; i++) {
            wrapper.create("/test/blah", new byte[] {0x0, 0x0}, null, CreateMode.PERSISTENT);
        }
        verify(transaction, never()).commit();
        
        wrapper.commit();
        
        verify(transaction, times(1)).commit();

    }

}
