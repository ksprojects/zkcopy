package com.github.ksprojects.zkcopy.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.ksprojects.zkcopy.Node;
import java.util.Arrays;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class WriterTest {
    
    private static final byte[] THEDATA = "the data".getBytes();
    private ZooKeeper mockZK;
    private Node mockNode;
    private Transaction mockTransaction;
    private Stat mockStat;
    private Node mockChildNode;

    @Before
    public void setupMocks() {
        mockZK = mock(ZooKeeper.class);
        mockNode = mock(Node.class);
        mockTransaction = mock(Transaction.class);
        mockStat = mock(Stat.class);
        mockChildNode = mock(Node.class);
        when(mockZK.transaction()).thenReturn(mockTransaction);
        when(mockNode.getAbsolutePath()).thenReturn("/destination/path");
        when(mockNode.getData()).thenReturn(THEDATA);
        when(mockChildNode.getAbsolutePath()).thenReturn("/destination/path/child");
        when(mockChildNode.getData()).thenReturn(THEDATA);
        when(mockNode.getChildren()).thenReturn(Arrays.asList(mockChildNode));
    }

    @Test
    public void testWriteNewNode() throws InterruptedException, KeeperException {
        Writer writer = new Writer(mockZK, "/destination", mockNode, false, true, -1, 10);
        writer.write();
        verify(mockZK, times(1)).transaction();
        verify(mockTransaction, times(1)).create(eq("/destination/path"), eq(THEDATA), anyListOf(ACL.class), any(CreateMode.class));
        verify(mockTransaction, times(1)).commit();
    }
    
    @Test
    public void testWriteExistingNode() throws InterruptedException, KeeperException {
        when(mockZK.exists(anyString(), anyBoolean())).thenReturn(mockStat);
        
        Writer writer = new Writer(mockZK, "/destination", mockNode, false, true, -1, 10);
        writer.write();
        verify(mockZK, times(1)).transaction();
        verify(mockTransaction, times(2)).setData(startsWith("/destination/path"), eq(THEDATA), eq(-1));
        verify(mockTransaction, times(1)).commit();
    }
    
    @Test
    public void testWriteRemoveDeprecated() throws InterruptedException, KeeperException {
        when(mockZK.getChildren(eq("/destination/path"), anyBoolean())).thenReturn(Arrays.asList("a", "b"));
        
        Writer writer = new Writer(mockZK, "/destination", mockNode, true, true, -1, 10);
        writer.write();
        verify(mockZK, times(1)).transaction();
        verify(mockTransaction, times(1)).create(eq("/destination/path"), eq(THEDATA), anyListOf(ACL.class), any(CreateMode.class));
        verify(mockTransaction, times(1)).create(eq("/destination/path/child"), eq(THEDATA), anyListOf(ACL.class), any(CreateMode.class));
        verify(mockTransaction, times(1)).commit();
        verify(mockTransaction, times(1)).delete(eq("/destination/path/a"), anyInt());
        verify(mockTransaction, times(1)).delete(eq("/destination/path/b"), anyInt());
    }
    
    @Test
    public void testWriteSkipNewer() throws InterruptedException, KeeperException {
        when(mockNode.getMtime()).thenReturn(12345L);
        
        Writer writer = new Writer(mockZK, "/destination", mockNode, false, true, 12346, 10);
        writer.write();
        verify(mockZK, times(1)).transaction();
        verify(mockTransaction, times(0)).setData(anyString(), eq(THEDATA), anyInt());
        verify(mockTransaction, times(0)).create(anyString(), eq(THEDATA), anyListOf(ACL.class), any(CreateMode.class));
        verify(mockTransaction, times(1)).commit();
        verifyNoMoreInteractions(mockZK, mockTransaction);
    }

    @Test
    public void testWriteIgnoreEphemeral() throws InterruptedException, KeeperException {
        when(mockChildNode.isEphemeral()).thenReturn(true);
        
        Writer writer = new Writer(mockZK, "/destination", mockNode, false, true, -1, 10);
        writer.write();
        verify(mockZK, times(1)).transaction();
        verify(mockTransaction, times(1)).create(anyString(), nullable(byte[].class), anyListOf(ACL.class), any(CreateMode.class));
        verify(mockTransaction, times(1)).commit();
    }
}
