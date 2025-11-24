package com.github.ksprojects.zkcopy.replicator;

import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.comparator.Comparator;
import com.github.ksprojects.zkcopy.reader.Reader;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class SyncReplicator {
    private static final Logger LOGGER = Logger.getLogger(SyncReplicator.class);
    
    private final String sourceAddress;
    private final String targetAddress;
    private final String sourcePath;
    private final String targetPath;
    private final int sessionTimeout;
    private final boolean ignoreEphemeralNodes;
    private final int workers;
    private final Set<String> ignoredPaths;
    
    private ZooKeeper sourceZk;
    private ZooKeeper targetZk;
    
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    private final Watcher sourceWatcher;
    private final Watcher targetWatcher;
    
    public SyncReplicator(String sourceAddress, String targetAddress, int sessionTimeout, int workers, boolean ignoreEphemeralNodes, Set<String> ignoredPaths) {
        this.sourceAddress = sourceAddress;
        this.targetAddress = targetAddress;
        this.sessionTimeout = sessionTimeout;
        this.workers = workers;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.ignoredPaths = ignoredPaths;
        
        this.sourcePath = getPath(sourceAddress);
        this.targetPath = getPath(targetAddress);
        
        this.sourceWatcher = event -> processEvent(event, true);
        this.targetWatcher = event -> processEvent(event, false);
    }
    
    private String getHost(String addr) { return addr.split("/", 2)[0]; }
    private String getPath(String addr) { return "/" + addr.split("/", 2)[1]; }
    
    public void start() {
        try {
            connect();
            
            if (!runCompare()) {
                 LOGGER.error("Initial comparison failed. Aborting sync mode.");
                 System.exit(1);
            }
            
            LOGGER.info("Initial comparison successful. Starting event listeners...");
            subscribe(sourceZk, sourcePath, sourceWatcher);
            subscribe(targetZk, targetPath, targetWatcher);
            
            synchronized (this) {
                while (isRunning.get()) {
                    wait();
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error in SyncReplicator", e);
            System.exit(1);
        }
    }

    private void connect() throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        
        sourceZk = new ZooKeeper(getHost(sourceAddress), sessionTimeout, event -> {
            if (event.getType() == Watcher.Event.EventType.None) {
                 handleConnectionState(event.getState(), true);
                 if (event.getState() == Watcher.Event.KeeperState.SyncConnected) latch.countDown();
            }
        });
        
        targetZk = new ZooKeeper(getHost(targetAddress), sessionTimeout, event -> {
             if (event.getType() == Watcher.Event.EventType.None) {
                 handleConnectionState(event.getState(), false);
                 if (event.getState() == Watcher.Event.KeeperState.SyncConnected) latch.countDown();
             }
        });
        
        LOGGER.info("Connecting to Zookeeper instances...");
        latch.await();
        LOGGER.info("Connected.");
    }

    private boolean runCompare() {
        LOGGER.info("Running comparison...");
        Reader sourceReader = new Reader(sourceAddress, workers, sessionTimeout, ignoreEphemeralNodes, ignoredPaths);
        Node sourceRoot = sourceReader.read();
        
        Reader targetReader = new Reader(targetAddress, workers, sessionTimeout, ignoreEphemeralNodes, ignoredPaths);
        Node targetRoot = targetReader.read();
        
        Comparator comparator = new Comparator(sourceRoot, targetRoot);
        return comparator.compare();
    }
    
    private void processEvent(WatchedEvent event, boolean isSource) {
        if (paused.get()) {
            LOGGER.warn("Ignoring event because replication is paused: " + event);
            return;
        }
        
        if (event.getType() == Watcher.Event.EventType.None) {
            handleConnectionState(event.getState(), isSource);
            return;
        }

        String path = event.getPath();
        if (path == null) return;
        
        LOGGER.debug("Processing event " + event.getType() + " on " + path + " (Source=" + isSource + ")");

        final ZooKeeper local = isSource ? sourceZk : targetZk;
        final ZooKeeper remote = isSource ? targetZk : sourceZk;
        final String localRoot = isSource ? sourcePath : targetPath;
        final String remoteRoot = isSource ? targetPath : sourcePath;
        final Watcher localWatcher = isSource ? sourceWatcher : targetWatcher;
        final Watcher remoteWatcher = isSource ? targetWatcher : sourceWatcher;
        
        String relativePath;
        if (path.equals(localRoot)) {
            relativePath = "";
        } else if (path.startsWith(localRoot + "/")) {
            relativePath = path.substring(localRoot.length());
        } else {
            return; 
        }
        
        String remoteNodePath = remoteRoot + relativePath;
        if (remoteNodePath.startsWith("//")) remoteNodePath = remoteNodePath.substring(1);
        
        try {
            switch (event.getType()) {
                case NodeDataChanged:
                    Stat stat = new Stat();
                    byte[] data = local.getData(path, localWatcher, stat);
                    
                    if (ignoreEphemeralNodes && stat.getEphemeralOwner() > 0) {
                        return;
                    }

                    try {
                        Stat remoteStat = new Stat();
                        byte[] remoteData = remote.getData(remoteNodePath, false, remoteStat);
                        if (!Arrays.equals(data, remoteData)) {
                             LOGGER.info("Replicating data change to " + remoteNodePath);
                             remote.setData(remoteNodePath, data, -1);
                        }
                    } catch (KeeperException.NoNodeException e) {
                        LOGGER.info("Remote node missing, creating: " + remoteNodePath);
                        remote.create(remoteNodePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        subscribe(remote, remoteNodePath, remoteWatcher); 
                    }
                    break;
                    
                case NodeChildrenChanged:
                    List<String> children = local.getChildren(path, localWatcher);
                    List<String> remoteChildren;
                    try {
                        remoteChildren = remote.getChildren(remoteNodePath, false);
                    } catch (KeeperException.NoNodeException e) {
                         return;
                    }
                    
                    Set<String> localSet = new HashSet<>(children);
                    Set<String> remoteSet = new HashSet<>(remoteChildren);
                    
                    for (String child : localSet) {
                        if (!remoteSet.contains(child)) {
                            String childPath = makeChildPath(path, child);
                            if (ignoredPaths.contains(childPath)) {
                                continue;
                            }
                            String remoteChildPath = makeChildPath(remoteNodePath, child);
                            
                            LOGGER.info("Replicating creation of " + remoteChildPath);
                            copyNodeRecursive(local, remote, childPath, remoteChildPath, localWatcher, remoteWatcher);
                        }
                    }

                    for (String child : remoteSet) {
                        if (!localSet.contains(child)) {
                             String remoteChildPath = makeChildPath(remoteNodePath, child);
                             LOGGER.info("Replicating deletion of " + remoteChildPath);
                             deleteRecursive(remote, remoteChildPath);
                        }
                    }
                    break;
                case NodeDeleted:
                    local.exists(path, localWatcher);
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Error syncing change", e);
        }
    }
    
    private void copyNodeRecursive(ZooKeeper from, ZooKeeper to, String fromPath, String toPath, Watcher fromWatcher, Watcher toWatcher) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data = from.getData(fromPath, fromWatcher, stat);
        
        if (ignoreEphemeralNodes && stat.getEphemeralOwner() > 0) {
            return;
        }
        
        try {
            to.create(toPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            to.setData(toPath, data, -1);
        }
        subscribe(to, toPath, toWatcher);
        
        List<String> children = from.getChildren(fromPath, fromWatcher);
        for (String child : children) {
            String childFullPath = fromPath.equals("/") ? "/" + child : fromPath + "/" + child;
            if (ignoredPaths.contains(childFullPath)) {
                continue;
            }
            copyNodeRecursive(from, to, fromPath + "/" + child, toPath + "/" + child, fromWatcher, toWatcher);
        }
    }
    
    private void deleteRecursive(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        try {
            List<String> children = zk.getChildren(path, false);
            for (String child : children) {
                deleteRecursive(zk, path + "/" + child);
            }
            zk.delete(path, -1);
        } catch (KeeperException.NoNodeException e) {
        }
    }
    
    private void handleConnectionState(Watcher.Event.KeeperState state, boolean isSource) {
        if (state == Watcher.Event.KeeperState.Disconnected || state == Watcher.Event.KeeperState.Expired) {
            LOGGER.warn((isSource ? "Source" : "Target") + " disconnected. Pausing...");
            paused.set(true);
        } else if (state == Watcher.Event.KeeperState.SyncConnected) {
            if (paused.get()) {
                LOGGER.info("Connection restored. Running comparison...");
                if (runCompare()) {
                    LOGGER.info("Comparison OK. Resuming.");
                    paused.set(false);
                    try {
                         subscribe(sourceZk, sourcePath, sourceWatcher);
                         subscribe(targetZk, targetPath, targetWatcher);
                    } catch (Exception e) {
                        LOGGER.error("Resubscribe failed", e);
                    }
                } else {
                    LOGGER.error("Comparison failed after recovery. Exiting.");
                    System.exit(1);
                }
            }
        }
    }
    
    private void subscribe(ZooKeeper zk, String path, Watcher watcher) throws KeeperException, InterruptedException {
        if (ignoredPaths.contains(path)) {
            return;
        }
        try {
            zk.getData(path, watcher, null);
            List<String> children = zk.getChildren(path, watcher);
            for (String child : children) {
                String childPath = makeChildPath(path, child);
                subscribe(zk, childPath, watcher);
            }
        } catch (KeeperException.NoNodeException ignore) {
        }
    }

    private String makeChildPath(String path, String child){
        return path.equals("/") ? "/" + child : path + "/" + child;
    }
}

