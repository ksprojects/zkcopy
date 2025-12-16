package com.github.ksprojects.zkcopy.replicator;

import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.comparator.Comparator;
import com.github.ksprojects.zkcopy.metric.SyncReplicatorMetricsManager;
import com.github.ksprojects.zkcopy.reader.Reader;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ZkEventListener {
    private static final Logger log = Logger.getLogger(ZkEventListener.class);
    
    private final String sourceAddress;
    private final String targetAddress;
    private final String sourcePath;
    private final String targetPath;
    private final int sessionTimeout;
    private final boolean ignoreEphemeralNodes;
    private final int workers;
    private final Set<String> ignoredPaths;

    private final ZkEventTemplate zkEventTemplate;
    private CuratorFramework sourceClient;
    private CuratorFramework targetClient;
    private CuratorCache sourceTree;
    private CuratorCache targetTree;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    private final SyncReplicatorMetricsManager metricsManager;
    private final AtomicLong connectedAt;

    public ZkEventListener(
        String sourceAddress, String targetAddress, int sessionTimeout, int workers,
        boolean ignoreEphemeralNodes, Set<String> ignoredPaths, ZkEventTemplate zkEventTemplate, SyncReplicatorMetricsManager metricsManager
    ) {
        this.sourceAddress = sourceAddress;
        this.targetAddress = targetAddress;
        this.sessionTimeout = sessionTimeout;
        this.workers = workers;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.ignoredPaths = ignoredPaths;
        
        this.sourcePath = getPath(sourceAddress);
        this.targetPath = getPath(targetAddress);
        this.zkEventTemplate = zkEventTemplate;
        this.metricsManager = metricsManager;
        this.connectedAt = new AtomicLong();
    }
    
    private String getHost(String addr) { return addr.split("/", 2)[0]; }
    private String getPath(String addr) { return "/" + addr.split("/", 2)[1]; }
    
    public void start() {
        try {
            connect();
            
            initConnectionGauge();

            if (!runCompare()) {
                 log.error("Initial comparison failed. Aborting sync mode.");
                 System.exit(1);
            }

            initCacheTries();

            synchronized (this) {
                while (isRunning.get()) {
                    wait();
                }
            }
        } catch (Exception e) {
            log.error("Error in SyncReplicator", e);
            System.exit(1);
        } finally {
            closeQuietly(sourceTree);
            closeQuietly(targetTree);
            closeQuietly(sourceClient);
            closeQuietly(targetClient);
        }
    }

    private void connect() throws InterruptedException {
        log.info("Starting ZkEventListener...");

        sourceClient = CuratorFrameworkFactory.newClient(
            getHost(sourceAddress),
            sessionTimeout,
            sessionTimeout,
            new ExponentialBackoffRetry(1000, 3));

        targetClient = CuratorFrameworkFactory.newClient(
            getHost(targetAddress),
            sessionTimeout,
            sessionTimeout,
            new ExponentialBackoffRetry(1000, 3));

        sourceClient.start();
        targetClient.start();

        sourceClient.blockUntilConnected();
        targetClient.blockUntilConnected();
        log.info("Connected to ZooKeeper instances.");
    }

    private void consume(CuratorCacheListener.Type type, ChildData oldData, ChildData newData, boolean isSource){
        try {
            switch (type) {
                case NODE_CREATED:
                case NODE_DELETED:
                case NODE_CHANGED:
                    processDataEvent(type, oldData, newData, isSource);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("Error processing event with type: " + type, e);
        }
    }

    private void processDataEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData newData, boolean isSource){
        Stat oldStat = oldData.getStat();
        Stat newStat = newData.getStat();
        String path = getEventPath(type, oldData, newData);

        ZkEvent event = new ZkEvent(oldStat, newStat, path, oldData.getData(), newData.getData(), isSource, type);

        zkEventTemplate.send(event);
    }

    private String getEventPath(CuratorCacheListener.Type type, ChildData oldData, ChildData newData){
        switch (type) {
            case NODE_CHANGED:
            case NODE_CREATED: return newData.getPath();
            case NODE_DELETED: return oldData.getPath();
            default: return null;
        }
    }

    private void initConnectionGauge(){
        connectedAt.set(System.currentTimeMillis());
        metricsManager.initConnectionGauge(connectedAt, paused);
        metricsManager.initUpTimeGauge();
        metricsManager.initMaxMemoryGauge();
        metricsManager.initFreeMemoryGauge();
        metricsManager.initUsedMemoryGauge();
    }

    private void initCacheTries(){
        sourceTree = CuratorCache.build(sourceClient, sourcePath);
        targetTree = CuratorCache.build(targetClient, targetPath);

        sourceTree.listenable().addListener((type, oldData, newData) -> consume(type, oldData, newData, true));
        targetTree.listenable().addListener((type, oldData, newData) -> consume(type, oldData, newData, false));

        log.info("Starting CuratorCache on source: " + sourcePath);
        sourceTree.start();
        log.info("Starting CuratorCache on target: " + targetPath);
        targetTree.start();
        log.info("Listeners started. Replication active.");
    }

    private boolean runCompare() {
        log.info("Running comparison...");
        Reader sourceReader = new Reader(sourceAddress, workers, sessionTimeout, true, ignoredPaths);
        Node sourceRoot = sourceReader.read();
        
        Reader targetReader = new Reader(targetAddress, workers, sessionTimeout, true, ignoredPaths);
        Node targetRoot = targetReader.read();
        
        Comparator comparator = new Comparator(sourceRoot, targetRoot, ignoredPaths);
        return comparator.compare();
    }

    private void closeQuietly(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignore) {
            }
        }
    }
}
