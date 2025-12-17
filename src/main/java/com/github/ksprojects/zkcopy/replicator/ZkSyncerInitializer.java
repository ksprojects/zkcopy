package com.github.ksprojects.zkcopy.replicator;

import com.github.ksprojects.zkcopy.metric.SyncReplicatorMetricsManager;
import com.github.ksprojects.zkcopy.presync.ZkClustersPresyncer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ZkSyncerInitializer {
    private static final Logger log = Logger.getLogger(ZkSyncerInitializer.class);

    private final String sourceAddress;
    private final String targetAddress;
    private final Set<String> ignoredPaths;
    private CuratorFramework sourceClient;
    private CuratorFramework targetClient;
    private final int sessionTimeout;
    private final SyncReplicatorMetricsManager metricsManager;
    private final boolean ignoreEphemeralNodes;
    private final ZkLocalEventBroker localEventBroker;

    private ZkEventListener zkEventListener;
    private ZkEventHandler zkEventHandler;
    private ZkEventExecutor zkEventExecutor;

    private final AtomicLong connectedAt = new AtomicLong();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public ZkSyncerInitializer(String sourceAddress, String targetAddress, Set<String> ignoredPaths, int sessionTimeout, SyncReplicatorMetricsManager metricsManager, boolean ignoreEphemeralNodes) {
        this.sourceAddress = sourceAddress;
        this.targetAddress = targetAddress;
        this.ignoredPaths = ignoredPaths;
        this.sessionTimeout = sessionTimeout;
        this.metricsManager = metricsManager;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.localEventBroker = new ZkLocalEventBroker(metricsManager);
    }

    public void initialize() {
        try {
            connect();

            initConnectionStateListeners();

            if (!presyncClusters()) {
                log.error("Initial comparison failed. Aborting sync mode.");
                System.exit(1);
            }

            initConnectionGauge();
            initMemoryGauge();
            initListener();
            initEventHandler();
            initEventExecutor();
        } catch (Exception e) {
            log.error("Critical error while syncer initialization!", e);
            System.exit(1);
        }
    }

    public void start(){
        try{
            zkEventListener.start();
            zkEventExecutor.start();

            lockMainThread();
        } catch (Exception e) {
            log.error("Critical syncer error!", e);
            System.exit(1);
        } finally {
            zkEventListener.close();
            closeQuietly(sourceClient);
            closeQuietly(targetClient);
        }
    }

    private void initListener(){
        log.info("Initializing zkEventListener...");

        zkEventListener = new ZkEventListener(
            getPath(sourceAddress), getPath(targetAddress), sourceClient, targetClient,
            ignoreEphemeralNodes, ignoredPaths, localEventBroker
        );
    }

    private void initEventHandler(){
        log.info("Initializing zkEventHandler...");

        zkEventHandler = new ZkEventHandler(
            sourceClient, targetClient, getPath(sourceAddress), getPath(targetAddress),
            ignoredPaths, ignoreEphemeralNodes, metricsManager
        );
    }

    private void initEventExecutor(){
        log.info("Initializing zkEventExecutor...");

        zkEventExecutor = new ZkEventExecutor(localEventBroker, zkEventHandler, isRunning, paused);
    }

    private void initConnectionGauge(){
        log.info("Initializing gauge connection metrics...");

        connectedAt.set(System.currentTimeMillis());
        metricsManager.initConnectionGauge(connectedAt, paused);
        metricsManager.initUpTimeGauge();
    }

    private void initMemoryGauge(){
        log.info("Initializing gauge memory metrics...");

        metricsManager.initMaxMemoryGauge();
        metricsManager.initFreeMemoryGauge();
        metricsManager.initUsedMemoryGauge();
    }

    private boolean presyncClusters() {
        log.info("Running presyncer...");

        var presyncer = new ZkClustersPresyncer(sourceClient, targetClient);
        var res = presyncer.presync(getPath(sourceAddress), getPath(targetAddress));

        log.info("Presynchronization has been finished!");
        return res;
    }

    private void lockMainThread() throws InterruptedException {
        synchronized (this) {
            while (isRunning.get()) {
                wait();
            }
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

    private void initConnectionStateListeners() {
        var listener = getConnectionStateListener();
        sourceClient.getConnectionStateListenable().addListener(listener);
        targetClient.getConnectionStateListenable().addListener(listener);
    }

    private ConnectionStateListener getConnectionStateListener(){
        return (client, newState) -> {
            switch (newState) {
                case CONNECTED:
                case RECONNECTED:
                    paused.set(false);
                    connectedAt.set(System.currentTimeMillis());
                    log.info("Curator connection established/restored to " + client.getZookeeperClient().getCurrentConnectionString());
                    break;
                case SUSPENDED:
                    paused.set(true);
                    log.warn("Curator connection suspended to " + client.getZookeeperClient().getCurrentConnectionString());
                    log.warn("Replication stopped.");
                    break;
                case LOST:
                    log.error("Connection finally lost! Session died with ephemerals. Exiting...");
                    System.exit(1);
                default:
                    break;
            }
        };
    }

    private String getHost(String addr) { return addr.split("/", 2)[0]; }

    private String getPath(String addr) { return "/" + addr.split("/", 2)[1]; }

    private void closeQuietly(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignore) {
            }
        }
    }
}
