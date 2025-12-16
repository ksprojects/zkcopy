package com.github.ksprojects.zkcopy.replicator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.util.Set;

public class ZkEventListener implements Closeable {
    private static final Logger log = Logger.getLogger(ZkEventListener.class);
    
    private final String sourcePath;
    private final String targetPath;
    private final Set<String> ignoredPaths;
    private final boolean ignoreEphemeralNodes;

    private final ZkEventTemplate zkEventTemplate;
    private final CuratorFramework sourceClient;
    private final CuratorFramework targetClient;
    private CuratorCache sourceTree;
    private CuratorCache targetTree;

    public ZkEventListener(
        String sourcePath, String targetPath, CuratorFramework sourceClient, CuratorFramework targetClient,
        boolean ignoreEphemeralNodes, Set<String> ignoredPaths, ZkEventTemplate zkEventTemplate
    ) {
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.ignoredPaths = ignoredPaths;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.zkEventTemplate = zkEventTemplate;
        this.sourceClient = sourceClient;
        this.targetClient = targetClient;
    }

    public void start() {
        initCacheTries();

        log.info("Starting CuratorCache on source: " + sourcePath);
        sourceTree.start();
        log.info("Starting CuratorCache on target: " + targetPath);
        targetTree.start();

        log.info("Listeners started. Replication active.");
    }

    @Override
    public void close() {
        closeQuietly(sourceTree);
        closeQuietly(targetTree);
    }

    private void consume(CuratorCacheListener.Type type, ChildData oldData, ChildData newData, boolean isSource){
        try {
            Stat oldStat = oldData.getStat();
            Stat newStat = newData.getStat();
            String path = getEventPath(type, oldData, newData);

            if (ignoreEphemeralNodes && isEphemeral(type, oldData, newData)) return;
            if (shouldIgnore(path)) return;

            ZkEvent event = new ZkEvent(oldStat, newStat, path, oldData.getData(), newData.getData(), isSource, type);

            log.info(String.format("Prepared new %s event for path %s", type, path));
            zkEventTemplate.send(event);
        } catch (Exception e) {
            log.error("Error processing event with type: " + type, e);
        }
    }

    private String getEventPath(CuratorCacheListener.Type type, ChildData oldData, ChildData newData){
        switch (type) {
            case NODE_CHANGED:
            case NODE_CREATED: return newData.getPath();
            case NODE_DELETED: return oldData.getPath();
            default: throw new IllegalArgumentException("Provided illegal event type: " + type);
        }
    }

    private boolean isEphemeral(CuratorCacheListener.Type type, ChildData oldData, ChildData newData){
        switch (type) {
            case NODE_CHANGED:
            case NODE_CREATED: return newData.getStat().getEphemeralOwner() > 0;
            case NODE_DELETED: return oldData.getStat().getEphemeralOwner() > 0;
            default: throw new IllegalArgumentException("Provided illegal event type: " + type);
        }
    }

    private void initCacheTries(){
        sourceTree = CuratorCache.build(sourceClient, sourcePath);
        targetTree = CuratorCache.build(targetClient, targetPath);

        sourceTree.listenable().addListener((type, oldData, newData) -> consume(type, oldData, newData, true));
        targetTree.listenable().addListener((type, oldData, newData) -> consume(type, oldData, newData, false));
    }

    private void closeQuietly(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignore) {
            }
        }
    }

    private boolean shouldIgnore(String path) {
        return ignoredPaths.stream().anyMatch(path::contains);
    }
}
