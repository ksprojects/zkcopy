package com.github.ksprojects.zkcopy.replicator;

import com.github.ksprojects.zkcopy.metric.SyncReplicatorMetricsManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.Set;

public class ZkEventHandler {
    private static final Logger log = Logger.getLogger(ZkEventHandler.class);

    private final CuratorFramework source;
    private final CuratorFramework target;
    private final String sourcePath;
    private final String targetPath;
    private final Set<String> ignoredPaths;
    private final boolean ignoreEphemeralNodes;
    private final SyncReplicatorMetricsManager metricsManager;

    public ZkEventHandler(CuratorFramework source, CuratorFramework target, String sourcePath, String targetPath, Set<String> ignoredPaths, boolean ignoreEphemeralNodes, SyncReplicatorMetricsManager metricsManager) {
        this.source = source;
        this.target = target;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.ignoredPaths = ignoredPaths;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.metricsManager = metricsManager;
    }

    public void handle(ZkEvent event){
        switch (event.getType()){
            case NODE_CREATED:
                handleCreate(event);
                break;
            case NODE_CHANGED:
                handleUpdate(event);
                break;
            case NODE_DELETED:
                handleDelete(event);
                break;
        }
    }

    protected void handleCreate(ZkEvent event){
        String path = event.getPath();
        if (shouldIgnore(path)) return;

        String relativePath = makeRelativePath(path, event.isSource());
        if (relativePath == null) return;
        var remoteNodePath = makeRemotePath(relativePath, event.isSource());

        var remoteClient = event.isSource() ? target : source;

        try {
            var createMode = getCreateMode(event);

            if (ignoreEphemeralNodes && isEphemeral(event.getNewStat())) {
                return;
            }

            log.info(String.format("[%s] Replicating creation: %s. Value: %s.", event.getType(), remoteNodePath, bytesToString(event.getNewData())));
            try {
                remoteClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(createMode)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(remoteNodePath, event.getNewData());
                metricsManager.countCreation(event.isSource(), extractParentNodeName(relativePath));
            } catch (org.apache.zookeeper.KeeperException.NodeExistsException e) {
                log.info(String.format("Node %s already exist! Skipped.", remoteNodePath));
            }
        } catch (Exception e) {
            log.error("Failed to replicate creation for " + path, e);
        }
    }

    protected void handleUpdate(ZkEvent event){
        var path = event.getPath();
        if (shouldIgnore(path)) return;

        String relativePath = makeRelativePath(path, event.isSource());
        if (relativePath == null) return;
        var remoteNodePath = makeRemotePath(relativePath, event.isSource());

        var remoteClient = event.isSource() ? target : source;

        try {
            if (ignoreEphemeralNodes && isEphemeral(event.getNewStat())) {
                return;
            }

            log.info(String.format("[%s] Replicating data change of: %s. Old value: %s. New value: %s.", event.getType(), remoteNodePath, bytesToString(event.getOldData()), bytesToString(event.getNewData())));
            try {
                remoteClient.setData()
                    .forPath(remoteNodePath, event.getNewData());
                metricsManager.countDataChanged(event.isSource(), extractParentNodeName(relativePath));
            } catch (KeeperException.NoNodeException e) {
                log.info(String.format("Node %s does not exist! Skipped.", remoteNodePath));
            }
        } catch (Exception e) {
            log.error("Failed to replicate data changed for " + path, e);
        }
    }

    protected void handleDelete(ZkEvent event){
        var path = event.getPath();
        if (shouldIgnore(path)) return;

        String relativePath = makeRelativePath(path, event.isSource());
        if (relativePath == null) return;
        var remoteNodePath = makeRemotePath(relativePath, event.isSource());

        var remoteClient = event.isSource() ? target : source;

        try {
            if (ignoreEphemeralNodes && isEphemeral(event.getOldStat())) {
                return;
            }

            log.info(String.format("[%s] Replicating deletion of: %s. Old value: %s.", event.getType(), remoteNodePath, bytesToString(event.getOldData())));
            try {
                remoteClient.delete()
                    .forPath(remoteNodePath);
                metricsManager.countDeletion(event.isSource(), extractParentNodeName(relativePath));
            } catch (KeeperException.NoNodeException ignore) {}
        } catch (Exception e) {
            log.error("Failed to replicate data deletion for " + path, e);
        }
    }

    private boolean shouldIgnore(String path) {
        return ignoredPaths.contains(path);
    }

    private String makeRelativePath(String localNodePath, boolean isSource){
        var localRoot = isSource ? sourcePath : targetPath;
        if (localNodePath.equals(localRoot)) {
            return "";
        } else if (localNodePath.startsWith(localRoot + "/")) {
            return localNodePath.substring(localRoot.length());
        } else {
            return null;
        }
    }

    private String makeRemotePath(String relativePath, boolean isSource) {
        var remotePath = isSource ? targetPath : sourcePath;

        return remotePath + relativePath;
    }

    private CreateMode getCreateMode(ZkEvent event){
        if (CuratorCacheListener.Type.NODE_DELETED.equals(event.getType())){
            return event.getOldStat().getEphemeralOwner() > 0 ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
        }

        return event.getNewStat().getEphemeralOwner() > 0 ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
    }

    private boolean isEphemeral(Stat stat){
        return stat.getEphemeralOwner() > 0;
    }

    private String bytesToString(byte[] bytes) {
        return new String(bytes);
    }

    private String extractParentNodeName(String relativePath){
        if (relativePath.isEmpty()) return relativePath;
        int ind = relativePath.substring(1).indexOf('/');
        ind = ind == -1 ? relativePath.length() : ind + 1;
        return relativePath.substring(1, ind);
    }
}
