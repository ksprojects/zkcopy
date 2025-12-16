package com.github.ksprojects.zkcopy.presync;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ZkClustersPresyncer {

    private static final Logger LOGGER = Logger.getLogger(ZkClustersPresyncer.class);

    private final CuratorFramework sourceClient;
    private final CuratorFramework targetClient;

    public ZkClustersPresyncer(CuratorFramework sourceClient, CuratorFramework targetClient) {
        this.sourceClient = sourceClient;
        this.targetClient = targetClient;
    }

    public boolean presync(String sourcePath, String targetPath) {
        LOGGER.info("Starting presync between " + sourcePath + " and " + targetPath);
        return process(sourcePath, targetPath);
    }

    private boolean process(String sPath, String tPath) {
        try {
            Stat sStat = sourceClient.checkExists().forPath(sPath);
            Stat tStat = targetClient.checkExists().forPath(tPath);

            if (sStat != null && tStat != null) {
                return handleBothExist(sPath, sStat, tPath, tStat);
            } else if (sStat != null) {
                return handleSourceOnly(sPath, sStat, tPath);
            } else if (tStat != null) {
                return handleTargetOnly(sPath, tPath, tStat);
            } else {
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Error processing paths " + sPath + " / " + tPath, e);
            return false;
        }
    }

    private boolean handleBothExist(String sPath, Stat sStat, String tPath, Stat tStat) throws Exception {
        syncData(sPath, sStat, tPath, tStat);
        return processChildren(sPath, tPath);
    }

    private void syncData(String sPath, Stat sStat, String tPath, Stat tStat) throws Exception {
        byte[] sData = sourceClient.getData().forPath(sPath);
        byte[] tData = targetClient.getData().forPath(tPath);

        if (!Arrays.equals(sData, tData)) {
            if (sStat.getMtime() > tStat.getMtime()) {
                LOGGER.info("Source is newer. Updating target: " + tPath);
                targetClient.setData().forPath(tPath, sData);
            } else {
                LOGGER.info("Target is newer. Updating source: " + sPath);
                sourceClient.setData().forPath(sPath, tData);
            }
        }
    }

    private boolean processChildren(String sPath, String tPath) throws Exception {
        List<String> sChildren = sourceClient.getChildren().forPath(sPath);
        List<String> tChildren = targetClient.getChildren().forPath(tPath);
        Set<String> allChildren = new HashSet<>(sChildren);
        allChildren.addAll(tChildren);

        boolean success = true;
        for (String child : allChildren) {
            String nextSPath = buildPath(sPath, child);
            String nextTPath = buildPath(tPath, child);
            if (!process(nextSPath, nextTPath)) {
                success = false;
            }
        }
        return success;
    }

    private boolean handleSourceOnly(String sPath, Stat sStat, String tPath) throws Exception {
        if (sStat.getEphemeralOwner() != 0) {
            LOGGER.info("Ephemeral node missing at target. Creating: " + tPath);
            byte[] data = sourceClient.getData().forPath(sPath);
            targetClient.create().withMode(CreateMode.EPHEMERAL).forPath(tPath, data);
            return true;
        } else {
            LOGGER.warn("Persistent node missing at target: " + tPath + ". Presync failed.");
            return false;
        }
    }

    private boolean handleTargetOnly(String sPath, String tPath, Stat tStat) throws Exception {
        if (tStat.getEphemeralOwner() != 0) {
            LOGGER.info("Ephemeral node missing at source. Creating: " + sPath);
            byte[] data = targetClient.getData().forPath(tPath);
            sourceClient.create().withMode(CreateMode.EPHEMERAL).forPath(sPath, data);
            return true;
        } else {
            LOGGER.warn("Persistent node missing at source: " + sPath + ". Presync failed.");
            return false;
        }
    }

    private String buildPath(String parent, String child) {
        if (parent.endsWith("/")) {
            return parent + child;
        }
        return parent + "/" + child;
    }
}
