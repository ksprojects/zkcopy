package com.github.ksprojects.zkcopy.replicator;

import org.apache.zookeeper.data.Stat;

import org.apache.curator.framework.recipes.cache.CuratorCacheListener.Type;

import java.util.Arrays;

public class ZkEvent {
    private final Stat oldStat;

    private final Stat newStat;

    private final String path;

    private final byte[] newData;

    private final byte[] oldData;

    private final boolean isSource;

    private final Type type;

    public ZkEvent(Stat oldStat, Stat newStat, String path, byte[] oldData, byte[] newData, boolean isSource, Type type) {
        this.oldStat = oldStat;
        this.newStat = newStat;
        this.path = path;
        this.oldData = oldData;
        this.newData = newData;
        this.isSource = isSource;
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public byte[] getNewData() {
        return newData;
    }

    public boolean isSource() {
        return isSource;
    }

    public Type getType() {
        return type;
    }

    public Stat getNewStat() {
        return newStat;
    }

    public Stat getOldStat() {
        return oldStat;
    }

    public byte[] getOldData() {
        return oldData;
    }

    @Override
    public String toString() {
        return "ZkEvent{" +
            "oldStat=" + oldStat +
            ", newStat=" + newStat +
            ", path='" + path + '\'' +
            ", newData=" + Arrays.toString(newData) +
            ", oldData=" + Arrays.toString(oldData) +
            ", isSource=" + isSource +
            ", type=" + type +
            '}';
    }
}
