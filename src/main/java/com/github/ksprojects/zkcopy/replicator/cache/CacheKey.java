package com.github.ksprojects.zkcopy.replicator.cache;

import java.util.Arrays;
import java.util.Objects;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener.Type;

public class CacheKey {
    private final String path;
    private final int dataHashCode;
    private final Type type;

    public CacheKey(String path, byte[] data, Type type) {
        this.path = path;
        this.dataHashCode = Arrays.hashCode(data);
        this.type = type;
    }

    public static CacheKey of(String path, byte[] data, Type type){
        return new CacheKey(path, data, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return dataHashCode == cacheKey.dataHashCode &&
            Objects.equals(path, cacheKey.path) &&
            type == cacheKey.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, dataHashCode, type);
    }
}
