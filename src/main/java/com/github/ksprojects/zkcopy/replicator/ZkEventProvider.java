package com.github.ksprojects.zkcopy.replicator;

@FunctionalInterface
public interface ZkEventProvider {
    ZkEvent provide() throws InterruptedException;
}
