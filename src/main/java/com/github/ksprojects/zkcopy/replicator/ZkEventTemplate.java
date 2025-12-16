package com.github.ksprojects.zkcopy.replicator;

@FunctionalInterface
public interface ZkEventTemplate {
    void send(ZkEvent event);
}
