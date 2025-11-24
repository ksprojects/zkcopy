package com.github.ksprojects.zkcopy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Node {
    private final Map<String, Node> children;
    private final Node parent;
    private String path;
    private byte[] data;
    private boolean isEphemeral;
    private long mtime;

    /**
     * Create new root node instance for a given path.
     */
    public Node(String path) {
        children = new ConcurrentHashMap<>();
        parent = null;
        this.path = path;
        data = null;
        isEphemeral = false;
    }

    /**
     * Create new child node.
     */
    public Node(Node parent, String path) {
        children = new ConcurrentHashMap<>();
        this.parent = parent;
        this.path = path;
        data = null;
    }

    public Node getParent() {
        return parent;
    }

    public void appendChild(Node child) {
        children.put(child.getPath(), child);
    }

    public void removeChild(Node child) {
        children.remove(child.getPath());
    }

    public List<Node> getChildren() {
        return new ArrayList<>(children.values());
    }

    public Set<String> getChildrenNamed() {
        return new HashSet<>(children.keySet());
    }

    /**
     * Get an absolute path of this node.
     */
    public String getAbsolutePath() {
        if (parent == null) { // root
            return path;
        } else {
            if ("/".equals(parent.getAbsolutePath())) { // parent is root
                return parent.getAbsolutePath() + path;
            } else {
                return parent.getAbsolutePath() + "/" + path;
            }
        }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean isEphemeral() {
        return isEphemeral;
    }

    public void setEphemeral(boolean ephemeral) {
        isEphemeral = ephemeral;
    }

    public long getMtime() {
        return mtime;
    }

    
    public void setMtime(long mtime) {
        this.mtime = mtime;
    }
}
