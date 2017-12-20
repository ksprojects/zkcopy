package com.github.ksprojects.zkcopy;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Node {
    private final List<Node> children;
    private final Set<String> childrenNames;
    private Node parent;
    private String path;
    private byte[] data;
    private boolean isEphemeral;
    private long mtime;

    /**
     * Create new root node instance for a given path.
     */
    public Node(String path) {
        children = new LinkedList<Node>();
        childrenNames = new HashSet<String>();
        parent = null;
        this.path = path;
        data = null;
        isEphemeral = false;
    }

    /**
     * Create new child node.
     */
    public Node(Node parent, String path) {
        children = new LinkedList<Node>();
        childrenNames = new HashSet<String>();
        this.parent = parent;
        this.path = path;
        data = null;
    }

    public Node getParent() {
        return parent;
    }

    public void appendChild(Node child) {
        children.add(child);
        childrenNames.add(child.getPath());
    }

    public List<Node> getChildren() {
        return children;
    }

    public Set<String> getChildrenNamed() {
        return childrenNames;
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
