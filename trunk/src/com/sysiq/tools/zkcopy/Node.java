package com.sysiq.tools.zkcopy;

import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;

public class Node
{
    private Node parent;
    private final List<Node> children;
    private final Set<String> childrenNames;
    private String path;
    private byte[] data;
    
    public Node(String path) {
        children = new LinkedList<Node>();
        childrenNames = new HashSet<String>();
        parent = null;
        this.path = path;
        data = null;
    }
    
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
    
    public void setChildren(List<Node> newChildren) {
        children.clear();
        for(Node child:newChildren) {
            appendChild(child);
        }
    }
    
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
    
    public void setData(byte[] data) {
        this.data = data;
    }
    
    public byte[] getData() {
        return data;
    }
}
