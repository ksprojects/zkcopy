package com.sysiq.tools.zkcopy;

import java.util.List;
import java.util.LinkedList;

public class Node
{
    private Node parent;
    private List<Node> children;
    private String path;
    private byte[] data;
    
    public Node(String path) {
        children = new LinkedList<Node>();
        parent = null;
        this.path = path;
        data = null;
    }
    
    public Node(Node parent, String path) {
        children = new LinkedList<Node>();
        this.parent = parent;
        this.path = path;
        data = null;
    }
    
    public Node getParent() {
        return parent;
    }
    
    public void appendChild(Node child) { 
        children.add(child);
    }
    
    public List<Node> getChildren() {
        return children;
    }
    
    public void setChildren(List<Node> newChildren) {
        children.clear();
        for(Node child:newChildren) {
            appendChild(child);
        }
    }
    
    public String getPath() {
        if (parent == null) { // root
            return path;
        } else {
            if ("/".equals(parent.getPath())) { // parent is root
                return parent.getPath() + path;
            } else {
                return parent.getPath() + "/" + path;
            }
        }
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
