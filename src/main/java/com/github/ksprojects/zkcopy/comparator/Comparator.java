package com.github.ksprojects.zkcopy.comparator;

import com.github.ksprojects.zkcopy.Node;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class Comparator {
    private static final Logger LOGGER = Logger.getLogger(Comparator.class);
    private final Node sourceRoot;
    private final Node targetRoot;
    private final Set<String> ignoredPaths;

    public Comparator(Node sourceRoot, Node targetRoot, Set<String> ignoredPaths) {
        this.sourceRoot = sourceRoot;
        this.targetRoot = targetRoot;
        this.ignoredPaths = ignoredPaths;
    }
    
    public boolean compare() {
        List<String> errors = new ArrayList<>();
        if (sourceRoot == null || targetRoot == null){
            LOGGER.error("Null node provided for compare!");
            return false;
        }
        compareNodes(sourceRoot, targetRoot, errors);
        
        if (errors.isEmpty()) {
            LOGGER.info("Comparison successful. No differences found.");
            return true;
        } else {
            LOGGER.error("Comparison failed. Differences found:");
            for (String error : errors) {
                LOGGER.error(error);
            }
            return false;
        }
    }
    
    private void compareNodes(Node source, Node target, List<String> errors) {
        if (source == null || target == null) return;

        if (ignoredPaths.contains(source.getAbsolutePath())) return;
        if (ignoredPaths.contains(target.getAbsolutePath())) return;

        if (!Arrays.equals(source.getData(), target.getData())) {
            errors.add("Data mismatch at " + source.getAbsolutePath() + " vs " + target.getAbsolutePath());
        }

        Set<String> sourceChildrenNames = source.getChildrenNamed();
        Set<String> targetChildrenNames = target.getChildrenNamed();

        for (String childName : sourceChildrenNames) {
            if (!targetChildrenNames.contains(childName)) {
                if (isIgnored(source.getAbsolutePath(), childName) || isIgnored(target.getAbsolutePath(), childName)) {
                    continue;
                }
                errors.add("Node missing in target: " + source.getAbsolutePath() + "/" + childName);
            }
        }

        for (String childName : targetChildrenNames) {
            if (!sourceChildrenNames.contains(childName)) {
                if (isIgnored(target.getAbsolutePath(), childName) || isIgnored(source.getAbsolutePath(), childName)) {
                    continue;
                }
                errors.add("Node missing in source: " + target.getAbsolutePath() + "/" + childName);
            }
        }
        
        for (Node sourceChild : source.getChildren()) {
            String childName = sourceChild.getPath();
            if (targetChildrenNames.contains(childName)) {
                Node targetChild = findChild(target, childName);
                compareNodes(sourceChild, targetChild, errors);
            }
        }
    }

    private boolean isIgnored(String parentPath, String childName) {
        String path = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;
        return ignoredPaths.contains(path);
    }


    private Node findChild(Node parent, String name) {
        for (Node child : parent.getChildren()) {
            if (child.getPath().equals(name)) {
                return child;
            }
        }
        return null;
    }
}

