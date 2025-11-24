package com.github.ksprojects.zkcopy.comparator;

import com.github.ksprojects.zkcopy.Node;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class Comparator {
    private static final Logger LOGGER = Logger.getLogger(Comparator.class);
    private final Node sourceRoot;
    private final Node targetRoot;
    
    public Comparator(Node sourceRoot, Node targetRoot) {
        this.sourceRoot = sourceRoot;
        this.targetRoot = targetRoot;
    }
    
    public boolean compare() {
        List<String> errors = new ArrayList<>();
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

        if (!Arrays.equals(source.getData(), target.getData())) {
            errors.add("Data mismatch at " + source.getAbsolutePath() + " vs " + target.getAbsolutePath());
        }

        Set<String> sourceChildrenNames = source.getChildrenNamed();
        Set<String> targetChildrenNames = target.getChildrenNamed();

        for (String childName : sourceChildrenNames) {
            if (!targetChildrenNames.contains(childName)) {
                errors.add("Node missing in target: " + source.getAbsolutePath() + "/" + childName);
            }
        }

        for (String childName : targetChildrenNames) {
            if (!sourceChildrenNames.contains(childName)) {
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

    private Node findChild(Node parent, String name) {
        for (Node child : parent.getChildren()) {
            if (child.getPath().equals(name)) {
                return child;
            }
        }
        return null;
    }
}

