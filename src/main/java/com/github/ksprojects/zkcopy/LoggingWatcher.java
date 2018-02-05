package com.github.ksprojects.zkcopy;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Watcher implementation that simply logs at debug.
 */
public class LoggingWatcher implements Watcher {
    Logger logger = Logger.getLogger(LoggingWatcher.class);
    
    @Override
    public void process(WatchedEvent event) {
        logger.debug("Ignoring watched event: " + event);
    }
}