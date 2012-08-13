package com.sysiq.tools.zkcopy.reader;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.sysiq.tools.zkcopy.Node;

/**
 * ZooKeeper data reader
 * @author schepanovsky@gmail.com
 *
 */
public class Reader
{
    private final int threadsNumber;
    private final String source;
    private String server;
    private String path;
    
    private static Logger logger = Logger.getLogger(Reader.class);
    
    public Reader(String source) {
        threadsNumber = 1;
        this.source = source;
        parseSource();        
    }
    
    public Reader(String source, int nThreads) {
        threadsNumber = nThreads;
        this.source = source;
        parseSource();
    }
    
    private void parseSource() {
        int p = source.indexOf('/');
        server = source.substring(0, p);
        path = source.substring(p);
    }
    
    /**
     * 
     * @return ZkNode data tree
     */
    public Node read() {
        logger.info("Reading " + path + " from " + server);
        
        Node znode = new Node(path);
        
        ExecutorService pool = Executors.newFixedThreadPool(threadsNumber, new ReaderThreadFactory(server));        
        AtomicInteger totalCounter = new AtomicInteger(0);
        AtomicInteger processedCounter = new AtomicInteger(0);
        pool.execute(new NodeReader(pool, znode, totalCounter, processedCounter));
        try
        {
            while(true) {
                if (pool.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.info("Completed.");
                    break;
                } 
                logger.info("Processing, total=" + totalCounter + ", processed=" + processedCounter);
                if (totalCounter.get() == processedCounter.get()) {
                    // all work finished
                    pool.shutdown();
                }
            }
        }
        catch(InterruptedException e)
        {
            logger.error("Await Termination of pool was unsuccessful", e);
            return null;
        }
        return znode;
    }
    
}
