package com.github.ksprojects.zkcopy.reader;

import com.github.ksprojects.zkcopy.Node;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

/**
 * ZooKeeper data reader
 *
 * @author schepanovsky@gmail.com
 */
public final class Reader {
    private static Logger logger = Logger.getLogger(Reader.class);
    private final int threadsNumber;
    private final String source;
    private String server;
    private String path;
    private int timeout;

    /**
     * Create new reader instance for a given source.
     *
     * @param source address of the data to read
     * @param threads number of concurrent thread for reading data
     * @param timeout the session timeout for read operations
     */
    public Reader(String source, int threads, int timeout) {
        threadsNumber = threads;
        this.source = source;
        this.timeout = timeout;
        parseSource();
    }

    private void parseSource() {
        int p = source.indexOf('/');
        server = source.substring(0, p);
        path = source.substring(p);
    }

    /**
     * Read data from the source.
     */
    public Node read() {
        logger.info("Reading " + path + " from " + server);

        Node znode = new Node(path);

        ReaderThreadFactory threadFactory = new ReaderThreadFactory(server, timeout);
        ExecutorService pool = Executors.newFixedThreadPool(threadsNumber, threadFactory);
        AtomicInteger totalCounter = new AtomicInteger(0);
        AtomicInteger processedCounter = new AtomicInteger(0);
        AtomicBoolean failed = new AtomicBoolean(false);
        pool.execute(new NodeReader(pool, znode, totalCounter, processedCounter, failed));
        try {
            while (true) {
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
        } catch (InterruptedException e) {
            logger.error("Await Termination of pool was unsuccessful", e);
            return null;
        } finally {
            threadFactory.closeZookeepers();
        }
        if (failed.get()) {
            return null;
        }
        return znode;
    }

}
