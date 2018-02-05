package com.github.ksprojects;

import com.github.ksprojects.zkcopy.LoggingWatcher;
import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.reader.Reader;
import com.github.ksprojects.zkcopy.writer.Writer;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zkcopy", showDefaultValues = true)
public class ZkCopy implements Callable<Void> {

    private static final Logger LOGGER = Logger.getLogger(ZkCopy.class);
    private static final int DEFAULT_THREADS_NUMBER = 10;
    private static final boolean DEFAULT_COPY_ONLY = false;
    private static final boolean DEFAULT_IGNORE_EPHEMERAL_NODES = true;
    private static final int DEFAULT_BATCH_SIZE = 1000;

    @Option(names = "--help", usageHelp = true, description = "display this help and exit")
    boolean help;
    
    @Option(names = { "-s", "--source" }, 
            paramLabel = "server:port/path", 
            required = true, 
            description = "location of a source tree to copy")
    String source;

    @Option(names = { "-t", "--target" }, 
            paramLabel = "server:port/path", 
            required = true, 
            description = "target location")
    String target;

    @Option(names = { "-w", "--workers" }, 
            description = "number of concurrent workers to copy data")
    int workers = DEFAULT_THREADS_NUMBER;

    @Option(names = { "-c", "--copyOnly" },
            description = "set this flag if you do not want to remove nodes that are removed on source",
            arity = "0..1")
    boolean copyOnly = DEFAULT_COPY_ONLY;

    @Option(names = { "-i", "--ignoreEphemeralNodes" },
            description = "set this flag to false if you do not want to copy ephemeral ZNodes",
            arity = "0..1")
   
    boolean ignoreEphemeralNodes = DEFAULT_IGNORE_EPHEMERAL_NODES;

    @Option(names = { "-m", "--mtime" },
            description = "Ignore nodes older than mtime")
    long mtime = -1;
    
    @Option(names = { "--timeout" }, description = "Session timeout in milliseconds")
    int sessionTimeout = 40000;

    @Option(names = { "-b", "--batchSize" },
            description = "Batch write operations into transactions of this many operations. " 
                        + "Batch sizes are limited by the jute.maxbuffer server-side config, usually around 1 MB.")
    int batchSize = DEFAULT_BATCH_SIZE;

    /**
     * Main entry point - start ZkCopy.
     */
    public static void main(String[] args) {
        CommandLine.call(new ZkCopy(), System.err, args);
    }

    @Override
    public Void call() throws Exception {
        boolean removeDeprecatedNodes = !copyOnly;
        LOGGER.info("using " + workers + " concurrent workers to copy data");
        LOGGER.info("delete nodes = " + String.valueOf(removeDeprecatedNodes));
        LOGGER.info("ignore ephemeral nodes = " + String.valueOf(ignoreEphemeralNodes));
        Reader reader = new Reader(source, workers, sessionTimeout);
        Node root = reader.read();
        if (root != null) {
            ZooKeeper zookeeper = null;
            try {
                zookeeper = new ZooKeeper(zkHost(target), sessionTimeout, new LoggingWatcher());
                Writer writer = new Writer(zookeeper, zkPath(target), root, removeDeprecatedNodes, ignoreEphemeralNodes,
                        mtime, batchSize);
                writer.write();
            } finally {
                if (zookeeper != null) {
                    zookeeper.close();
                }
            }
        } else {
            LOGGER.error("FAILED");
        }
        return null;
    }
    
    
    /**
     * Returns the host part of the ZK target
     * 
     * e.g. if passed 127.0.0.1:1234/parent/child, returns 127.0.0.1:1234
     * @param addr the target address
     * @return Zookeeper Host/port
     */
    private String zkHost(String addr) {
        return addr.split("/", 2)[0];
    }
    
    /**
     * Returns the path of the ZK target
     * 
     * e.g. if passed 127.0.0.1:1234/parent/child, returns `parent/child`
     * @param addr the target address
     * @return Zookeeper target path
     */
    private String zkPath(String addr) {
        return '/'  + addr.split("/", 2)[1];
    }

}
