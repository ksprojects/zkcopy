package com.github.ksprojects;

import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.reader.Reader;
import com.github.ksprojects.zkcopy.writer.Writer;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zkcopy", showDefaultValues = true)
public class ZkCopy implements Callable<Void> {

    private static final Logger LOGGER = Logger.getLogger(ZkCopy.class);
    private static final int DEFAULT_THREADS_NUMBER = 10;
    private static final boolean DEFAULT_COPY_ONLY = false;
    private static final boolean DEFAULT_IGNORE_EPHEMERAL_NODES = true;
    private static final int DEFAULT_BATCH_SIZE = 10000;

    
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

    @Option(names = { "-b", "--batchSize" },
            description = "Batch write operations into transactions of this many operations.")
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
        Reader reader = new Reader(source, workers);
        Node root = reader.read();
        if (root != null) {
            Writer writer = new Writer(target, root, removeDeprecatedNodes, ignoreEphemeralNodes, mtime, batchSize);
            writer.write();
        } else {
            LOGGER.error("FAILED");
        }
        return null;
    }

}
