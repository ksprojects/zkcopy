package itests;
import static org.junit.Assert.assertEquals;

import com.github.ksprojects.ZkCopy;
import com.github.ksprojects.zkcopy.LoggingWatcher;
import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

public class ZkCopyIT {

    public static int ZOOKEEPER_PORT = 2181;

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static GenericContainer zkSource = new GenericContainer("zookeeper:latest").withExposedPorts(ZOOKEEPER_PORT);

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static GenericContainer zkDest = new GenericContainer("zookeeper:latest").withExposedPorts(ZOOKEEPER_PORT);


    @Before
    public void addData() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = connectTo(zkSource);
        for(String path : new String[] {"/itest", "/itest/persistent", "/itest/ephemeral"}) {
            zk.create(path, path.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        for (int i = 0; i < 200; i++) {
            zk.create(String.format("/itest/persistent/node-%s", i), Integer.toString(i).getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zk.close();
    }

    @Test
    public void testSimpleCopy() throws KeeperException, InterruptedException, IOException {
        ZooKeeper dest = connectTo(zkDest);
        ZooKeeper src = connectTo(zkSource);
        
        for (int i = 0; i < 10; i++) {
            src.create(String.format("/itest/ephemeral/node-%s", i), Integer.toString(i).getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }

        ZkCopy.main(new String[] {"-s", connectionString(zkSource, "itest"), "-t", connectionString(zkDest, "itest"), "--ignoreEphemeralNodes", "false"});
        
        List<String> persistentChildren = dest.getChildren("/itest/persistent", false);
        List<String> ephemeralChildren = dest.getChildren("/itest/ephemeral", false);
        dest.close();
        src.close();
        
        assertEquals("There should be 200 persistent nodes under /itest/persistent", 200, persistentChildren.size());
        assertEquals("There should be 10 ephemeral nodes under /itest/ephemeral", 10, ephemeralChildren.size());
    }
    
    /*
     * Utility functions
     */
    
    private static String connectionString(GenericContainer<?> container, String path) {
        String containerIpAddress = container.getContainerIpAddress();
        Integer mappedPort = container.getMappedPort(ZOOKEEPER_PORT);
        String hostPort = String.format("%s:%s", containerIpAddress, mappedPort);
        if(path != null) {
            return String.format("%s/%s", hostPort, path);
        } else {
            return hostPort;
        }
    }
    
    private static ZooKeeper connectTo(GenericContainer<?> container) throws IOException {
        return new ZooKeeper(connectionString(container, null), 20000, new LoggingWatcher());
    }
}