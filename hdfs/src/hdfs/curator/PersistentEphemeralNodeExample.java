package hdfs.curator;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

public class PersistentEphemeralNodeExample {


    private static final String PATH = "/example/ephemeralNode";
    private static final String PATH2 = "/example/node";

    public static void main(String[] args) throws Exception {
        TestingServer server = new TestingServer();
        CuratorFramework client = null;
        PersistentEphemeralNode node = null;
        try {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    System.out.println("client state:" + newState.name());

                }
            });
            client.start();

            //http://zookeeper.apache.org/doc/r3.2.2/api/org/apache/zookeeper/CreateMode.html
            //临时节点驻存在ZooKeeper中，当连接和session断掉时被删除。
			// EPHEMERAL： 以ZooKeeper的 CreateMode.EPHEMERAL方式创建节点。
			// EPHEMERAL_SEQUENTIAL:
			// 如果path已经存在，以CreateMode.EPHEMERAL创建节点，否则以CreateMode.EPHEMERAL_SEQUENTIAL方式创建节点。
			// PROTECTED_EPHEMERAL: 以CreateMode.EPHEMERAL创建，提供保护方式。
			// PROTECTED_EPHEMERAL_SEQUENTIAL: 类似EPHEMERAL_SEQUENTIAL，提供保护方式。

            node = new PersistentEphemeralNode(client, Mode.EPHEMERAL,PATH, "test".getBytes());
            node.start();
            node.waitForInitialCreate(3, TimeUnit.SECONDS);
            String actualPath = node.getActualPath();
            System.out.println("node " + actualPath + " value: " + new String(client.getData().forPath(actualPath)));

            client.create().forPath(PATH2, "persistent node".getBytes());
            System.out.println("node " + PATH2 + " value: " + new String(client.getData().forPath(PATH2)));
            KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());
            System.out.println("node " + actualPath + " doesn't exist: " + (client.checkExists().forPath(actualPath) == null));
            System.out.println("node " + PATH2 + " value: " + new String(client.getData().forPath(PATH2)));

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(node);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(server);
        }

    }

}
