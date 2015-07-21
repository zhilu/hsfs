package hdfs.zookeeper.queue;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 实现的思路非常简单，在/queue-fifo的目录下创建 SEQUENTIAL 类型的子目录
 * /x(i)，这样就能保证所有成员加入队列时都是有编号的，出队列时通过 getChildren( )
 * 方法可以返回当前所有的队列中的元素，然后消费其中最小的一个，这样就能保证FIFO。
 * 
 * @author shi
 *
 */
public class FIFOZooKeeper {

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			doOne();
		} else {
			doAction(Integer.parseInt(args[0]));
		}
	}

	public static void doOne() throws Exception {
		String host1 = "192.168.1.108:2181";
		ZooKeeper zk = connection(host1);
		initQueue(zk);
		produce(zk, 1);
		produce(zk, 2);
		cosume(zk);
		cosume(zk);
		cosume(zk);
		zk.close();
	}

	public static void doAction(int client) throws Exception {
		String host1 = "192.168.1.201:2181";
		String host2 = "192.168.1.201:2182";
		String host3 = "192.168.1.201:2183";

		ZooKeeper zk = null;
		switch (client) {
		case 1:
			zk = connection(host1);
			initQueue(zk);
			produce(zk, 1);
			break;
		case 2:
			zk = connection(host2);
			initQueue(zk);
			produce(zk, 2);
			break;
		case 3:
			zk = connection(host3);
			initQueue(zk);
			cosume(zk);
			cosume(zk);
			cosume(zk);
			break;
		}
	}

	public static ZooKeeper connection(String host) throws IOException {
		return new ZooKeeper(host, 60000, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println(event.toString());
			}
		});
	}

	public static void initQueue(ZooKeeper zk) throws KeeperException,
			InterruptedException {
		System.out.println(zk.exists("/queue-fifo", false));
		if (zk.exists("/queue-fifo", false) == null) {
			System.out.println("create /queue-fifo task-queue-fifo");
			zk.create("/queue-fifo", "task-queue-fifo".getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			System.out.println("/queue-fifo is exist!");
		}
	}

	public static void produce(ZooKeeper zk, int x) throws KeeperException,
			InterruptedException {
		System.out.println("create /queue-fifo/x" + x + " x" + x);
		zk.create("/queue-fifo/x" + x, ("x" + x).getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	public static void cosume(ZooKeeper zk) throws KeeperException,
			InterruptedException {
		List<String> list = zk.getChildren("/queue-fifo", true);
		if (list.size() > 0) {
			long min = Long.MAX_VALUE;
			for (String num : list) {
				if (min > Long.parseLong(num.substring(1))) {
					min = Long.parseLong(num.substring(1));
				}
			}
			System.out.println("delete /queue/x" + min);
			//zk.delete("/queue-fifo/x" + min, 0);
		} else {
			System.out.println("No node to cosume");
		}
	}

}
