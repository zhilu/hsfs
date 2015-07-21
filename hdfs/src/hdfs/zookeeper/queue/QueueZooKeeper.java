package hdfs.zookeeper.queue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 创建一个父目录 /queue，每个成员都监控(Watch)标志位目录/queue/start 是否存在，然后每个成员都加入这个队列，加入队列的方式就是创建
 * /queue/x(i)的临时目录节点，然后每个成员获取 /queue 目录的所有目录节点，也就是 x(i)。判断 i
 * 的值是否已经是成员的个数，如果小于成员个数等待 /queue/start 的出现，如果已经相等就创建 /queue/start。
 * 
 * @author shi 问题还很多
 *
 */
public class QueueZooKeeper {

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			doOne();
		} else {
			doAction(Integer.parseInt(args[0]));
		}
	}

	public static void doOne() throws Exception {
		String host1 = "192.168.1.201:2181";
		ZooKeeper zk = connection(host1);
		initQueue(zk);
		joinQueue(zk, 1);
		joinQueue(zk, 2);
		joinQueue(zk, 3);
		zk.close();
	}

	public static void initQueue(ZooKeeper zk) throws KeeperException,
			InterruptedException {
		System.out.println("WATCH => /queue/start");
		zk.exists("/queue/start", true);

		if (zk.exists("/queue", false) == null) {
			System.out.println("create /queue task-queue");
			zk.create("/queue", "task-queue".getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} else {
			System.out.println("/queue is exist!");
		}
	}

	public static void joinQueue(ZooKeeper zk, int x) throws KeeperException,
			InterruptedException {
		System.out.println("create /queue/x" + x + " x" + x);
		zk.create("/queue/x" + x, ("x" + x).getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		isCompleted(zk);
	}

	public static void isCompleted(ZooKeeper zk) throws KeeperException,
			InterruptedException {
		int size = 3;
		int length = zk.getChildren("/queue", true).size();

		System.out.println("Queue Complete:" + length + "/" + size);
		if (length >= size) {
			System.out.println("create /queue/start start");
			zk.create("/queue/start", "start".getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
		}
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
			joinQueue(zk, 1);
			break;
		case 2:
			zk = connection(host2);
			initQueue(zk);
			joinQueue(zk, 2);
			break;
		case 3:
			zk = connection(host3);
			initQueue(zk);
			joinQueue(zk, 3);
			break;
		}
	}

	// 创建一个与服务器的连接
	public static ZooKeeper connection(String host) throws Exception {
		ZooKeeper zk = new ZooKeeper(host, 60000, new Watcher() {
			// 监控所有被触发的事件
			@Override
			public void process(WatchedEvent event) {
				if (event.getType() == Event.EventType.NodeCreated
						&& event.getPath().equals("/queue/start")) {
					System.out.println("Queue has Completed.Finish testing!!!");
				}

			}
		});
		return zk;
	}

}
