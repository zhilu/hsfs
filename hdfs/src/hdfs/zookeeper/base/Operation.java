package hdfs.zookeeper.base;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Operation {

	private static final int SESSION_TIMEOUT = 30000;
	ZooKeeper zk;

	public static void main(String[] args) throws Exception {
		Operation dm = new Operation();
		dm.zkServerCreate();
		dm.zkOperations();
		dm.ZKClose();
	}

	Watcher wh = new Watcher() {
		@Override
		public void process(org.apache.zookeeper.WatchedEvent event) {
			System.out.println(event.toString());
		}

	};

	public void zkOperations() throws Exception {
		System.out
				.println("/n1. 创建 ZooKeeper 节点"
						+ " (znode ： zoo2, 数据： myData2 ，权限：OPEN_ACL_UNSAFE ，节点类型： Persistent");

		zk.create("/zoo2", "myData2".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);

		System.out.println("/n2. 查看是否创建成功：");
		System.out.println(new String(zk.getData("/zoo2", true, null)));
		System.out.println("/n3. 修改节点数据 ");

		zk.setData("/zoo2", "shenlan211314".getBytes(), -1);
		System.out.println("/n4. 查看是否修改成功： ");
		System.out.println(new String(zk.getData("/zoo2", true, null)));
		System.out.println("/n5. 删除节点 ");

		zk.delete("/zoo2", -1);
		System.out.println("/n6. 查看节点是否被删除： ");
		System.out.println(" 节点状态： [" + zk.exists("/zoo2", false) + "]");

	}

	private void ZKClose() throws Exception {
		zk.close();
	}

	public void zkServerCreate() throws Exception {
		zk = new ZooKeeper("localhost:2181", SESSION_TIMEOUT, this.wh);
	}

}
