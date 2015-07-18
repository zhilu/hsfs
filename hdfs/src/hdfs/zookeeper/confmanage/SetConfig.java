package hdfs.zookeeper.confmanage;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * zookeeper 配置管理，设置配置项
 * 
 * @author shi
 *
 */

public class SetConfig {

	public static String url = "localhost:2181";
	
	private final static String root = "/myConf";
	// 节点配置
	private final static String UrlNode = root + "/url";
	private final static String userNameNode = root + "/username";
	private final static String passWdNode = root + "/passwd";

	private final static String auth_type = "digest";
	private final static String auth_passwd = "password";

	// 实际数据
	private final static String urlString = "database Url";
	private final static String userName = "username";
	private final static String password = "password123";

	public static void main(String[] args) throws Exception {

		ZooKeeper zk = new ZooKeeper(url, 3000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("触发了事件： " + event.getType());
			}
		});
		zk.addAuthInfo(auth_type, auth_passwd.getBytes());
		
		System.out.println(zk.getState());
		while (ZooKeeper.States.CONNECTED != zk.getState()) {
			Thread.sleep(1000);
		}
		
		
		
	//	zk.delete(UrlNode, -1);

		if (zk.exists(root, true) == null) {
			zk.create(root, "root".getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		if (zk.exists(UrlNode, true) == null) {
			zk.create(UrlNode, urlString.getBytes(),
					Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
		}
		if (zk.exists(userNameNode, true) == null) {
			zk.create(userNameNode, userName.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		if (zk.exists(passWdNode, true) == null) {
			zk.create(passWdNode, password.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}

	}

}
