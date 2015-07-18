package hdfs.zookeeper.confmanage;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class MyClient implements Watcher {
	public static String url = "192.168.1.108:2181";

	private final static String root = "/myConf";
	// 节点配置
	private  static String UrlNode = root + "/url";
	private  static String userNameNode = root + "/username";
	private  static String passWdNode = root + "/passwd";

	public static String authType = "digest";
	public static String authPassWd = "password";

	// 实际数据
	private  static String urlString;
	private  static String userName;
	private  static String password;

	ZooKeeper zk = null;
	public  String getPassword() {
		return password;
	}

	public  void setPassword(String password) {
		MyClient.password = password;
	}

	public  String getUrlString() {
		return urlString;
	}

	public  void setUrlString(String urlString) {
		MyClient.urlString = urlString;
	}

	public  String getUserName() {
		return userName;
	}

	public  void setUserName(String userName) {
		MyClient.userName = userName;
	}


	public void initValue(){
		try {
			urlString= new String(zk.getData(UrlNode, true, null)) ;
			userName= new String(zk.getData(userNameNode, true, null)) ;
			password= new String(zk.getData(passWdNode, true, null)) ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ZooKeeper getZK() throws Exception{
		zk = new ZooKeeper(url, 1000, this);
		zk.addAuthInfo(authType, authPassWd.getBytes());
		while (ZooKeeper.States.CONNECTED != zk.getState()) {
			Thread.sleep(1000);
		}
		return zk;
	}
	
	public static void main(String[] args) throws Exception {
		MyClient myclient = new MyClient();
		
		ZooKeeper zk = myclient.getZK();
		myclient.initValue();
		
		int counter=0;
		while(true){
			System.out.println(myclient.getUrlString());
			System.out.println(myclient.getUserName());
			System.out.println(myclient.getPassword());
			Thread.sleep(1000);
			counter++;
			if(counter>5){
				break;
			}
		}
		zk.close();
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getType()==Watcher.Event.EventType.None){
			System.out.println("连接服务器成功");
		}else if(event.getType()==Watcher.Event.EventType.NodeCreated){
			System.out.println("节点创建成功");
		}else if(event.getType()==Watcher.Event.EventType.NodeChildrenChanged){
			System.out.println("子节点更新成功");
			initValue();
		}else if(event.getType()==Watcher.Event.EventType.NodeDataChanged){
			System.out.println("数据更新成功");
			initValue();
		}else if(event.getType()==Watcher.Event.EventType.NodeDeleted){
			System.out.println("几点删除成功");
		}
	}

}
