package ch.epfl.data.squall.components.signal_components.storm;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.utils.Utils;

public class StormSignalConnection extends AbstractSignalConnection {
    private static final Logger LOG = LoggerFactory
	    .getLogger(StormSignalConnection.class);

    public StormSignalConnection(String name, SignalListener listener) {
	this.name = name;
	this.listener = listener;
    }

    @SuppressWarnings("rawtypes")
    public void init(Map conf) throws Exception {
	String connectString = zkHosts(conf);
	int retryCount = Utils.getInt(conf.get("storm.zookeeper.retry.times"));
	int retryInterval = Utils.getInt(conf
		.get("storm.zookeeper.retry.interval"));

	this.client = CuratorFrameworkFactory.builder().namespace(namespace)
		.connectString(connectString)
		.retryPolicy(new RetryNTimes(retryCount, retryInterval))
		.build();
	this.client.start();

	super.initWatcher();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static String zkHosts(Map conf) {
	int zkPort = Utils.getInt(conf.get("storm.zookeeper.port"));
	List<String> zkServers = (List<String>) conf
		.get("storm.zookeeper.servers");

	Iterator<String> it = zkServers.iterator();
	StringBuffer sb = new StringBuffer();
	while (it.hasNext()) {
	    sb.append(it.next());
	    sb.append(":");
	    sb.append(zkPort);
	    if (it.hasNext()) {
		sb.append(",");
	    }
	}
	return sb.toString();
    }
}
