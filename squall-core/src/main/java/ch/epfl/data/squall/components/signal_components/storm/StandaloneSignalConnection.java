package ch.epfl.data.squall.components.signal_components.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.curator.retry.RetryNTimes;
public class StandaloneSignalConnection extends AbstractSignalConnection {
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneSignalConnection.class);

    private String connectString;
    private int zkRetries = 5;
    private int zkRetryInterval = 1000;

    public StandaloneSignalConnection(String name, SignalListener listener, String connectString) {
        this.name = name;
        this.listener = listener;
        this.connectString = connectString;
    }

    public void init() throws Exception {

        this.client = CuratorFrameworkFactory.builder().namespace(namespace).connectString(connectString)
                .retryPolicy(new RetryNTimes(this.zkRetries, this.zkRetryInterval)).build();
        this.client.start();
        super.initWatcher();
    }

}
