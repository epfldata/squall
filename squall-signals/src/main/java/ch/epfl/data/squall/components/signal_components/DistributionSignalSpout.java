package ch.epfl.data.squall.components.signal_components;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;
import ch.epfl.data.squall.components.signal_components.storm.SignalClient;

public class DistributionSignalSpout extends BaseRichSpout {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    private Random _rnd;
    private transient SignalClient _sc;
    private String _zookeeperhost, _syncedSpoutName;
    private int _currentValue = 0;
    private static Logger LOG = Logger.getLogger(DistributionSignalSpout.class);
    private int _refreshTimeSecs;
    

    public DistributionSignalSpout(String zookeeperhost, String syncedSpoutName, int refreshTimeSecs) {
	_rnd = new Random();
	_zookeeperhost = zookeeperhost;
	_syncedSpoutName = syncedSpoutName;
	_refreshTimeSecs=refreshTimeSecs;
    }

    @Override
    public void close() {
	super.close();
	_sc.close();
    }

    @Override
    public void nextTuple() {
	Utils.sleep(1000 * _refreshTimeSecs);
	try {
		
	    _currentValue = _rnd.nextInt(100);
	    byte[] signal= SignalUtilities.createSignal(SignalUtilities.DISTRIBUTION_SIGNAL, SignalUtilities.toBytes(_currentValue));
	    _sc.send(signal);
	    LOG.info("Distribution Signaller sending ....." + _currentValue);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    @Override
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
	_sc = new SignalClient(_zookeeperhost, _syncedSpoutName);
	_sc.start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {

    }


}
