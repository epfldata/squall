package ch.epfl.data.squall.components.signal_components;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.signal_components.storm.SignalClient;
import clojure.lang.LockingTransaction.Info;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;

public class SignalSpout extends BaseRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Random _rnd;
	private transient SignalClient _sc;
	private String _zookeeperhost, _syncedSpoutName;
	private int _currentValue=0;
	private static Logger LOG = Logger.getLogger(SignalSpout.class);
	
	public SignalSpout(String zookeeperhost, String syncedSpoutName) {
		_rnd = new Random();
		_zookeeperhost=zookeeperhost;
		_syncedSpoutName=syncedSpoutName;
	}
	
	@Override
	public void close() {
		super.close();
		_sc.close();
	}

	@Override
	public void nextTuple() {
		
		Utils.sleep(1000*2);
		
        try {
        	_currentValue= _rnd.nextInt(100);
			_sc.send(toBytes(_currentValue));
			LOG.info("Signaller sending ....."+_currentValue);
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
	
	private byte[] toBytes(int i)
	{
	  byte[] result = new byte[4];
	  result[0] = (byte) (i >> 24);
	  result[1] = (byte) (i >> 16);
	  result[2] = (byte) (i >> 8);
	  result[3] = (byte) (i /*>> 0*/);
	  return result;
	}

}
