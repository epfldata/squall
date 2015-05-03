package ch.epfl.data.squall.components.signal_components;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import scala.Array;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;
import ch.epfl.data.squall.components.signal_components.storm.BaseSignalSpout;
import ch.epfl.data.squall.components.signal_components.storm.SignalClient;

public class HarmonizerSignalSpout extends BaseSignalSpout{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(HarmonizerSignalSpout.class);
	private transient SignalClient _scDataSpout;
	private String _zookeeperhost, _syncedSpoutName;
	private boolean _isChange=false;
	private transient Histogram _freqHisto; 
	private transient HashSet<Integer> _frequentSet;
	private int _windowCountThreshold, _frequentThreshold;
	
	 

	public HarmonizerSignalSpout(String zookeeperhost, String spoutName, String harmonizerSpoutName, int windowCountThreshold, int frequentThreshold){
		super(harmonizerSpoutName);
		_syncedSpoutName=spoutName;
		_zookeeperhost=zookeeperhost;
	}

	/**
	 * Signals from the datasources 
	 * ~ Handle update frequencies
	 */
	@Override
	public void onSignal(byte[] data) {
		try{
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInput in = null;
			in = new ObjectInputStream(bis);
			HashMap<Integer, Integer> inputStats = (HashMap<Integer, Integer>)in.readObject();
			HashSet<Integer> result= _freqHisto.update(inputStats);
			if(result!=null){
				_isChange=true;
				_frequentSet=result;
			}
			bis.close();
			in.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void print(HashMap<Integer, Integer> hsh){
		for (Entry<Integer, Integer> element : hsh.entrySet()) {
			int key=element.getKey();
			int value= element.getValue();
			System.out.println(key+" "+value);
		}
	}

	/**
	 * Send Signals if there are any or Sleep!
	 */
	@Override
	public void nextTuple() {
		// TODO Either sleep or Send new set of random shuffling
		if(!_isChange)
			Utils.sleep(5000);
		else
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutput out = null;
				out = new ObjectOutputStream(bos);   
				out.writeObject(_frequentSet);
				byte[] objectBytes = bos.toByteArray();
				byte[] signal= SignalUtilities.createSignal(SignalUtilities.HARMONIZER_SIGNAL, objectBytes);
				_scDataSpout.send(signal);
				_isChange=false;
				LOG.info("Harmonizer sending frequent set....."+ _frequentSet.toString()+" " + _frequentSet.size());
				out.close();
				bos.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		_scDataSpout = new SignalClient(_zookeeperhost, _syncedSpoutName);
		_scDataSpout.start();
		_freqHisto= new Histogram(_windowCountThreshold, _frequentThreshold);
		_frequentSet= new HashSet<>();
	}

	@Override
	public void close() {
		super.close();
		_scDataSpout.close();
	}

}
