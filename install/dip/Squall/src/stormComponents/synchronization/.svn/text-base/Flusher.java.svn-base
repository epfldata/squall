/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package stormComponents.synchronization;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import stormComponents.StormComponent;
import java.util.Map;
import utilities.SystemParameters;
import utilities.MyUtilities;

import org.apache.log4j.Logger;

public class Flusher extends BaseRichSpout implements StormComponent {
        private static Logger LOG = Logger.getLogger(Flusher.class);

	private int _ID;
	private static final long serialVersionUID = 1L;
	private boolean _flushInProgress;
	private int _spoutsAcked;
	private int _totalNumberSpouts;
	private SpoutOutputCollector _collector;
	private int _interval;
	
	public Flusher(int interval, int totalNumberSpouts, TopologyBuilder builder) {
		_ID = MyUtilities.getNextTopologyId();
		_spoutsAcked = 0;
		_interval = interval;
		_flushInProgress = false;
		_totalNumberSpouts = totalNumberSpouts;
		builder.setSpout(Integer.toString(_ID), this);
	}

	@Override
	public void close() {}

	@Override
	public void nextTuple() {
		if (_flushInProgress)
			return;
		
		Utils.sleep(_interval);
		// Emit to all the emitters we are connected to
		for ( int i = 0 ; i < _totalNumberSpouts ; i++) {
			_collector.emit(SystemParameters.FlushmessageStream + i, new Values("FLUSH"), "ACKthisFlush");
		}
//		LOG.info("BEGINNING FLUSHING...");
		_flushInProgress = true;
	}

	@Override
	public void ack(Object o) {
		_spoutsAcked++;
		//LOG.info(("SR"+ _totalNumberSpouts+" "+ _spoutsAcked + " " + (String)o);
		if (_spoutsAcked == _totalNumberSpouts) {
			_flushInProgress = false;
			_spoutsAcked = 0;
//			LOG.info("FLUSH ENDED");
		}
	}

	@Override
	public void fail(Object o) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		for ( int i = 0 ; i < _totalNumberSpouts ; i++) {
			ofd.declareStream(SystemParameters.FlushmessageStream + i, new Fields("FLUSH"));
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		_collector = arg2;
	}

        //from StormComponent
        @Override
        public int getID() {
            return _ID;
	}

        @Override
        public String getInfoID() {
            String str ="Flusher has ID: " + _ID;
            return str;
        }

}
