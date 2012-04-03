package stormComponents.synchronization;

import java.util.Map;

import utilities.MyUtilities;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import stormComponents.StormComponent;
import utilities.SystemParameters;

import org.apache.log4j.Logger;

public class TrafficLight extends BaseRichSpout implements IRichSpout, StormComponent {
        private static Logger LOG = Logger.getLogger(TrafficLight.class);

	private int _ID;
	private int _interval;
	private SpoutOutputCollector _collector;
	
	public TrafficLight(int interval, TopologyBuilder builder) {
		_ID = MyUtilities.getNextTopologyId();
		_interval = interval;
		builder.setSpout(Integer.toString(_ID), this);
	}

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		_collector.emit(SystemParameters.TrafficLightStream, new Values("Switch"));
		Utils.sleep(_interval);
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		_collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declareStream(SystemParameters.TrafficLightStream, new Fields("SWITCH"));
	}

        //Helper methods
        @Override
        public int getID() {
            return _ID;
	}

        @Override
        public String getInfoID(){
            String str = "TrafficLight has ID: " + _ID;
            return str;
        }

}
