package stormComponents;

import backtype.storm.Config;
import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import utilities.SystemParameters;

import org.apache.log4j.Logger;
import stormComponents.synchronization.TopologyKiller;
import stormComponents.synchronization.TrafficLight;
import utilities.MyUtilities;

public class StormSrcHarmonizer extends BaseRichBolt implements StormComponent {
	private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(StormSrcHarmonizer.class);

        private OutputCollector _collector;

        private int _ID;
        private String _componentName;

	public StormSrcHarmonizer(String componentName,
                StormEmitter firstEmitter,
                StormEmitter secondEmitter,
                TopologyBuilder builder,
                TrafficLight trafficLight,
                TopologyKiller killer,
                Config conf){

		_componentName = componentName;
		
                _ID= MyUtilities.getNextTopologyId();

                int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
                InputDeclarer currentBolt = builder.setBolt(Integer.toString(_ID), this, parallelism);
                currentBolt = MyUtilities.attachEmitterComponents(currentBolt, firstEmitter, secondEmitter);

                /*
                if (trafficLight != null) {
		   currentBolt.allGrouping(trafficLight.getID(), SystemParameters.TrafficLightStream);
		}
                _hasTrafficLight = true;
                _turn = _firstSourceID;
		_buffer = new ArrayList<Tuple>();
                 */
	}


        /*
	public StormSrcHarmonizer(String componentName) {
		_ID= MyUtilities.getNextTopologyId();
		_hasTrafficLight = false;
                _componentName = componentName;
	}*/

	// from IRichBolt
	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple tuple) {
            String inputComponentName=tuple.getString(0);
            String inputTupleString=tuple.getString(1);
            String inputTupleHash=tuple.getString(2);

            _collector.emit(tuple, new Values(inputComponentName, inputTupleString, inputTupleHash));
            _collector.ack(tuple);

            /*
		if (_hasTrafficLight && (tuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.TrafficLightStream))) {
                        //LOG.info("StormSrcHarmonizer " + getID() + " is switching...");
			// Flush previous buffer
			int buffersize = _buffer.size();
			//LOG.info("StormSrcHarmonizer " + getID() + " found "+buffersize +" tuples buffered");
			for (int i = 0 ; i < buffersize ; i++) {
				Tuple t = _buffer.remove(0);
    			String inputComponentName = t.getString(0);
    			String inputTupleString = t.getString(1);
    			String hash = t.getString(2);
    			Values v = new Values();
    			v.add(inputComponentName);
    			v.add(inputTupleString);
    			v.add(hash);
    			_collector.emit(t, v);
    			_collector.ack(t);
			}
			assert(_buffer.isEmpty());
			// Change turn
			_turn = (_turn == _firstSourceID) ? _secondSourceID : _firstSourceID;
			return;
		}
		
		if (!_hasTrafficLight || (_turn == tuple.getSourceComponent())) {
			String tableName=tuple.getString(0);
			String tuplePayLoad=tuple.getString(1);
			String hash=tuple.getString(2);
			Values v = new Values();
			v.add(tableName);
			v.add(tuplePayLoad);
			v.add(hash);
			_collector.emit(tuple, v);
                        _collector.ack(tuple);
		} else {
			_buffer.add(tuple);
		}*/
	
	}

	@Override
	public void prepare(Map conf, TopologyContext arg1, OutputCollector collector) {
		_collector=collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		ArrayList<String> outputFields= new ArrayList<String>();
		outputFields.add("TableName");
		outputFields.add("Tuple");
		outputFields.add("Hash");		
		declarer.declare(new Fields(outputFields));
	}

        //from StormComponent
        @Override
        public int getID() {
            return _ID;
	}

        @Override
        public String getInfoID() {
            String str = "Harmonizer " + _componentName + " has ID: "+ _ID;
            return str;
        }
}