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
import java.util.List;
import utilities.SystemParameters;

import org.apache.log4j.Logger;
import stormComponents.synchronization.TopologyKiller;
import utilities.MyUtilities;

public class StormSrcHarmonizer extends BaseRichBolt implements StormComponent {
	private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(StormSrcHarmonizer.class);

        private OutputCollector _collector;
        private Map _conf;

        private int _ID;
        private String _componentName;
        private StormEmitter _firstEmitter, _secondEmitter;

        private int _numRemainingParents;

	public StormSrcHarmonizer(String componentName,
                StormEmitter firstEmitter,
                StormEmitter secondEmitter,
                TopologyBuilder builder,
                TopologyKiller killer,
                Config conf){
                _conf = conf;
		_componentName = componentName;
		
                _firstEmitter = firstEmitter;
                _secondEmitter = secondEmitter;

                _ID= MyUtilities.getNextTopologyId();

                int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
                InputDeclarer currentBolt = builder.setBolt(Integer.toString(_ID), this, parallelism);
                currentBolt = MyUtilities.attachEmitterComponents(currentBolt, _firstEmitter, _secondEmitter);

	}

	// from IRichBolt
	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple stormRcvTuple) {
            String inputComponentName=stormRcvTuple.getString(0);
            List<String> tuple = (List<String>) stormRcvTuple.getValue(1);
            String inputTupleHash=stormRcvTuple.getString(2);

            if(MyUtilities.isFinalAck(tuple, _conf)){
                _numRemainingParents--;
                MyUtilities.processFinalAck(_numRemainingParents, StormComponent.INTERMEDIATE, stormRcvTuple, _collector);
                return;
            }

            _collector.emit(stormRcvTuple, new Values(inputComponentName, tuple, inputTupleHash));
            _collector.ack(stormRcvTuple);
	}

        @Override
        public void tupleSend(List<String> tuple, Tuple stormTupleRcv) {
            throw new RuntimeException("Should not be here!");
        }

        @Override
        public void batchSend(){
            throw new RuntimeException("Should not be here!");
        }

	@Override
	public void prepare(Map conf, TopologyContext tc, OutputCollector collector) {
		_collector=collector;
                _numRemainingParents = MyUtilities.getNumParentTasks(tc, _firstEmitter, _secondEmitter);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> outputFields= new ArrayList<String>();
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

        public void printTuple(List<String> tuple) {
            //this is purposely empty
        }

        public void printContent() {
            //this class has no content: this is purposely empty
        }
}