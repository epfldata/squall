package plan_runner.ewh.storm_components;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.ComponentProperties;
import plan_runner.conversion.DateIntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.ewh.algorithms.BSPAlgorithm;
import plan_runner.ewh.algorithms.InputOutputShallowCoarsener;
import plan_runner.ewh.algorithms.InputShallowCoarsener;
import plan_runner.ewh.algorithms.OkcanCandidateInputAlgorithm;
import plan_runner.ewh.algorithms.OkcanCandidateOutputAlgorithm;
import plan_runner.ewh.algorithms.OkcanExactInputAlgorithm;
import plan_runner.ewh.algorithms.OkcanExactOutputAlgorithm;
import plan_runner.ewh.algorithms.ShallowCoarsener;
import plan_runner.ewh.algorithms.TilingAlgorithm;
import plan_runner.ewh.algorithms.WeightPrecomputation;
import plan_runner.ewh.algorithms.optimality.MaxAvgOptimality;
import plan_runner.ewh.algorithms.optimality.OptimalityMetricInterface;
import plan_runner.ewh.algorithms.optimality.WeightFunction;
import plan_runner.ewh.data_structures.JoinMatrix;
import plan_runner.ewh.data_structures.KeyRegion;
import plan_runner.ewh.data_structures.Region;
import plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import plan_runner.ewh.main.PushStatisticCollector;
import plan_runner.ewh.visualize.UJMPVisualizer;
import plan_runner.ewh.visualize.VisualizerInterface;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.operators.ProjectOperator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.storage.AggregationStorage;
import plan_runner.storage.BasicStore;
import plan_runner.storage.KeyValueStore;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormEmitter;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.DeepCopy;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;
import plan_runner.utilities.statistics.StatisticsUtilities;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DummyBolt<JAT extends Number & Comparable<JAT>> extends BaseRichBolt implements StormEmitter{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(DummyBolt.class);
	
	private StormEmitter _lastJoiner;
	private final String _componentName;
	private Map _conf;
	
	private List<String> _allCompNames;
	
	private int _hierarchyPosition;
	private int _numRemainingParents;

	private OutputCollector _collector;
		
	public DummyBolt(StormEmitter lastJoiner, ComponentProperties cp,
			List<String> allCompNames, int hierarchyPosition,
			TopologyBuilder builder, TopologyKiller killer, Config conf) {
		_componentName = cp.getName();
		_allCompNames = allCompNames;
		
		_lastJoiner = lastJoiner;
		
		_hierarchyPosition = hierarchyPosition;
		_conf = conf;
		
		final int parallelism = SystemParameters.getInt(conf, _componentName + "_PAR");
		
		// connecting with previous level
		InputDeclarer currentBolt = builder.setBolt(_componentName, this, parallelism);
		currentBolt = MyUtilities.attachEmitterShuffle(_conf, currentBolt, lastJoiner.getName());

		if(_hierarchyPosition == StormComponent.FINAL_COMPONENT){
			killer.registerComponent(this, _componentName, parallelism);
		}
	}

	private void processNonLastTuple(String inputComponentIndex, String sourceStreamId, List<String> tuple) {
		// do nothing
	}

			
	private void finalizeProcessing(){
		// nothing
	}
	
	// BaseRichSpout
	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		_collector = collector;
		_numRemainingParents = MyUtilities
				.getNumParentTasks(tc, Arrays.asList(_lastJoiner));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(_hierarchyPosition == StormComponent.FINAL_COMPONENT){
			declarer.declareStream(SystemParameters.EOF_STREAM,
					new Fields(SystemParameters.EOF));
		}else{
			// do nothing
		}
	}
	
	// ----------- below you don't need to change --------------	
	// if true, we should exit from method which called this method
	@Override
	public void execute(Tuple stormTupleRcv) {
		final String inputComponentIndex = stormTupleRcv
				.getStringByField(StormComponent.COMP_INDEX); // getString(0);
		final List<String> tuple = (List<String>) stormTupleRcv
				.getValueByField(StormComponent.TUPLE); // getValue(1);
		String sourceStreamId = stormTupleRcv.getSourceStreamId();

		if (processFinalAck(tuple, stormTupleRcv))
			return;

		processNonLastTuple(inputComponentIndex, sourceStreamId, tuple);

		_collector.ack(stormTupleRcv);
	}
	
	protected boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv) {
		if (MyUtilities.isFinalAck(tuple, _conf)) {
			_numRemainingParents--;
			if (_numRemainingParents == 0) {
				finalizeProcessing();
			}
			MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, _conf,
					stormTupleRcv, _collector);			
			return true;
		}
		return false;
	}
	
	// from IRichBolt
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return _conf;
	}

	@Override
	public String[] getEmitterIDs() {
		return new String[]{_componentName};
	}

	@Override
	public String getName() {
		return _componentName;
	}
	
	@Override
	public String getInfoID() {
		throw new RuntimeException("Should not be here!");
	}
}