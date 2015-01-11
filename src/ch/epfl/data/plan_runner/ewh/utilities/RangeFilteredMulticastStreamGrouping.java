package ch.epfl.data.plan_runner.ewh.utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.ewh.data_structures.ListAdapter;
import ch.epfl.data.plan_runner.ewh.storm_components.OkcanSampleMatrixBolt;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.SystemParameters.HistogramType;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

// This class is used only for connecting D2Combiner to S1Reservoir,
//   when D2Combiner computes only d2_equi and has some duplicates of it over the boundaries (that's why filtering) due to a equi-depth histogram on R2
//   and S1Reservoir computes d2 using another equi-depth histogram on R1
public class RangeFilteredMulticastStreamGrouping extends RangeMulticastStreamGrouping {
	private static Logger LOG = Logger.getLogger(RangeFilteredMulticastStreamGrouping.class);
	private List _srcRangeBoundaries; // (numTargetTasks - 1) of them
	
	private String _parentCompName;
	private Map<Integer, Integer> _parentTaskIdtoIndex = new HashMap<Integer, Integer>(); // Id is Storm-Dependent, Index = [0, parallelism)
	
	// with multicast
	public RangeFilteredMulticastStreamGrouping(Map map, ComparisonPredicate comparison, NumericConversion wrapper, 
			HistogramType dstHistType, HistogramType srcHistType, String parentCompName) {
		super(map, comparison, wrapper, dstHistType);
		_parentCompName = parentCompName;
		//LOG.info("I should be  EWH_SAMPLE_D2_COMBINER = " + _parentCompName);
		int numLastJoiners = SystemParameters.getInt(_map, "PAR_LAST_JOINERS");
		_srcRangeBoundaries = createBoundariesFromHistogram(srcHistType.filePrefix(), numLastJoiners);
	}
	
	@Override
	public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi, List<Integer> targetTasks) {
		List<Integer> parentTaskIds = wtc.getComponentTasks(_parentCompName);
		for(int i = 0; i < parentTaskIds.size(); i++){
			// creating inverted index
			// this should be the same order as in R2.targetTasks when sending to D2Combiner; this method is used in theta-joins (particularly dynamic)
			int parentTaskId = parentTaskIds.get(i);
			_parentTaskIdtoIndex.put(parentTaskId, i);
		}
		
		super.prepare(wtc, gsi, targetTasks);
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
		final List<String> tuple = (List<String>) stormTuple.get(1);
		final String tupleHash = (String) stormTuple.get(2);
		if(!MyUtilities.isFinalAck(tuple, _map)){
			// taskId is Storm-dependent sender Id
			int taskIndex = _parentTaskIdtoIndex.get(taskId); // translation to [0, parallelism)
			if(!isWithinBoundaries(taskIndex, tupleHash, _srcRangeBoundaries)){
				//LOG.info("FILTERED TUPLE key = " + tupleHash + " in taskIndex " + taskIndex + "!");
				//send it nowhere
				return new ArrayList<Integer>();
			}
		}
		return super.chooseTasks(taskId, stormTuple);
	}
	
	private boolean isWithinBoundaries(int taskIndex, String strKey, List boundaries){
		Object key = _wrapper.fromString(strKey);
		int keyTaskIndex = chooseTaskIndex(key, boundaries);
		//LOG.info("For each nonFinalAckTuple: key = " + key + ", keyTaskIndex = " + keyTaskIndex);
		return keyTaskIndex == taskIndex;
	}
}