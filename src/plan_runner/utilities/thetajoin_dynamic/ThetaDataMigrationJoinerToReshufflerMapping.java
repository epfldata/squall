package plan_runner.utilities.thetajoin_dynamic;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import plan_runner.utilities.SystemParameters;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class ThetaDataMigrationJoinerToReshufflerMapping implements CustomStreamGrouping {

	/**
	 * This class is only responsible for the mapping for
	 * ThetaDataMigrationJoinerToReshuffler ... ONLY !!
	 */
	private static final long serialVersionUID = 1L;

	private List<Integer> _targetTasks;
	private Random rnd;

	public ThetaDataMigrationJoinerToReshufflerMapping(Map map, int seed) {
		if (seed >= 0)
			rnd = new Random(seed);
		else
			rnd = new Random();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {

		final List<String> tupleList = (List<String>) values.get(1);
		if (tupleList.get(0).equals(SystemParameters.ThetaJoinerDataMigrationEOF))
			return _targetTasks;
		// else uniformly choose an element from the tasks (Shuffling grouping)
		return Arrays.asList(_targetTasks.get(rnd.nextInt(_targetTasks.size())));
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		_targetTasks = targetTasks;
	}

	/*
	 * Uncomment for storm version 0.7
	 * **********************************************
	 * @Override public void prepare(Fields outFields, int numTasks) { _numTasks
	 * = numTasks;//number of reshufflers }
	 * @Override public List<Integer> taskIndices(List<Object> values) {
	 * ////////////////////// String tupleString = (String) values.get(1);
	 * if(tupleString.equals(SystemParameters.ThetaJoinerDataMigrationEOF)){
	 * List<Integer> result = new ArrayList<Integer>(); for(int i=0; i<
	 * _numTasks; i++){ result.add(i); } return result; } //////////////////
	 * uniformly choose an element from the tasks (Shuffling grouping)
	 * ArrayList<Integer> tasks= new ArrayList<Integer>(1);
	 * tasks.add(rnd.nextInt(_numTasks)); return tasks; }
	 * *********************************************
	 */

}
