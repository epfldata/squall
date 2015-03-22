package ch.epfl.data.plan_runner.utilities;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/*
 * If the list of all possible hash values isspecified,
 *   it uses uniform key (not number of tuples!) distribution
 * Otherwise, we encode fieldGrouping exactly the same as the Storm authors.
 * Because of NoACK possibility, have to be used everywhere in the code.
 */
public class HashStreamGrouping implements CustomStreamGrouping {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// the number of tasks on the level this stream grouping is sending to
	private int _numTargetTasks;
	private List<Integer> _targetTasks;

	private final List<String> _fullHashList;

	private final Map _map;

	/*
	 * fullHashList is null if grouping is not balanced
	 */
	public HashStreamGrouping(Map map, List<String> fullHashList) {
		_map = map;
		_fullHashList = fullHashList;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
		final List<String> tuple = (List<String>) stormTuple.get(1);
		final String tupleHash = (String) stormTuple.get(2);
		if (MyUtilities.isFinalAck(tuple, _map))
			// send to everyone
			return _targetTasks;
		if (!isBalanced())
			return Arrays.asList(_targetTasks.get(MyUtilities
					.chooseHashTargetIndex(tupleHash, _numTargetTasks)));
		else
			return Arrays.asList(_targetTasks.get(MyUtilities
					.chooseBalancedTargetIndex(tupleHash, _fullHashList,
							_numTargetTasks)));
	}

	private boolean isBalanced() {
		return (_fullHashList != null);
	}

	@Override
	public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi,
			List<Integer> targetTasks) {
		_targetTasks = targetTasks;
		_numTargetTasks = targetTasks.size();
	}

}