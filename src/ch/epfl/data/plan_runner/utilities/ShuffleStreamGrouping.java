package ch.epfl.data.plan_runner.utilities;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

/*
 * If the list of all possible hash values isspecified,
 *   it uses uniform key (not number of tuples!) distribution
 * Otherwise, we encode fieldGrouping exactly the same as the Storm authors.
 * Because of NoACK possibility, have to be used everywhere in the code.
 */
public class ShuffleStreamGrouping implements CustomStreamGrouping {
    private static final long serialVersionUID = 1L;
    // the number of tasks on the level this stream grouping is sending to
    private int _numTargetTasks;
    private List<Integer> _targetTasks;

    private final Map _map;

    private Random _rndGen = new Random();

    /*
     * fullHashList is null if grouping is not balanced
     */
    public ShuffleStreamGrouping(Map map) {
	_map = map;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
	final List<String> tuple = (List<String>) stormTuple.get(1);
	if (MyUtilities.isFinalAck(tuple, _map)) {
	    // send to everyone
	    return _targetTasks;
	} else
	    return Arrays.asList(_targetTasks.get(_rndGen
		    .nextInt(_numTargetTasks)));
    }

    @Override
    public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi,
	    List<Integer> targetTasks) {
	_targetTasks = targetTasks;
	_numTargetTasks = targetTasks.size();
    }

}