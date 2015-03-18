package ch.epfl.data.plan_runner.utilities.thetajoin_static;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.MatrixAssignment;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.MatrixAssignment.Dimension;
import ch.epfl.data.plan_runner.utilities.MyUtilities;

public class ThetaJoinStaticMapping implements CustomStreamGrouping {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(ThetaJoinStaticMapping.class);

	private final MatrixAssignment _assignment;
	private final String _firstEmitterIndex, _secondEmitterIndex;
	private List<Integer> _targetTasks;
	private final Map _map;

	public ThetaJoinStaticMapping(String firstIndex, String secondIndex,
			MatrixAssignment assignment, Map map) {
		_assignment = assignment;
		_firstEmitterIndex = firstIndex;
		_secondEmitterIndex = secondIndex;
		_map = map;
	}

	// @Override
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
		// the following is checking for FinalAck
		if (!MyUtilities.isManualBatchingMode(_map)) {
			final List<String> tuple = (List<String>) stormTuple.get(1); // TUPLE
			if (MyUtilities.isFinalAck(tuple, _map))
				return _targetTasks;
		} else {
			final String tupleBatch = (String) stormTuple.get(1); // TUPLE
			if (MyUtilities.isFinalAckManualBatching(tupleBatch, _map))
				// send to everyone
				return _targetTasks;
		}

		// the following picks tasks for Non-FinalAck
		return chooseTasksNonFinalAck(stormTuple);
	}

	private List<Integer> chooseTasksNonFinalAck(List<Object> stormTuple) {
		// ////////////////
		List<Integer> tasks = null;
		final String tableName = (String) stormTuple.get(0);
		if (tableName.equals(_firstEmitterIndex))
			tasks = translateIdsToTasks(_assignment.getRegionIDs(Dimension.ROW));
		else if (tableName.equals(_secondEmitterIndex))
			tasks = translateIdsToTasks(_assignment
					.getRegionIDs(Dimension.COLUMN));
		else {
			LOG.info("First Name: " + _firstEmitterIndex);
			LOG.info("Second Name: " + _secondEmitterIndex);
			LOG.info("Table Name: " + tableName);
			LOG.info("WRONG ASSIGNMENT");
		}
		return tasks;
	}

	@Override
	public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi,
			List<Integer> targetTasks) {
		// LOG.info("Number of tasks is : "+numTasks);
		_targetTasks = targetTasks;
	}

	private List<Integer> translateIdsToTasks(ArrayList<Integer> ids) {
		final List<Integer> converted = new ArrayList<Integer>();
		for (final int id : ids)
			converted.add(_targetTasks.get(id));
		return converted;
	}

}