/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.storm_components.theta.stream_grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import ch.epfl.data.squall.thetajoin.matrix_assignment.MatrixAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.MatrixAssignment.Dimension;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;

public class ContentSensitiveThetaJoinGrouping<KeyType> implements
	CustomStreamGrouping {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger
	    .getLogger(ContentSensitiveThetaJoinGrouping.class);

    private final MatrixAssignment _assignment;
    private final String _firstEmitterIndex, _secondEmitterIndex;
    private List<Integer> _targetTasks;
    private final Map _map;
    private final Type<KeyType> _wrapper;

    public ContentSensitiveThetaJoinGrouping(String firstIndex,
	    String secondIndex, MatrixAssignment assignment, Map map,
	    Type<KeyType> wrapper) {
	_assignment = assignment;
	_firstEmitterIndex = firstIndex;
	_secondEmitterIndex = secondIndex;
	_map = map;
	_wrapper = wrapper;
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
	List<Integer> tasks = null;
	final String tableName = (String) stormTuple.get(0);

	KeyType tupleKey = _wrapper.fromString((String) stormTuple.get(2)); // hash

	if (tableName.equals(_firstEmitterIndex))
	    tasks = translateIdsToTasks(_assignment.getRegionIDs(Dimension.ROW,
		    tupleKey));
	else if (tableName.equals(_secondEmitterIndex))
	    tasks = translateIdsToTasks(_assignment.getRegionIDs(
		    Dimension.COLUMN, tupleKey));
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