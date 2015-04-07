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


package ch.epfl.data.squall.storm_components.stream_grouping;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class BatchStreamGrouping implements CustomStreamGrouping {

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
	public BatchStreamGrouping(Map map, List<String> fullHashList) {
		_map = map;
		_fullHashList = fullHashList;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
		final String tupleBatch = (String) stormTuple.get(1);
		if (MyUtilities.isFinalAckManualBatching(tupleBatch, _map))
			// send to everyone
			return _targetTasks;

		final int endIndex = tupleBatch
				.indexOf(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
		final String aHash = tupleBatch.substring(0, endIndex);

		if (!isBalanced())
			return Arrays.asList(_targetTasks.get(MyUtilities
					.chooseHashTargetIndex(aHash, _numTargetTasks)));
		else
			return Arrays.asList(_targetTasks.get(MyUtilities
					.chooseBalancedTargetIndex(aHash, _fullHashList,
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