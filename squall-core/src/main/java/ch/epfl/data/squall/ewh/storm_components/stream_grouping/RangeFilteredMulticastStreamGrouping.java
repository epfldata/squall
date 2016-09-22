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

package ch.epfl.data.squall.ewh.storm_components.stream_grouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.SystemParameters.HistogramType;

// This class is used only for connecting D2Combiner to S1Reservoir,
//   when D2Combiner computes only d2_equi and has some duplicates of it over the boundaries (that's why filtering) due to a equi-depth histogram on R2
//   and S1Reservoir computes d2 using another equi-depth histogram on R1
public class RangeFilteredMulticastStreamGrouping extends
	RangeMulticastStreamGrouping {
    private static Logger LOG = Logger
	    .getLogger(RangeFilteredMulticastStreamGrouping.class);
    private List _srcRangeBoundaries; // (numTargetTasks - 1) of them

    private String _parentCompName;
    private Map<Integer, Integer> _parentTaskIdtoIndex = new HashMap<Integer, Integer>(); // Id
											  // is
											  // Storm-Dependent,
											  // Index
											  // =
											  // [0,
											  // parallelism)

    // with multicast
    public RangeFilteredMulticastStreamGrouping(Map map,
	    ComparisonPredicate comparison, NumericType wrapper,
	    HistogramType dstHistType, HistogramType srcHistType,
	    String parentCompName) {
	super(map, comparison, wrapper, dstHistType);
	_parentCompName = parentCompName;
	// LOG.info("I should be  EWH_SAMPLE_D2_COMBINER = " + _parentCompName);
	int numLastJoiners = SystemParameters.getInt(_map, "PAR_LAST_JOINERS");
	_srcRangeBoundaries = createBoundariesFromHistogram(
		srcHistType.filePrefix(), numLastJoiners);
    }

    @Override
    public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi,
	    List<Integer> targetTasks) {
	List<Integer> parentTaskIds = wtc.getComponentTasks(_parentCompName);
	for (int i = 0; i < parentTaskIds.size(); i++) {
	    // creating inverted index
	    // this should be the same order as in R2.targetTasks when sending
	    // to D2Combiner; this method is used in theta-joins (particularly
	    // dynamic)
	    int parentTaskId = parentTaskIds.get(i);
	    _parentTaskIdtoIndex.put(parentTaskId, i);
	}

	super.prepare(wtc, gsi, targetTasks);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
	final List<String> tuple = (List<String>) stormTuple.get(1);
	final String tupleHash = (String) stormTuple.get(2);
	if (!MyUtilities.isFinalAck(tuple, _map)) {
	    // taskId is Storm-dependent sender Id
	    int taskIndex = _parentTaskIdtoIndex.get(taskId); // translation to
							      // [0,
							      // parallelism)
	    if (!isWithinBoundaries(taskIndex, tupleHash, _srcRangeBoundaries)) {
		// LOG.info("FILTERED TUPLE key = " + tupleHash +
		// " in taskIndex " + taskIndex + "!");
		// send it nowhere
		return new ArrayList<Integer>();
	    }
	}
	return super.chooseTasks(taskId, stormTuple);
    }

    private boolean isWithinBoundaries(int taskIndex, String strKey,
	    List boundaries) {
	Object key = _wrapper.fromString(strKey);
	int keyTaskIndex = chooseTaskIndex(key, boundaries);
	// LOG.info("For each nonFinalAckTuple: key = " + key +
	// ", keyTaskIndex = " + keyTaskIndex);
	return keyTaskIndex == taskIndex;
    }
}