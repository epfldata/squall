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

package ch.epfl.data.squall.thetajoin.adaptive.storm_matrix_mapping;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import ch.epfl.data.squall.utilities.MyUtilities;

public class ThetaJoinAdaptiveMapping implements CustomStreamGrouping {

    /**
     * This class is only responsible for the mapping for DATA_STREAM and
     * LAST_ACKS ... ONLY !!
     */
    private static final long serialVersionUID = 1L;

    private List<Integer> _targetTasks;
    private final Map _map;
    private Random rnd;

    public ThetaJoinAdaptiveMapping(Map map, int seed) {
	_map = map;
	if (seed >= 0)
	    rnd = new Random(seed);
	else
	    rnd = new Random();
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
	final List<String> tuple = (List<String>) values.get(1);
	if (MyUtilities.isFinalAck(tuple, _map))
	    return _targetTasks;

	// else uniformly choose an element from the tasks (Shuffling grouping)
	return Arrays
		.asList(_targetTasks.get(rnd.nextInt(_targetTasks.size())));
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
	    List<Integer> targetTasks) {
	_targetTasks = targetTasks;
    }

    /*
     * Uncomment for storm version 0.7
     * **********************************************
     * 
     * @Override public void prepare(Fields outFields, int numTasks) { _numTasks
     * = numTasks; }
     * 
     * @Override public List<Integer> taskIndices(List<Object> values) {
     * ////////////////////// String tupleString = (String) values.get(1);
     * if(MyUtilities.isFinalAck(tupleString, _map)){ List<Integer> result = new
     * ArrayList<Integer>(); for(int i=0; i< _numTasks; i++){ result.add(i); }
     * return result; } ////////////////// uniformly choose an element from the
     * tasks (Shuffling grouping) ArrayList<Integer> tasks= new
     * ArrayList<Integer>(1); tasks.add(rnd.nextInt(_numTasks)); return tasks; }
     * *********************************************
     */

}
