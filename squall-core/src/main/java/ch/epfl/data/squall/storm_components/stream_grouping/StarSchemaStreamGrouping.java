/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.storm_components.stream_grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import ch.epfl.data.squall.utilities.MyUtilities;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class StarSchemaStreamGrouping implements CustomStreamGrouping {

    private List<Integer> _targetTasks;
    private int _numTargetTasks;
    private final Map _map;
    private final String _starEmitterIndex;
    private Random _rndGen = new Random();

    public StarSchemaStreamGrouping(Map map, String starEmitterIndex) {
        _map = map;
        _starEmitterIndex = starEmitterIndex;
    }

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> targetTasks) {
        _targetTasks = targetTasks;
        _numTargetTasks = targetTasks.size();
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
        String emitterIndex = (String) stormTuple.get(0);
        List<String> tuple = (List<String>) stormTuple.get(1);

        if (MyUtilities.isFinalAck(tuple, _map))
            // send to everyone
            return _targetTasks;

        if (emitterIndex.equals(_starEmitterIndex)) {
            return Arrays.asList(_targetTasks.get(_rndGen
                    .nextInt(_numTargetTasks)));
        } else {
            return _targetTasks;
        }

    }
}
