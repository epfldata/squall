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
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignment;
import ch.epfl.data.squall.utilities.MyUtilities;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by khayyam on 4/10/15.
 */
public class HyperCubeStaticGrouping implements CustomStreamGrouping {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(HyperCubeStaticGrouping.class);

    private final HyperCubeAssignment _assignment;
    private final String[] _emitterIndexes;
    private List<Integer> _targetTasks;
    private final Map _map;

    public HyperCubeStaticGrouping(String[] emitterIndexes,
                                   HyperCubeAssignment assignment, Map map) {
        _assignment = assignment;
        _emitterIndexes = emitterIndexes;
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
        for (int i = 0; i < _emitterIndexes.length; i++)
            if (tableName.equals(_emitterIndexes[i])) {
                List<Integer> regionsID = _assignment.getRegionIDs(HyperCubeAssignment.Dimension.d(i));
                tasks = translateIdsToTasks(regionsID);
                break;
            }


//        if (tasks == null) {
//            for (int i = 0; i < _emitterIndexes.length; i++)
//                LOG.info("First Name: " + _emitterIndexes[i]);
//
//            LOG.info("Table Name: " + tableName);
//            LOG.info("WRONG ASSIGNMENT");
//        }
        return tasks;
    }

    @Override
    public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi,
                        List<Integer> targetTasks) {
        // LOG.info("Number of tasks is : "+numTasks);
        _targetTasks = targetTasks;
    }

    private List<Integer> translateIdsToTasks(List<Integer> ids) {

//        for (int i = 0; i < _targetTasks.size(); i++)
//            System.out.println(_targetTasks.get(i));

        final List<Integer> converted = new ArrayList<Integer>();
        for (final int id : ids)
            converted.add(_targetTasks.get(id));
        return converted;
    }
}
