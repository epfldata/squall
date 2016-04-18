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

package ch.epfl.data.squall.storm_components.hyper_cube;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.net.URL;
import java.io.File;
import java.net.URLClassLoader;
import java.net.MalformedURLException;


import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.dbtoaster.DBToasterEngine;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateStream;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.storm_components.StormBoltComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.storm_components.hash_hypercube.HashHyperCubeGrouping.EmitterDesc;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignmentBruteForce;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignmentBruteForce.ColumnDesc;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignerFactory;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ManualHybridHyperCubeAssignment.Dimension;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HybridHyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ManualHybridHyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HybridHyperCubeAssignmentBruteForce;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.PartitioningScheme;
import ch.epfl.data.squall.utilities.PeriodicAggBatchSend;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.statistics.StatisticsUtilities;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storage.TupleStorage;
import ch.epfl.data.squall.storm_components.StormJoinerBoltComponent;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class TradionalTwoWayJoin extends StormJoinerBoltComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(TradionalTwoWayJoin.class);
    private final TupleStorage _firstRelationStorage, _secondRelationStorage;

    private List<StormEmitter> _emitters;
    private Map<String, String[]> _emitterColNames;
    private Map<String, Type[]> _emitterNamesColTypes;
	private Set<String> _randomColumns;
    
    public TradionalTwoWayJoin(StormEmitter firstEmitter,
	    StormEmitter secondEmitter, Map<String, String[]> emitterColNames,
	    Map<String, Type[]> emitterNamesColTypes, Set<String> randomColumns,
	    ComponentProperties cp, List<String> allCompNames, Predicate joinPredicate,
	    boolean isPartitioner, int hierarchyPosition,
	    TopologyBuilder builder, TopologyKiller killer, Config conf,
	    boolean isContentSensitive, Type wrapper) {
	super(firstEmitter, secondEmitter, cp, allCompNames, joinPredicate,
		hierarchyPosition, builder, killer, isPartitioner, conf);
	_statsUtils = new StatisticsUtilities(getConf(), LOG);
	final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
	InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
	
	_emitterColNames = emitterColNames;
	_emitterNamesColTypes = emitterNamesColTypes;
	_randomColumns = randomColumns;
	
	_emitters = new ArrayList<StormEmitter>();
	_emitters.add(firstEmitter);
	_emitters.add(secondEmitter);

    currentBolt = attachEmitters(conf, currentBolt, allCompNames, parallelism);

	if (getHierarchyPosition() == FINAL_COMPONENT
		&& (!MyUtilities.isAckEveryTuple(conf)))
	    killer.registerComponent(this, parallelism);
	if (cp.getPrintOut() && _operatorChain.isBlocking())
	    currentBolt.allGrouping(killer.getID(),
		    SystemParameters.DUMP_RESULTS_STREAM);
	_firstRelationStorage = new TupleStorage();
	_secondRelationStorage = new TupleStorage();
	if (_joinPredicate != null) {
	    createIndexes();
	    _existIndexes = true;
	} else
	    _existIndexes = false;
    }

    private InputDeclarer attachEmitters(Config conf, InputDeclarer currentBolt,
                 List<String> allCompNames, int parallelism) {

        switch (getPartitioningScheme(conf)) {
            case BRUTEFORCEHYBRIDHYPERCUBE:
                long[] cardinality = getEmittersCardinality(_emitters, conf);
                List<ColumnDesc> columns = getColumnDesc(cardinality, _emitters);

                List<EmitterDesc> emittersDesc = MyUtilities.getEmitterDesc(
                        _emitters, _emitterColNames, allCompNames, cardinality);

                LOG.info("cardinalities: " + Arrays.toString(cardinality));

                HybridHyperCubeAssignment _currentHybridHyperCubeMappingAssignment = 
                    new HybridHyperCubeAssignmentBruteForce(emittersDesc, columns, _randomColumns, parallelism);
                
                LOG.info("assignment: " + _currentHybridHyperCubeMappingAssignment.getMappingDimensions());

                currentBolt = MyUtilities.attachEmitterHybridHyperCube(currentBolt, 
                        _emitters, _emitterColNames, allCompNames,
                        _currentHybridHyperCubeMappingAssignment, emittersDesc, conf);   
                break;
            case HASHHYPERCUBE:
                cardinality = getEmittersCardinality(_emitters, conf);
                LOG.info("cardinalities: " + Arrays.toString(cardinality));
                
                columns = getColumnDesc(cardinality, _emitters);
                emittersDesc = MyUtilities.getEmitterDesc(
                        _emitters, _emitterColNames, allCompNames, cardinality);

                HashHyperCubeAssignment _currentHashHyperCubeMappingAssignment = 
                    new HashHyperCubeAssignmentBruteForce(parallelism, columns, emittersDesc);
                
                LOG.info("assignment: " + _currentHashHyperCubeMappingAssignment.getMappingDimensions());

                currentBolt = MyUtilities.attachEmitterHashHyperCube(currentBolt, 
                        _emitters, _emitterColNames, _currentHashHyperCubeMappingAssignment, 
                        emittersDesc, conf);
                break;            
            case HYPERCUBE:
                cardinality = getEmittersCardinality(_emitters, conf);
                LOG.info("cardinalities: " + Arrays.toString(cardinality));
                final HyperCubeAssignment _currentHyperCubeMappingAssignment =
                        new HyperCubeAssignerFactory().getAssigner(parallelism, cardinality);

                LOG.info("assignment: " + _currentHyperCubeMappingAssignment.getMappingDimensions());
                currentBolt = MyUtilities.attachEmitterHyperCube(currentBolt,
                        _emitters, allCompNames,
                        _currentHyperCubeMappingAssignment, conf);
                break;
        }
        return currentBolt;
    }

    private List<ColumnDesc> getColumnDesc(long[] cardinality, List<StormEmitter> emitters) {
        HashMap<String, ColumnDesc> tmp = new HashMap<String, ColumnDesc>();

        List<ColumnDesc> desc = new ArrayList<ColumnDesc>();
        
        for (int i = 0; i < emitters.size(); i++) {
            String emitterName = emitters.get(i).getName();
            Type[] columnTypes = _emitterNamesColTypes.get(emitterName);
            String[] columnNames = _emitterColNames.get(emitterName);

            for (int j = 0; j < columnNames.length; j++) {
                ColumnDesc cd = new ColumnDesc(columnNames[j], columnTypes[j], cardinality[i]);

                if (tmp.containsKey(cd.name)) {
                    cd.size += tmp.get(cd.name).size;
                    tmp.put(cd.name, cd);
                } else {
                    tmp.put(cd.name, cd);
                }
            }
        }

        for (String key : tmp.keySet()) {
            desc.add(tmp.get(key));
        }

        return desc;
    }

    private PartitioningScheme getPartitioningScheme(Config conf) {
        String schemeName = SystemParameters.getString(conf, getName() + "_PART_SCHEME");
        if (schemeName == null || schemeName.equals("")) {
            LOG.info("use default Hypercube partitioning scheme");
            return PartitioningScheme.HYPERCUBE;
        } else {
            LOG.info("use partitioning scheme : " + schemeName);
            return PartitioningScheme.valueOf(schemeName);
        }
    }

    private long[] getEmittersCardinality(List<StormEmitter> emitters, Config conf) {
        long[] cardinality = new long[emitters.size()];
        for (int i = 0; i < emitters.size(); i++) {
            cardinality[i] = SystemParameters.getInt(conf, emitters.get(i).getName() + "_CARD");
        }
        return cardinality;
    }


    @Override
    public void execute(Tuple stormTupleRcv) {
	// TODO
	// short circuit that this is a window configuration
	if (WindowSemanticsManager.evictStateIfSlidingWindowSemantics(this,
		stormTupleRcv)) {
	    return;
	}

	if (_firstTime
		&& MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)) {
	    _periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis,
		    this);
	    _firstTime = false;
	}

	if (receivedDumpSignal(stormTupleRcv)) {
	    MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
	    return;
	}

	if (!MyUtilities.isManualBatchingMode(getConf())) {
	    final String inputComponentIndex = stormTupleRcv
		    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
	    final List<String> tuple = (List<String>) stormTupleRcv
		    .getValueByField(StormComponent.TUPLE); // getValue(1);
	    final String inputTupleHash = stormTupleRcv
		    .getStringByField(StormComponent.HASH);// getString(2);
	    if (processFinalAck(tuple, stormTupleRcv))
		return;
	    final String inputTupleString = MyUtilities.tupleToString(tuple,
		    getConf());
	    processNonLastTuple(inputComponentIndex, tuple, inputTupleHash,
		    stormTupleRcv, true, _firstRelationStorage,
		    _secondRelationStorage);
	} else {
	    final String inputComponentIndex = stormTupleRcv
		    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
	    final String inputBatch = stormTupleRcv
		    .getStringByField(StormComponent.TUPLE);// getString(1);
	    final String[] wholeTuples = inputBatch
		    .split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
	    final int batchSize = wholeTuples.length;
	    for (int i = 0; i < batchSize; i++) {
		// parsing
		final String currentTuple = new String(wholeTuples[i]);
		final String[] parts = currentTuple
			.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
		String inputTupleHash = null;
		String inputTupleString = null;
		if (parts.length == 1)
		    // lastAck
		    inputTupleString = new String(parts[0]);
		else {
		    inputTupleHash = new String(parts[0]);
		    inputTupleString = new String(parts[1]);
		}
		final List<String> tuple = MyUtilities.stringToTuple(
			inputTupleString, getConf());
		// final Ack check
		if (processFinalAck(tuple, stormTupleRcv)) {
		    if (i != batchSize - 1)
			throw new RuntimeException(
				"Should not be here. LAST_ACK is not the last tuple!");
		    return;
		}
		// processing a tuple
		if (i == batchSize - 1)
		    processNonLastTuple(inputComponentIndex, tuple,
			    inputTupleHash, stormTupleRcv, true,
			    _firstRelationStorage, _secondRelationStorage);
		else
		    processNonLastTuple(inputComponentIndex, tuple,
			    inputTupleHash, stormTupleRcv, false,
			    _firstRelationStorage, _secondRelationStorage);
	    }
	}
	// TODO
	// Update LatestTimeStamp
	WindowSemanticsManager.updateLatestTimeStamp(this, stormTupleRcv);
	getCollector().ack(stormTupleRcv);
    }

    @Override
    protected void printStatistics(int type) {
	printStatistics(type, _firstRelationStorage.size(),
		_secondRelationStorage.size(), LOG);
    }

    // TODO WINDOW Semantics
    @Override
    public void purgeStaleStateFromWindow() {
	_firstRelationStorage.purgeState(_latestTimeStamp
		- WindowSemanticsManager._GC_PERIODIC_TICK,
		_firstRelationIndexes, _joinPredicate, getConf(), true);
	_secondRelationStorage.purgeState(_latestTimeStamp
		- WindowSemanticsManager._GC_PERIODIC_TICK,
		_secondRelationIndexes, _joinPredicate, getConf(), false);
	System.gc();
    }
}
