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


package ch.epfl.data.squall.storm_components;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storage.BPlusTreeStorage;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.PeriodicAggBatchSend;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.statistics.StatisticsUtilities;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class StormDstTupleStorageBDB extends StormJoinerBoltComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDstTupleStorageBDB.class);
	private BPlusTreeStorage _firstRelationStorage, _secondRelationStorage;
	// for load-balancing
	private final List<String> _fullHashList;

	public StormDstTupleStorageBDB(StormEmitter firstEmitter,
			StormEmitter secondEmitter, ComponentProperties cp,
			List<String> allCompNames, Predicate joinPredicate,
			int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {
		super(firstEmitter, secondEmitter, cp, allCompNames, joinPredicate,
				hierarchyPosition, builder, killer, conf);
		System.out.println("INITIAL TIME IS :"
				+ WindowSemanticsManager.INITIAL_TUMBLING_TIMESTAMP);
		_fullHashList = cp.getFullHashList();
		_statsUtils = new StatisticsUtilities(getConf(), LOG);
		final int parallelism = SystemParameters.getInt(getConf(), getID()
				+ "_PAR");
		// connecting with previous level
		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
		if (MyUtilities.isManualBatchingMode(getConf()))
			currentBolt = MyUtilities.attachEmitterBatch(conf, _fullHashList,
					currentBolt, firstEmitter, secondEmitter);
		else
			currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList,
					currentBolt, firstEmitter, secondEmitter);
		// connecting with Killer
		if (getHierarchyPosition() == FINAL_COMPONENT
				&& (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, parallelism);

		if (cp.getPrintOut() && _operatorChain.isBlocking())
			currentBolt.allGrouping(killer.getID(),
					SystemParameters.DUMP_RESULTS_STREAM);

		if (_joinPredicate != null) {
			createIndexes();
			_existIndexes = true;
		} else
			_existIndexes = false;
	}

	// from IRichBolt
	@Override
	public void cleanup() {
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
			System.out.println("DUMPING !!!!");
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

	// BaseRichSpout
	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		createStorage(_firstRelationStorage, _secondRelationStorage, LOG);
		super.prepare(map, tc, collector);
	}

	@Override
	protected void printStatistics(int type) {
		printStatistics(type, _firstRelationStorage.size(),
				_secondRelationStorage.size(), LOG);
		LOG.info("First Storage: " + _firstRelationStorage.getStatistics()
				+ "End of First Storage\n");
		LOG.info("Second Storage: " + _secondRelationStorage.getStatistics()
				+ "End of Second Storage\n");
	}

	@Override
	public void purgeStaleStateFromWindow() {
		// TODO WINDOW Semantics
		long first = _firstRelationStorage.size();
		_firstRelationStorage.purgeState(_latestTimeStamp
				- WindowSemanticsManager._GC_PERIODIC_TICK);
		long firstafter = _firstRelationStorage.size();
		_secondRelationStorage.purgeState(_latestTimeStamp
				- WindowSemanticsManager._GC_PERIODIC_TICK);
		LOG.info("Calling purge state t first size was: " + first
				+ " then it is " + firstafter);
		System.gc();
	}
}