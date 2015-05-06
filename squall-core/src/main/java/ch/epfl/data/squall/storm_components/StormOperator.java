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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.components.signal_components.SignaledDataSourceComponent;
import ch.epfl.data.squall.components.signal_components.SynchronizedStormDataSource;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.PeriodicAggBatchSend;
import ch.epfl.data.squall.utilities.SystemParameters;

public class StormOperator extends StormBoltComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormOperator.class);

    private final ChainOperator _operatorChain;

    private int _numSentTuples = 0;

    // if this is set, we receive using direct stream grouping
    private final List<String> _fullHashList;

    // for agg batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicAggBatchSend _periodicAggBatch;
    private final long _aggBatchOutputMillis;

    public StormOperator(ArrayList<Component> parentEmitters,
	    ComponentProperties cp, List<String> allCompNames,
	    int hierarchyPosition, TopologyBuilder builder,
	    TopologyKiller killer, Config conf) {
	super(cp, allCompNames, hierarchyPosition, conf);

	_aggBatchOutputMillis = cp.getBatchOutputMillis();

	final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");

	// if(parallelism > 1 && distinct != null){
	// throw new RuntimeException(_componentName +
	// ": Distinct operator cannot be specified for multiThreaded bolts!");
	// }
	_operatorChain = cp.getChainOperator();

	InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);

	_fullHashList = cp.getFullHashList();

	for (StormEmitter parentEmitter : parentEmitters) {
		
		//This is specific only to the Synchronized operator
		// add the shuffling stream grouping
		if(parentEmitter instanceof SignaledDataSourceComponent){
			final String[] emitterIDs = parentEmitter.getEmitterIDs();
		    for (final String emitterID : emitterIDs)
			currentBolt.shuffleGrouping(emitterID, SynchronizedStormDataSource.SHUFFLE_GROUPING_STREAMID);
		}
		
	    if (MyUtilities.isManualBatchingMode(getConf()))
		currentBolt = MyUtilities.attachEmitterBatch(conf,
			_fullHashList, currentBolt, parentEmitter);
	    else
		currentBolt = MyUtilities.attachEmitterHash(conf,
			_fullHashList, currentBolt, parentEmitter);
	}

	if (getHierarchyPosition() == FINAL_COMPONENT
		&& (!MyUtilities.isAckEveryTuple(conf)))
	    killer.registerComponent(this, parallelism);

	if (cp.getPrintOut() && _operatorChain.isBlocking())
	    currentBolt.allGrouping(killer.getID(),
		    SystemParameters.DUMP_RESULTS_STREAM);
    }

    @Override
    public void aggBatchSend() {
	if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
	    if (_operatorChain != null) {
		final Operator lastOperator = _operatorChain.getLastOperator();
		if (lastOperator instanceof AggregateOperator) {
		    try {
			_semAgg.acquire();
		    } catch (final InterruptedException ex) {
		    }

		    // sending
		    final AggregateOperator agg = (AggregateOperator) lastOperator;
		    final List<String> tuples = agg.getContent();
		    for (final String tuple : tuples)
			tupleSend(MyUtilities.stringToTuple(tuple, getConf()),
				null, 0);

		    // clearing
		    agg.clearStorage();

		    _semAgg.release();
		}
	    }
    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv,
	    List<String> tuple, boolean isLastInBatch) {
	long timestamp = 0;
	if (MyUtilities.isCustomTimestampMode(getConf())
		|| MyUtilities.isWindowTimestampMode(getConf()))
	    timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
	if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
	    try {
		_semAgg.acquire();
	    } catch (final InterruptedException ex) {
	    }
	tuple = _operatorChain.process(tuple, timestamp);
	if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
	    _semAgg.release();

	if (tuple == null) {
	    getCollector().ack(stormTupleRcv);
	    return;
	}
	_numSentTuples++;
	printTuple(tuple);

	if (MyUtilities
		.isSending(getHierarchyPosition(), _aggBatchOutputMillis)
		|| MyUtilities.isWindowTimestampMode(getConf())) {
	    tupleSend(tuple, stormTupleRcv, timestamp);
	}
	if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
	    if (MyUtilities.isManualBatchingMode(getConf())) {
		if (isLastInBatch) {
		    timestamp = stormTupleRcv
			    .getLongByField(StormComponent.TIMESTAMP); // getLong(2);
		    printTupleLatency(_numSentTuples - 1, timestamp);
		}
	    } else {
		timestamp = stormTupleRcv
			.getLongByField(StormComponent.TIMESTAMP); // getLong(3);
		printTupleLatency(_numSentTuples - 1, timestamp);
	    }
	}
    }

    // from IRichBolt
    @Override
    public void execute(Tuple stormTupleRcv) {
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
	    final List<String> tuple = (List<String>) stormTupleRcv
		    .getValueByField(StormComponent.TUPLE);// getValue(1);

	    if (processFinalAck(tuple, stormTupleRcv))
		return;

	    applyOperatorsAndSend(stormTupleRcv, tuple, true);

	} else {
		System.out.println(stormTupleRcv.getSourceStreamId()); 
	    final String inputBatch = stormTupleRcv
		    .getStringByField(StormComponent.TUPLE); // getString(1);

	    final String[] wholeTuples = inputBatch
		    .split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
	    final int batchSize = wholeTuples.length;
	    for (int i = 0; i < batchSize; i++) {
		// parsing
		final String currentTuple = new String(wholeTuples[i]);
		final String[] parts = currentTuple
			.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);

		String inputTupleString = null;
		if (parts.length == 1)
		    // lastAck
		    inputTupleString = new String(parts[0]);
		else
		    inputTupleString = new String(parts[1]);
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
		    applyOperatorsAndSend(stormTupleRcv, tuple, true);
		else
		    applyOperatorsAndSend(stormTupleRcv, tuple, false);
	    }
	}
	getCollector().ack(stormTupleRcv);
    }

    @Override
    public ChainOperator getChainOperator() {
	return _operatorChain;
    }

    // from StormComponent
    @Override
    public String getInfoID() {
	final String str = "OperatorComponent " + getID() + " has ID: "
		+ getID();
	return str;
    }

    @Override
    protected InterchangingComponent getInterComp() {
	// should never be invoked
	return null;
    }

    @Override
    public long getNumSentTuples() {
	return _numSentTuples;
    }

    @Override
    public PeriodicAggBatchSend getPeriodicAggBatch() {
	return _periodicAggBatch;
    }

    @Override
    protected void printStatistics(int type) {
	// TODO
    }

    @Override
    public void purgeStaleStateFromWindow() {
	throw new RuntimeException("Window semantics is not valid here.");
    }
}