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
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public abstract class StormSpoutComponent extends BaseRichSpout implements
	StormComponent, StormEmitter {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormSpoutComponent.class);

    private final Map _conf;
    private SpoutOutputCollector _collector;
    private final String _ID;
    private final String _componentIndex; // a unique index in a list of all the
    // components
    // used as a shorter name, to save some network traffic
    // it's of type int, but we use String to save more space
    private final boolean _printOut;
    private final int _hierarchyPosition;

    private final List<Integer> _hashIndexes;
    private final List<ValueExpression> _hashExpressions;

    // for ManualBatch(Queuing) mode
    private List<Integer> _targetTaskIds;
    private int _targetParallelism;
    private StringBuilder[] _targetBuffers;
    private long[] _targetTimestamps;

    // for CustomTimestamp mode
    private double _totalLatencyMillis;
    private long _numberOfSamples;

    // counting negative values
    protected long numNegatives = 0;
    protected double maxNegative = 0;

    // EWH histogram
    private boolean _isPartitioner;

    public StormSpoutComponent(ComponentProperties cp,
	    List<String> allCompNames, int hierarchyPosition,
	    boolean isPartitioner, Map conf) {
	_conf = conf;
	_ID = cp.getName();
	_componentIndex = String.valueOf(allCompNames.indexOf(_ID));
	_printOut = cp.getPrintOut();
	_hierarchyPosition = hierarchyPosition;

	_hashIndexes = cp.getHashIndexes();
	_hashExpressions = cp.getHashExpressions();

	_isPartitioner = isPartitioner;
    }

    // ManualBatchMode
    private void addToManualBatch(List<String> tuple, long timestamp) {
	final String tupleHash = MyUtilities.createHashString(tuple,
		_hashIndexes, _hashExpressions, _conf);
	final int dstIndex = MyUtilities.chooseHashTargetIndex(tupleHash,
		_targetParallelism);

	// we put in queueTuple based on tupleHash
	// the same hash is used in BatchStreamGrouping for deciding where a
	// particular targetBuffer is to be sent
	final String tupleString = MyUtilities.tupleToString(tuple, _conf);

	if (MyUtilities.isCustomTimestampMode(_conf))
	    if (_targetBuffers[dstIndex].length() == 0)
		// timestamp of the first tuple being added to a buffer is the
		// timestamp of the buffer
		_targetTimestamps[dstIndex] = timestamp;
	_targetBuffers[dstIndex].append(tupleHash)
		.append(SystemParameters.MANUAL_BATCH_HASH_DELIMITER)
		.append(tupleString)
		.append(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	if (MyUtilities.isAckEveryTuple(_conf)
		|| _hierarchyPosition == FINAL_COMPONENT)
	    declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
		    SystemParameters.EOF));

	final List<String> outputFields = new ArrayList<String>();
	if (MyUtilities.isManualBatchingMode(_conf)) {
	    outputFields.add(StormComponent.COMP_INDEX);
	    outputFields.add(StormComponent.TUPLE); // string
	} else {
	    outputFields.add(StormComponent.COMP_INDEX);
	    outputFields.add(StormComponent.TUPLE); // list of string
	    outputFields.add(StormComponent.HASH);
	}
	if (MyUtilities.isCustomTimestampMode(getConf())
		|| MyUtilities.isWindowTimestampMode(getConf()))
	    outputFields.add(StormComponent.TIMESTAMP);
	declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(
		outputFields));

	if (_isPartitioner) {
	    // EQUI-WEIGHT HISTOGRAM
	    final List<String> outputFieldsPart = new ArrayList<String>();
	    outputFieldsPart.add(StormComponent.COMP_INDEX);
	    outputFieldsPart.add(StormComponent.TUPLE); // list of string
	    outputFieldsPart.add(StormComponent.HASH);
	    declarer.declareStream(SystemParameters.PARTITIONER, new Fields(
		    outputFieldsPart));
	}
    }

    private void finalAckSend() {
	final Values finalAck = MyUtilities.createUniversalFinalAckTuple(_conf);
	_collector.emit(finalAck);
	if (_isPartitioner) {
	    // rel size
	    Values relSize = MyUtilities.createRelSizeTuple(_componentIndex,
		    (int) getNumSentTuples());
	    _collector.emit(SystemParameters.PARTITIONER, relSize);

	    // final ack
	    _collector.emit(SystemParameters.PARTITIONER, finalAck);
	}
    }

    public abstract ChainOperator getChainOperator();

    protected SpoutOutputCollector getCollector() {
	return _collector;
    }

    protected Map getConf() {
	return _conf;
    }

    // StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
	return new String[] { _ID };
    }

    protected int getHierarchyPosition() {
	return _hierarchyPosition;
    }

    // StormComponent
    @Override
    public String getID() {
	return _ID;
    }

    @Override
    public String getName() {
	return _ID;
    }

    public abstract long getNumSentTuples();

    private void manualBatchSend() {
	for (int i = 0; i < _targetParallelism; i++) {
	    final String tupleString = _targetBuffers[i].toString();
	    _targetBuffers[i] = new StringBuilder("");

	    if (!tupleString.isEmpty())
		// some buffers might be empty
		if (MyUtilities.isCustomTimestampMode(_conf))
		    _collector.emit(new Values(_componentIndex, tupleString,
			    _targetTimestamps[i]));
		else
		    _collector.emit(new Values(_componentIndex, tupleString));
	}
    }

    // BaseRichSpout
    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector collector) {
	_collector = collector;

	_targetTaskIds = MyUtilities.findTargetTaskIds(tc);
	_targetParallelism = _targetTaskIds.size();
	_targetBuffers = new StringBuilder[_targetParallelism];
	_targetTimestamps = new long[_targetParallelism];
	for (int i = 0; i < _targetParallelism; i++)
	    _targetBuffers[i] = new StringBuilder("");

	// equi-weight histogram
	if (_isPartitioner) {
	    // extract sampleAside operator
	    SampleAsideAndForwardOperator saf = getChainOperator()
		    .getSampleAside();
	    saf.setCollector(_collector);
	    saf.setComponentIndex(_componentIndex);
	}
    }

    @Override
    public void printContent() {
	if (_printOut)
	    if ((getChainOperator() != null) && getChainOperator().isBlocking()) {
		final Operator lastOperator = getChainOperator()
			.getLastOperator();
                MyUtilities.printBlockingResult(_ID,
                                                lastOperator,
                                                _hierarchyPosition, _conf, LOG);

	    }
    }

    @Override
    public void printTuple(List<String> tuple) {
	if (_printOut)
	    if ((getChainOperator() == null)
		    || !getChainOperator().isBlocking()) {
		final StringBuilder sb = new StringBuilder();
		sb.append("\nComponent ").append(_ID);
		sb.append("\nReceived tuples: ").append(getNumSentTuples());
		sb.append(" Tuple: ").append(
			MyUtilities.tupleToString(tuple, _conf));
		LOG.info(sb.toString());
	    }
    }

    // StormComponent
    // tupleSerialNum starts from 0
    @Override
    public void printTupleLatency(long tupleSerialNum, long timestamp) {
	final int freqCompute = SystemParameters.getInt(_conf,
		"FREQ_TUPLE_LOG_COMPUTE");
	final int freqWrite = SystemParameters.getInt(_conf,
		"FREQ_TUPLE_LOG_WRITE");
	final int startupIgnoredTuples = SystemParameters.getInt(_conf,
		"INIT_IGNORED_TUPLES");

	if (tupleSerialNum >= startupIgnoredTuples) {
	    tupleSerialNum = tupleSerialNum - startupIgnoredTuples; // start
	    // counting
	    // from zero
	    // when
	    // computing
	    // starts
	    long latencyMillis = -1;
	    if (tupleSerialNum % freqCompute == 0) {
		latencyMillis = System.currentTimeMillis() - timestamp;
		if (latencyMillis < 0) {
		    numNegatives++;
		    if (latencyMillis < maxNegative)
			maxNegative = latencyMillis;
		    // for every negative tuple we set 0
		    latencyMillis = 0;

		    /*
		     * LOG.info("Exception! Current latency is " + latency +
		     * "ms! Ignoring a tuple!"); return;
		     */
		}
		if (_numberOfSamples < 0) {
		    LOG.info("Exception! Number of samples is "
			    + _numberOfSamples + "! Ignoring a tuple!");
		    return;
		}
		_totalLatencyMillis += latencyMillis;
		_numberOfSamples++;
	    }
	    if (tupleSerialNum % freqWrite == 0) {
		LOG.info("Taking into account every " + freqCompute
			+ "th tuple, and printing every " + freqWrite
			+ "th one.");
		LOG.info("LAST tuple latency is " + latencyMillis + "ms.");
		LOG.info("AVERAGE tuple latency so far is "
			+ _totalLatencyMillis / _numberOfSamples + "ms.");
	    }
	}
    }

    // HELPER METHODS
    // non-ManualBatchMode
    private void regularTupleSend(List<String> tuple, long timestamp) {
	final Values stormTupleSnd = MyUtilities.createTupleValues(tuple,
		timestamp, _componentIndex, _hashIndexes, _hashExpressions,
		_conf);
	MyUtilities.sendTuple(stormTupleSnd, _collector, _conf);
    }

    @Override
    public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
	    long timestamp) {
	final boolean isLastAck = MyUtilities.isFinalAck(tuple, _conf);

	if (!MyUtilities.isManualBatchingMode(_conf)) {
	    if (isLastAck)
		finalAckSend();
	    else
		regularTupleSend(tuple, timestamp);
	} else if (!isLastAck) {
	    // appending tuple if it is not lastAck
	    addToManualBatch(tuple, timestamp);
	    if (getNumSentTuples() % MyUtilities.getCompBatchSize(_ID, _conf) == 0)
		manualBatchSend();
	} else {
	    // has to be sent separately, because of the BatchStreamGrouping
	    // logic
	    manualBatchSend(); // we need to send the last batch, if it is
	    // not empty
	    finalAckSend();
	}
    }
}
