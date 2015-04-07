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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.thetajoin.dynamic.storm_matrix_mapping.ThetaJoinDynamicMapping;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.thetajoin.dynamic.BufferedTuple;

public class InterchangingBolt extends BaseRichBolt implements StormComponent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String _ID;
	// components
	private final String _firstEmitterIndex, _secondEmitterIndex;
	private final StormEmitter _firstEmitter, _secondEmitter;
	private final int _parallelism;
	private int _numParentTasks;
	private OutputCollector _collector;
	private final Config _conf;
	private final LinkedList<BufferedTuple> bufferedTuplesRel1;
	private final LinkedList<BufferedTuple> bufferedTuplesRel2;
	private final int _hierarchyPosition = INTERMEDIATE;
	private final int _multFactor;
	private transient InputDeclarer _currentBolt;
	private long _currentRelationPointer = 1; // begin with relation one
	private long _currentCount = 0;
	private long _currentAccCount = 1;
	private long _currentAccCountALL = 0;
	// private long _previousAccCount=0;
	private HashSet<Integer> _firstRelationTasks, _secondRelationTasks;
	private boolean _isFirstFinished = false, _isSecondFinished = false;

	private long _relation1Count = 0, _relation2Count = 0;

	private static Logger LOG = Logger.getLogger(InterchangingBolt.class);

	public InterchangingBolt(StormEmitter firstEmitter,
			StormEmitter secondEmitter, ComponentProperties cp,
			List<String> allCompNames, TopologyBuilder builder,
			TopologyKiller killer, Config conf, int multiplicativeFactor) {

		_firstEmitter = firstEmitter;
		_secondEmitter = secondEmitter;
		_ID = cp.getName();
		_conf = conf;
		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter
				.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames
				.indexOf(_secondEmitter.getName()));
		_parallelism = SystemParameters.getInt(conf, _ID + "_PAR");
		_currentBolt = builder.setBolt(_ID, this, _parallelism);

		final ThetaJoinDynamicMapping dMap = new ThetaJoinDynamicMapping(conf,
				-1);

		// [0] because StormSrcJoin is considered dead
		_currentBolt = _currentBolt.customGrouping(
				firstEmitter.getEmitterIDs()[0], dMap);
		_currentBolt = _currentBolt.customGrouping(
				secondEmitter.getEmitterIDs()[0], dMap);

		bufferedTuplesRel1 = new LinkedList<BufferedTuple>();
		bufferedTuplesRel2 = new LinkedList<BufferedTuple>();
		_multFactor = multiplicativeFactor;

	}

	@Override
	public void aggBatchSend() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("CompIndex", "Tuple", "Hash"));
	}

	@Override
	public void execute(Tuple stormTupleRcv) {
		final String inputComponentIndex = stormTupleRcv.getString(0); // Table
																		// name
		String inputTupleString = "";
		List<String> tupleList = null;
		tupleList = (List<String>) stormTupleRcv.getValue(1);
		inputTupleString = MyUtilities.tupleToString(tupleList, _conf);

		if (MyUtilities.isFinalAck(tupleList, _conf)) {
			final int id = stormTupleRcv.getSourceTask();
			final boolean first = _firstRelationTasks.remove(id);
			final boolean second = _secondRelationTasks.remove(id);
			if (_firstRelationTasks.size() == 0 && first) {
				_isFirstFinished = true;
				_currentRelationPointer = 2;
				// remove all the buffered tuples
				flush(bufferedTuplesRel1);
			}
			if (_secondRelationTasks.size() == 0 && second) {
				_isSecondFinished = true;
				_currentRelationPointer = 1;
				// remove all the buffered tuples
				flush(bufferedTuplesRel2);
			}
			_numParentTasks--;
			if (_numParentTasks == 0) {
				flush(bufferedTuplesRel1);
				flush(bufferedTuplesRel2);
				MyUtilities.processFinalAck(_numParentTasks,
						_hierarchyPosition, _conf, stormTupleRcv, _collector,
						null);
			}
			_collector.ack(stormTupleRcv);
			return;
		}
		final String inputTupleHash = stormTupleRcv.getString(2); // Hash Tuple
		// / now processing
		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			if (_currentRelationPointer == 1) { // it is the right tuple
				// send right away
				_collector
						.emit(new Values(inputComponentIndex, MyUtilities
								.stringToTuple(inputTupleString, _conf),
								inputTupleHash));
				_currentCount++;
				_relation1Count++;
				// LOG.info("Emitting 1: ("+_relation1Count+","+_relation2Count+")");

				// now check if a switch should happen
				if ((_currentCount == _currentAccCount) && (!_isSecondFinished)) {
					_currentRelationPointer = 2;
					_currentCount = 0;
					_currentAccCountALL = _relation1Count * _multFactor;
					_currentAccCount = _currentAccCountALL - _relation2Count;
				}
			} else { // it is the other tuple
				bufferedTuplesRel1.add(new BufferedTuple(inputComponentIndex,
						inputTupleString, inputTupleHash));
				// emit a buffered tuple from the second relation if exists
				if (!bufferedTuplesRel2.isEmpty()) {
					final BufferedTuple bufTup = bufferedTuplesRel2
							.removeFirst();
					_collector.emit(new Values(bufTup.get_componentName(),
							MyUtilities.stringToTuple(bufTup.get_tupleString(),
									_conf), bufTup.get_tupleHash()));
					_currentCount++;
					_relation2Count++;
					// LOG.info("Emitting 2: ("+_relation1Count+","+_relation2Count+")");
					// now check if a switch should happen
					if ((_currentCount == _currentAccCount)
							&& (!_isFirstFinished)) {
						_currentRelationPointer = 1;
						_currentCount = 0;
						_currentAccCountALL = _relation2Count * _multFactor;
						_currentAccCount = _currentAccCountALL
								- _relation1Count;
					}
				}
			}

		} else if (_secondEmitterIndex.equals(inputComponentIndex))
			if (_currentRelationPointer == 2) {
				// send right away
				_collector
						.emit(new Values(inputComponentIndex, MyUtilities
								.stringToTuple(inputTupleString, _conf),
								inputTupleHash));
				_currentCount++;
				_relation2Count++;
				// LOG.info("Emitting 2: ("+_relation1Count+","+_relation2Count+")");

				// now check if a switch should happen
				if ((_currentCount == _currentAccCount) && (!_isFirstFinished)) {
					_currentRelationPointer = 1;
					_currentCount = 0;
					_currentAccCountALL = _relation2Count * _multFactor;
					_currentAccCount = _currentAccCountALL - _relation1Count;
				}
			} else { // it is the other tuple
				bufferedTuplesRel2.add(new BufferedTuple(inputComponentIndex,
						inputTupleString, inputTupleHash));
				// emit a buffered tuple from the second relation if exists
				if (!bufferedTuplesRel1.isEmpty()) {
					final BufferedTuple bufTup = bufferedTuplesRel1
							.removeFirst();
					_collector.emit(new Values(bufTup.get_componentName(),
							MyUtilities.stringToTuple(bufTup.get_tupleString(),
									_conf), bufTup.get_tupleHash()));
					_currentCount++;
					_relation1Count++;
					// LOG.info("Emitting 1: ("+_relation1Count+","+_relation2Count+")");
					// now check if a switch should happen
					if ((_currentCount == _currentAccCount)
							&& (!_isSecondFinished)) {
						_currentRelationPointer = 2;
						_currentCount = 0;
						_currentAccCountALL = _relation1Count * _multFactor;
						_currentAccCount = _currentAccCountALL
								- _relation2Count;
					}
				}
			}

		_collector.ack(stormTupleRcv);
	}

	private void flush(LinkedList<BufferedTuple> buffer) {
		for (final BufferedTuple bufTup : buffer)
			_collector.emit(new Values(bufTup.get_componentName(), MyUtilities
					.stringToTuple(bufTup.get_tupleString(), _conf), bufTup
					.get_tupleHash()));
		buffer.clear();
	}

	@Override
	public String getID() {

		return null;
	}

	@Override
	public String getInfoID() {

		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_numParentTasks = MyUtilities.getNumParentTasks(context, _firstEmitter,
				_secondEmitter);
		_collector = collector;
		_firstRelationTasks = new HashSet<Integer>();
		_secondRelationTasks = new HashSet<Integer>();
		_firstRelationTasks.addAll(context.getComponentTasks(_firstEmitter
				.getEmitterIDs()[0]));
		_secondRelationTasks.addAll(context.getComponentTasks(_secondEmitter
				.getEmitterIDs()[0]));
	}

	@Override
	public void printContent() {
		// TODO Auto-generated method stub

	}

	@Override
	public void printTuple(List<String> tuple) {

	}

	@Override
	public void printTupleLatency(long numSentTuples, long timestamp) {
		// TODO Auto-generated method stub

	}

	@Override
	public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
			long timestamp) {
		// TODO Auto-generated method stub

	}

}
