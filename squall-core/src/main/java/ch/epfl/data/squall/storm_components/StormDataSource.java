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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.CustomReader;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.PeriodicAggBatchSend;
import ch.epfl.data.squall.utilities.SerializableFileInputStream;
import ch.epfl.data.squall.utilities.SerializableHDFSFileInputStream;
import ch.epfl.data.squall.utilities.SystemParameters;

public class StormDataSource extends StormSpoutComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDataSource.class);

	private final String _inputPath;
	private int _fileSection;
	private final int _fileParts;
	private CustomReader _reader = null;

	private boolean _hasReachedEOF = false;
	private boolean _hasSentEOF = false; // have sent EOF to TopologyKiller
	// (AckEachTuple mode)
	private boolean _hasSentLastAck = false; // AckLastTuple mode

	private long _pendingTuples = 0;
	private int _numSentTuples = 0;

	private final ChainOperator _operatorChain;

	// for aggregate batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
	private final long _aggBatchOutputMillis;

	private boolean _isWindowMode;

	private String _name;

	public StormDataSource(ComponentProperties cp, List<String> allCompNames,
			String inputPath, int hierarchyPosition, int parallelism,
			boolean isPartitioner, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {

		super(cp, allCompNames, hierarchyPosition, isPartitioner, conf);
		_operatorChain = cp.getChainOperator();
		_name = cp.getName();
		_aggBatchOutputMillis = cp.getBatchOutputMillis();
		_inputPath = inputPath;
		_fileParts = parallelism;

		if (getHierarchyPosition() == FINAL_COMPONENT
				&& (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, parallelism);

		builder.setSpout(getID(), this, parallelism);
		if (MyUtilities.isAckEveryTuple(conf))
			killer.registerComponent(this, parallelism);
	}

	public void setWindowMode() {
		_isWindowMode = true;
	}

	// ack method on spout is called only if in AckEveryTuple mode (ACKERS > 0)
	@Override
	public void ack(Object msgId) {
		_pendingTuples--;
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

	protected void applyOperatorsAndSend(List<String> tuple) {
		long timestamp = 0;
		if (_isWindowMode
				|| (MyUtilities.isCustomTimestampMode(getConf()) && getHierarchyPosition() == StormComponent.NEXT_TO_LAST_COMPONENT)
				|| MyUtilities.isWindowTimestampMode(getConf()))
			timestamp = System.currentTimeMillis();
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}
		tuple = _operatorChain.process(tuple, timestamp);

		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			_semAgg.release();

		if (tuple == null)
			return;

		_numSentTuples++;
		_pendingTuples++;
		printTuple(tuple);

		if (MyUtilities
				.isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {
			tupleSend(tuple, null, timestamp);
		}
		if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
			printTupleLatency(_numSentTuples - 1, timestamp);
		}
	}

	@Override
	public void close() {
		try {
			_reader.close();
		} catch (final Exception e) {
			final String error = MyUtilities.getStackTrace(e);
			LOG.info(error);
		}
	}

	/*
	 * whatever is inside this method is done only once
	 */
	private void eofFinalization() {
		printContent();

		if (!MyUtilities.isAckEveryTuple(getConf()))
			if (getHierarchyPosition() == FINAL_COMPONENT) {
				if (!_hasSentEOF) {
					_hasSentEOF = true; // to ensure we will not send multiple
					// EOF per single spout
					getCollector().emit(SystemParameters.EOF_STREAM,
							new Values(SystemParameters.EOF));
				}
			} else if (!_hasSentLastAck) {
				LOG.info(getID() + ":Has sent last_ack, tuples sent:"
						+ _numSentTuples);
				_hasSentLastAck = true;
				final List<String> lastTuple = new ArrayList<String>(
						Arrays.asList(SystemParameters.LAST_ACK));
				tupleSend(lastTuple, null, 0);
			}
		if (_operatorChain != null) {
			_operatorChain.finalizeProcessing();
		}
	}

	@Override
	public void fail(Object msgId) {
		throw new RuntimeException("Failing tuple in " + getID());
	}

	@Override
	public ChainOperator getChainOperator() {
		return _operatorChain;
	}

	// StormComponent
	@Override
	public String getInfoID() {
		final StringBuilder sb = new StringBuilder();
		sb.append("Table ").append(getID()).append(" has ID: ").append(getID());
		return sb.toString();
	}

	@Override
	public long getNumSentTuples() {
		return _numSentTuples;
	}

	public long getPendingTuples() {
		return _pendingTuples;
	}

	// from IRichSpout interface
	@Override
	public void nextTuple() {
		if (_firstTime
				&& MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)) {
			_periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis,
					this);
			_firstTime = false;
		}

		if (SystemParameters.isExisting(getConf(), "TIMEOUT_1MS_EVERY_XTH")) {
			// Obsolete - this is for compatibility with old configurations
			final long timeout = 1;
			final int freqWait = SystemParameters.getInt(getConf(),
					"TIMEOUT_1MS_EVERY_XTH");
			if (_numSentTuples > 0 && _numSentTuples % freqWait == 0)
				Utils.sleep(timeout);
		}

		if (SystemParameters.isExisting(getConf(), "TIMEOUT_EVERY_X_TUPLE")
				&& SystemParameters.isExisting(getConf(), "TIMEOUT_X_MS")) {
			final int freqWait = SystemParameters.getInt(getConf(),
					"TIMEOUT_EVERY_X_TUPLE");
			final long timeout = SystemParameters.getInt(getConf(),
					"TIMEOUT_X_MS");
			if (_numSentTuples > 0 && _numSentTuples % freqWait == 0)
				Utils.sleep(timeout);
		}

		final String line = readLine();
		if (line == null) {
			if (!_hasReachedEOF) {
				_hasReachedEOF = true;
				// we reached EOF, first time this happens we invoke the method:
				eofFinalization();
			}
			sendEOF();
			// sleep since we are not going to do useful work,
			// but still are looping in nextTuple method
			Utils.sleep(SystemParameters.EOF_TIMEOUT_MILLIS);
			return;
		}

		final List<String> tuple = MyUtilities.fileLineToTuple(line, getConf());
		applyOperatorsAndSend(tuple);
	}

	// BaseRichSpout
	@Override
	public void open(Map map, TopologyContext tc, SpoutOutputCollector collector) {
		super.open(map, tc, collector);
		try {
			_fileSection = tc.getThisTaskIndex();
			_reader = new SerializableFileInputStream(new File(_inputPath),
					1 * 1024 * 1024, _fileSection, _fileParts);

//			_reader = new SerializableHDFSFileInputStream(_inputPath,
//					1 * 1024 * 1024, _fileSection, _fileParts);

		} catch (final Exception e) {
			final String error = MyUtilities.getStackTrace(e);
			LOG.info(error);
			throw new RuntimeException("Filename not found:" + error);
		}
	}

	// HELPER methods
	protected String readLine() {
		String text = null;
		try {
			text = _reader.readLine();
		} catch (final IOException e) {
			final String errMessage = MyUtilities.getStackTrace(e);
			LOG.info(errMessage);
		}
		return text;
	}

	/*
	 * sending EOF in AckEveryTuple mode when we send at least one tuple to the
	 * next component
	 */
	private void sendEOF() {
		if (MyUtilities.isAckEveryTuple(getConf()))
			if (_pendingTuples == 0)
				if (!_hasSentEOF) {
					_hasSentEOF = true;
					getCollector().emit(SystemParameters.EOF_STREAM,
							new Values(SystemParameters.EOF));
				}
	}
}