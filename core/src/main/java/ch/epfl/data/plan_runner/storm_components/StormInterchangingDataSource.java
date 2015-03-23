package ch.epfl.data.plan_runner.storm_components;

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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import ch.epfl.data.plan_runner.components.InterchangingDataSourceComponent;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.CustomReader;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SerializableFileInputStream;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

/**
 * This class is for interchanging the roles of the data sources. its
 * parallelism is only ONE.
 */
public class StormInterchangingDataSource extends BaseRichSpout implements
		StormEmitter, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger
			.getLogger(StormInterchangingDataSource.class);

	private final String _inputPath1;
	private final String _inputPath2;
	private final String _ID;
	private final int _hierarchyPosition;
	private final List<Integer> _hashIndexes;
	private final List<ValueExpression> _hashExpressions;

	private boolean _hasReachedEOF = false;
	private boolean _hasSentEOF = false; // have sent EOF to TopologyKiller
	// (AckEachTuple mode)
	private boolean _hasReachedEOF1 = false;
	private boolean _hasReachedEOF2 = false;

	private boolean _hasSentLastAck = false; // AckLastTuple mode
	private long _pendingTuples = 0;
	private final String _componentIndexRel1, _componentIndexRel2; // a unique
																	// index
	// in a list of
	// all the
	// components
	// used as a shorter name, to save some network traffic
	// it's of type int, but we use String to save more space

	private CustomReader _readerRel1 = null;
	private CustomReader _readerRel2 = null;
	private final Map _conf;
	private SpoutOutputCollector _collector;
	private final ChainOperator _operatorChainRel1;
	private final ChainOperator _operatorChainRel2;

	private final boolean _printOut;

	/**
	 * Additional members
	 */
	private final int _multFactor;
	private long _numSentRel1 = 0;
	private long _numSentRel2 = 0;

	private int currentRelationPointer = 2;
	private long nextToStop = 1;
	private long _sentPointer = -1;

	// for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicBatch;
	private final long _batchOutputMillis;
	private int _invocations; // how many time execute method is invoked
	private int _tuplesSleep; // after how many _invocations we sleep 1ms

	public StormInterchangingDataSource(InterchangingDataSourceComponent cp,
			List<String> allCompNames, int multFactor, String inputPath1,
			String inputPath2, int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {
		_multFactor = multFactor;
		_conf = conf;
		_operatorChainRel1 = cp.getChainOperatorRel1();
		_operatorChainRel2 = cp.getChainOperatorRel2();

		_hierarchyPosition = hierarchyPosition;

		_ID = cp.getName();

		_componentIndexRel1 = String.valueOf(allCompNames.indexOf(new String(
				_ID.split("-")[0])));
		_componentIndexRel2 = String.valueOf(allCompNames.indexOf(new String(
				_ID.split("-")[1])));

		_inputPath1 = inputPath1;
		_inputPath2 = inputPath2;
		_batchOutputMillis = cp.getBatchOutputMillis();

		if (SystemParameters.isExisting(_conf, "DIP_TUPLES_SLEEP"))
			_tuplesSleep = SystemParameters.getInt(_conf, "DIP_TUPLES_SLEEP");

		_hashIndexes = cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
		_printOut = cp.getPrintOut();
		if (_hierarchyPosition == FINAL_COMPONENT
				&& (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, 1);
		builder.setSpout(_ID, this, 1);
		if (MyUtilities.isAckEveryTuple(conf))
			killer.registerComponent(this, 1);
	}

	// ack method on spout is called only if in AckEveryTuple mode (ACKERS > 0)
	@Override
	public void ack(Object msgId) {
		_pendingTuples--;
	}

	@Override
	public void aggBatchSend() {
	}

	protected boolean applyOperatorsAndSend(List<String> tuple, int RelIndex) {
		// do selection and projection
		if (MyUtilities.isAggBatchOutputMode(_batchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}
		if (RelIndex == 1)
			tuple = _operatorChainRel1.process(tuple, 0);
		else
			tuple = _operatorChainRel2.process(tuple, 0);

		if (MyUtilities.isAggBatchOutputMode(_batchOutputMillis))
			_semAgg.release();

		if (tuple == null)
			return false;

		_pendingTuples++;
		printTuple(tuple);

		if (MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis))
			tupleSend(tuple, null, RelIndex);
		return true;
	}

	@Override
	public void close() {
		try {
			_readerRel1.close();
			_readerRel2.close();
		} catch (final Exception e) {
			final String error = MyUtilities.getStackTrace(e);
			LOG.info(error);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (MyUtilities.isAckEveryTuple(_conf)
				|| _hierarchyPosition == FINAL_COMPONENT)
			declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
					SystemParameters.EOF));
		declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(
				"CompIndex", "Tuple", "Hash"));
	}

	/*
	 * whatever is inside this method is done only once
	 */
	private void eofFinalization() {
		printContent();

		if (!MyUtilities.isAckEveryTuple(_conf))
			if (_hierarchyPosition == FINAL_COMPONENT) {
				if (!_hasSentEOF) {
					_hasSentEOF = true; // to ensure we will not send multiple
					// EOF per single spout
					_collector.emit(SystemParameters.EOF_STREAM, new Values(
							SystemParameters.EOF));
				}
			} else if (!_hasSentLastAck) {
				_hasSentLastAck = true;
				final List<String> lastTuple = new ArrayList<String>(
						Arrays.asList(SystemParameters.LAST_ACK));
				_collector.emit(new Values("N/A", lastTuple, "N/A"));
			}
		if (_operatorChainRel1 != null) {
			_operatorChainRel1.finalizeProcessing();
		}
		if (_operatorChainRel2 != null) {
			_operatorChainRel2.finalizeProcessing();
		}
	}

	@Override
	public void fail(Object msgId) {
		throw new RuntimeException("Failing tuple in " + _ID);

	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return new String[] { _ID };
	}

	// from StormComponent interface
	@Override
	public String getID() {
		return _ID;
	}

	@Override
	public String getInfoID() {
		final StringBuilder sb = new StringBuilder();
		sb.append("Table ").append(_ID).append(" has ID: ").append(_ID);
		return sb.toString();
	}

	@Override
	public String getName() {
		return _ID;
	}

	// Helper methods
	public long getPendingTuples() {
		return _pendingTuples;
	}

	// from IRichSpout interface
	@Override
	public void nextTuple() {
		if (_firstTime && MyUtilities.isAggBatchOutputMode(_batchOutputMillis)) {
			_periodicBatch = new PeriodicAggBatchSend(_batchOutputMillis, this);
			_firstTime = false;
		}

		if (_hasReachedEOF1 && _hasReachedEOF2) { // if both sources finished.
			if (!_hasReachedEOF) {
				_hasReachedEOF = true;
				eofFinalization();
				sendEOF();
			}
			Utils.sleep(SystemParameters.EOF_TIMEOUT_MILLIS);
			return;

		} else if (_hasReachedEOF1) { // if Rel1 source finished :- send Rel2
			// only
			final String line = readLine(2);
			if (line == null)
				_hasReachedEOF2 = true;
			else {
				final List<String> tuple = MyUtilities.fileLineToTuple(line,
						_conf);
				applyOperatorsAndSend(tuple, 2);
				_numSentRel1++;
			}
		} else if (_hasReachedEOF2) { // if Rel2 source finished :- send Rel1
			// only
			final String line = readLine(1);
			if (line == null)
				_hasReachedEOF1 = true;
			else {
				final List<String> tuple = MyUtilities.fileLineToTuple(line,
						_conf);
				applyOperatorsAndSend(tuple, 1);
				_numSentRel2++;
			}
		} else if (currentRelationPointer == 2) {

			final String line = readLine(2);
			boolean isSent = false;
			if (line == null) {
				_hasReachedEOF2 = true;
				return;
			} else {
				final List<String> tuple = MyUtilities.fileLineToTuple(line,
						_conf);
				isSent = applyOperatorsAndSend(tuple, 2);
			}
			if (isSent) {
				_numSentRel2++;
				_sentPointer++;

				if (_sentPointer == nextToStop) {
					_sentPointer = 0;
					nextToStop = (_numSentRel2 * _multFactor) - _numSentRel1;
					currentRelationPointer = 1;
				}
			}
		} else if (currentRelationPointer == 1) {

			final String line = readLine(1);
			boolean isSent = false;
			if (line == null) {
				_hasReachedEOF1 = true;
				return;
			} else {
				final List<String> tuple = MyUtilities.fileLineToTuple(line,
						_conf);
				isSent = applyOperatorsAndSend(tuple, 1);
			}

			if (isSent) {
				_numSentRel1++;
				_sentPointer++;

				if (_sentPointer == nextToStop) {
					_sentPointer = 0;
					nextToStop = (_numSentRel1 * _multFactor) - _numSentRel2;
					currentRelationPointer = 2;
				}
			}
		}

		if (SystemParameters.isExisting(_conf, "DIP_TUPLES_SLEEP")) {
			_invocations++;
			if (_invocations % _tuplesSleep == 0) {
				_invocations = 0;
				Utils.sleep(1);
			}
		}
	}

	@Override
	public void open(Map map, TopologyContext tc, SpoutOutputCollector collector) {
		_collector = collector;

		try {
			_readerRel1 = new SerializableFileInputStream(
					new File(_inputPath1), 1 * 1024 * 1024, 0, 1);
			_readerRel2 = new SerializableFileInputStream(
					new File(_inputPath2), 1 * 1024 * 1024, 0, 1);

		} catch (final Exception e) {
			final String error = MyUtilities.getStackTrace(e);
			LOG.info(error);
			throw new RuntimeException("Filename not found:" + error);
		}
	}

	@Override
	public void printContent() {
	}

	@Override
	public void printTuple(List<String> tuple) {
	}

	@Override
	public void printTupleLatency(long numSentTuples, long timestamp) {

	}

	private String readLine(int RelIndex) {
		String text = null;
		try {
			if (RelIndex == 1)
				text = _readerRel1.readLine();
			else
				text = _readerRel2.readLine();
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
		if (MyUtilities.isAckEveryTuple(_conf))
			if (_pendingTuples == 0)
				if (!_hasSentEOF) {
					_hasSentEOF = true;
					_collector.emit(SystemParameters.EOF_STREAM, new Values(
							SystemParameters.EOF));
				}
	}

	public void tupleSend(List<String> tuple, Tuple stormTupleRcv, int relIndex) {

		String index = "";
		if (relIndex == 1)
			index = _componentIndexRel1;
		else
			index = _componentIndexRel2;

		final Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 0,
				index, _hashIndexes, _hashExpressions, _conf);
		MyUtilities.sendTuple(stormTupleSnd, _collector, _conf);
	}

	// IGNORED
	@Override
	public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
			long timestamp) {

	}
}