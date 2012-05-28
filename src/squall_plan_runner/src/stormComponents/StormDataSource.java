package stormComponents;


import backtype.storm.Config;
import stormComponents.synchronization.TopologyKiller;
import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import utilities.MyUtilities;
import utilities.SerializableFileInputStream;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import expressions.ValueExpression;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import utilities.SystemParameters;

import org.apache.log4j.Logger;
import utilities.CustomReader;
import utilities.PeriodicBatchSend;

public class StormDataSource extends BaseRichSpout implements StormEmitter, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDataSource.class);

	private String _inputPath;
	private String _ID;
	private int _hierarchyPosition;

	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private boolean _hasReachedEOF=false;
	private long _pendingTuples=0;
        private String _componentIndex; //a unique index in a list of all the components
                            //used as a shorter name, to save some network traffic
                            //it's of type int, but we use String to save more space

	private CustomReader _reader=null;
	private Map _conf;
	private SpoutOutputCollector _collector;
	private ChainOperator _operatorChain;

	private int _numSentTuples=0;
	private boolean _alreadyPrintedContent;
	private boolean _printOut;

	private int _fileSection, _fileParts;
	private boolean _hasEmitted=false; //have this spout emitted at least one tuple

	//NoAck
	private boolean _hasSentLastAck = false;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicBatchSend _periodicBatch;
	private long _batchOutputMillis;

	public StormDataSource(String componentName,
                        List<String> allCompNames,
			String inputPath,
			List<Integer> hashIndexes,
			List<ValueExpression> hashExpressions,
			SelectionOperator selection,
			DistinctOperator distinct,
			ProjectionOperator projection,
			AggregateOperator aggregation,
			int hierarchyPosition,
			boolean printOut,
			long batchOutputMillis,
			int parallelism,
			TopologyBuilder	builder,
			TopologyKiller killer,
			Config conf) {
		_conf = conf;
		_operatorChain = new ChainOperator(selection, distinct, projection, aggregation);
		_hierarchyPosition = hierarchyPosition;
		_ID=componentName;
                _componentIndex = String.valueOf(allCompNames.indexOf(componentName));
		_inputPath=inputPath;
		_batchOutputMillis = batchOutputMillis;

		_hashIndexes=hashIndexes;
		_hashExpressions = hashExpressions;

		_printOut = printOut;

		_fileParts = parallelism;

                if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		builder.setSpout(_ID, this, parallelism);
		if(MyUtilities.isAckEveryTuple(conf)){
			killer.registerComponent(this, parallelism);
		}
	}

	// from IRichSpout interface
	@Override
		public void nextTuple() {
			if(_firstTime && MyUtilities.isBatchOutputMode(_batchOutputMillis)){
				_periodicBatch = new PeriodicBatchSend(_batchOutputMillis, this);
				_firstTime = false;
			}

			if(_hasReachedEOF) {
				eofFinalization();

				Utils.sleep(SystemParameters.EOF_TIMEOUT_MILLIS);
				return;
			}

			String line = readLine();
			if(line==null) {
				_hasReachedEOF=true;
				return;
			}

			List<String> tuple = MyUtilities.fileLineToTuple(line, _conf);
			applyOperatorsAndSend(tuple);
		}

	protected void applyOperatorsAndSend(List<String> tuple){
		// do selection and projection
		if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
			try {
				_semAgg.acquire();
			} catch (InterruptedException ex) {}
		}
		tuple = _operatorChain.process(tuple);
		if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
			_semAgg.release();
		}

		if(tuple==null){
			return;
		}
                
		_numSentTuples++;
		_pendingTuples++;
		printTuple(tuple);

		if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis)){
			tupleSend(tuple, null);
		}

                if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis) || MyUtilities.isBatchOutputMode(_batchOutputMillis)){
                    // if we are sending tuple, or we will do it in future, we have to set hasEmitter
                    _hasEmitted = true;
                }
	}

	private void eofFinalization(){
		if(!_alreadyPrintedContent){
			_alreadyPrintedContent=true;
			printContent();
		}
		if(MyUtilities.isAckEveryTuple(_conf)){
			if(!_hasEmitted){
				//we never emitted anything, and we reach end of the file
				//nobody will call our ack method
				_hasEmitted=true; // to ensure we will not send multiple EOF per single spout
				_collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
			}
		}else{
			if(_hierarchyPosition == FINAL_COMPONENT){
				if(!_hasEmitted){
					//we never emitted anything, and we reach end of the file
					//nobody will call our ack method
					_hasEmitted=true; // to ensure we will not send multiple EOF per single spout
					_collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
				}
			} else {
				if(!_hasSentLastAck){
					_hasSentLastAck = true;
                                        List<String> lastTuple = new ArrayList<String>(Arrays.asList(SystemParameters.LAST_ACK));
					_collector.emit(new Values("N/A", lastTuple, "N/A"));
				}
			}
		}
	}

	@Override
		public void tupleSend(List<String> tuple, Tuple stormTupleRcv) {
			Values stormTupleSnd = MyUtilities.createTupleValues(tuple, _componentIndex,
					_hashIndexes, _hashExpressions, _conf);
			MyUtilities.sendTuple(stormTupleSnd, _collector, _conf);
		}

	@Override
		public void batchSend(){
			if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
				if (_operatorChain != null){
					Operator lastOperator = _operatorChain.getLastOperator();
					if(lastOperator instanceof AggregateOperator){
						try {
							_semAgg.acquire();
						} catch (InterruptedException ex) {}

						//sending
						AggregateOperator agg = (AggregateOperator) lastOperator;
						List<String> tuples = agg.getContent();
						for(String tuple: tuples){
							tupleSend(MyUtilities.stringToTuple(tuple, _conf), null);
						}

						//clearing
						agg.clearStorage();

						_semAgg.release();
					}
				}
			}
		}

	@Override
		public void open(Map map, TopologyContext tc, SpoutOutputCollector collector){
			_collector = collector;
			_fileSection = tc.getThisTaskIndex();

			try {
				//  _reader = new BufferedReader(new FileReader(new File(_inputPath)));
				_reader = new SerializableFileInputStream(new File(_inputPath),1*1024*1024, _fileSection, _fileParts);

			} catch (Exception e) {
				String error=MyUtilities.getStackTrace(e);
				LOG.info(error);
				throw new RuntimeException("Filename not found:" + error);
			}
		}

        //ack method on spout is called only if in AckEveryTuple mode (ACKERS > 0)
	@Override
		public void ack(Object msgId) {
			_pendingTuples--;	    
			if (_hasReachedEOF) {
				if (_pendingTuples == 0) {
					if(MyUtilities.isAckEveryTuple(_conf)){
						_collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
					}
				}
			}
		}

	@Override
		public void close() {
			try {
				_reader.close();
			} catch (Exception e) {
				String error=MyUtilities.getStackTrace(e);
				LOG.info(error);
			}
		}

	@Override
		public void fail(Object msgId) {
			throw new RuntimeException("Failing tuple in " + _ID);

		}

	@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			if(MyUtilities.isAckEveryTuple(_conf) || _hierarchyPosition == FINAL_COMPONENT){
				declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
			}
                        declarer.declareStream(SystemParameters.DATA_STREAM, new Fields("CompIndex", "Tuple", "Hash"));
		}

	@Override
		public void printTuple(List<String> tuple){
			if(_printOut){
				if((_operatorChain == null) || !_operatorChain.isBlocking()){
					StringBuilder sb = new StringBuilder();
					sb.append("\nComponent ").append(_ID);
					sb.append("\nReceived tuples: ").append(_numSentTuples);
					sb.append(" Tuple: ").append(MyUtilities.tupleToString(tuple, _conf));
					LOG.info(sb.toString());
				}
			}
		}

	@Override
		public void printContent() {
			if(_printOut){
				if((_operatorChain != null) && _operatorChain.isBlocking()){
					Operator lastOperator = _operatorChain.getLastOperator();
					if (lastOperator instanceof AggregateOperator){
						MyUtilities.printBlockingResult(_ID,
								(AggregateOperator) lastOperator,
								_hierarchyPosition,
								_conf,
								LOG);
					}else{
						MyUtilities.printBlockingResult(_ID,
								lastOperator.getNumTuplesProcessed(),
								lastOperator.printContent(),
								_hierarchyPosition,
								_conf,
								LOG);
					}
				}
			}
		}

	// from StormComponent interface
	@Override
		public String getID() {
			return _ID;
		}

	// from StormEmitter interface
	@Override
		public String[] getEmitterIDs() {
			return new String[]{_ID};
		}

	@Override
		public String getName() {
			// TODO Auto-generated method stub
			return _ID;
		}

	@Override
		public List<Integer> getHashIndexes(){
			return _hashIndexes;
		}

	@Override
		public List<ValueExpression> getHashExpressions() {
			return _hashExpressions;
		}

	@Override
		public String getInfoID() {
			StringBuilder sb = new StringBuilder();
			sb.append("Table ").append(_ID).append(" has ID: ").append(_ID);
			return sb.toString();
		}

	// Helper methods
	public long getPendingTuples() {
		return _pendingTuples;
	}

	private String readLine(){
		String text=null;
		try {
			text=_reader.readLine();
		} catch (IOException e) {
			String errMessage = MyUtilities.getStackTrace(e);
			LOG.info(errMessage);
		}
		return text;
	}
}
