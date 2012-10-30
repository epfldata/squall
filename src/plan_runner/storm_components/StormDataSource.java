package plan_runner.storm_components;

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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;
import plan_runner.components.ComponentProperties;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.CustomReader;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicBatchSend;
import plan_runner.utilities.SerializableFileInputStream;
import plan_runner.utilities.SystemParameters;

public class StormDataSource extends BaseRichSpout implements StormEmitter, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDataSource.class);

	private String _inputPath;
	private String _ID;
	private int _hierarchyPosition;

	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private boolean _hasReachedEOF=false;
        private boolean _hasSentEOF = false; //have sent EOF to TopologyKiller (AckEachTuple mode)
	private boolean _hasSentLastAck = false; // AckLastTuple mode
        
	private long _pendingTuples=0;
        private String _componentIndex; //a unique index in a list of all the components
                            //used as a shorter name, to save some network traffic
                            //it's of type int, but we use String to save more space

	private CustomReader _reader=null;
	private Map _conf;
	private SpoutOutputCollector _collector;
	private ChainOperator _operatorChain;

	private int _numSentTuples=0;
	private boolean _printOut;

	private int _fileSection, _fileParts;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicBatchSend _periodicBatch;
	private long _batchOutputMillis;
        
        //for CustomTimestamp mode
        private double _totalLatency;
        
        //for ManualBatch(Queuing) mode
        private List<Integer> _targetTaskIds;
        private int _targetParallelism;

        private StringBuffer[] _targetBuffers;
        private long[] _targetTimestamps;


	public StormDataSource(ComponentProperties cp,
                        List<String> allCompNames,
			String inputPath,
			int hierarchyPosition,
			int parallelism,
			TopologyBuilder	builder,
			TopologyKiller killer,
			Config conf) {
		_conf = conf;
		_operatorChain = cp.getChainOperator();
		_hierarchyPosition = hierarchyPosition;
		_ID=cp.getName();
                _componentIndex = String.valueOf(allCompNames.indexOf(_ID));
		_inputPath=inputPath;
		_batchOutputMillis = cp.getBatchOutputMillis();

		_hashIndexes=cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();

		_printOut = cp.getPrintOut();

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
                        
                        if(SystemParameters.isExisting(_conf, "BATCH_TIMEOUT_MILLIS")){
                            int timeout = SystemParameters.getInt(_conf, "BATCH_TIMEOUT_MILLIS");
                            if(timeout > 0 && _numSentTuples > 0 &&
                                      _numSentTuples % MyUtilities.getCompBatchSize(_ID, _conf) == 0){
                                Utils.sleep(timeout);
                            }
                        }
                        long timestamp = System.currentTimeMillis();

                        
			String line = readLine();
                        if(line==null) {
                                if(!_hasReachedEOF){
                                    _hasReachedEOF=true;
                                    //we reached EOF, first time this happens we invoke the method:
                                    eofFinalization();
                                }
                                sendEOF();
                                //sleep since we are not going to do useful work,
                                //  but still are looping in nextTuple method
				Utils.sleep(SystemParameters.EOF_TIMEOUT_MILLIS);
				return;
			}

			List<String> tuple = MyUtilities.fileLineToTuple(line, _conf);
			applyOperatorsAndSend(tuple, timestamp);
		}

	protected void applyOperatorsAndSend(List<String> tuple, long timestamp){
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
                        tupleSend(tuple, null, timestamp);
		}
                if(MyUtilities.isPrintLatency(_hierarchyPosition, _conf)){
                    printTupleLatency(_numSentTuples - 1, timestamp);
                }
	}

        /*
         * whatever is inside this method is done only once
         */
	private void eofFinalization(){
		printContent();

		if(!MyUtilities.isAckEveryTuple(_conf)){
			if(_hierarchyPosition == FINAL_COMPONENT){
                            if(!_hasSentEOF){
                                _hasSentEOF=true; // to ensure we will not send multiple EOF per single spout
                                _collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
                            }
			} else {
                            if(!_hasSentLastAck){
                                _hasSentLastAck = true;
                                List<String> lastTuple = new ArrayList<String>(Arrays.asList(SystemParameters.LAST_ACK));
                                tupleSend(lastTuple, null, 0);
                            }
                        }
		}
	}  

        
        /*
         * sending EOF in AckEveryTuple mode when we send at least one tuple to the next component
         */
        private void sendEOF(){
            if (MyUtilities.isAckEveryTuple(_conf)){
                if (_pendingTuples == 0) {
                    if(!_hasSentEOF){
                        _hasSentEOF = true;
                        _collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
                    }
                }
            }
        }

        
	@Override
		public void tupleSend(List<String> tuple, Tuple stormTupleRcv, long timestamp) {
                        boolean isLastAck = MyUtilities.isFinalAck(tuple, _conf);
                        
                        if(!MyUtilities.isManualBatchingMode(_conf)){
                            if(isLastAck){
                                finalAckSend();
                            }else{
                                regularTupleSend(tuple, timestamp);
                            }
                        }else{                     
                            if(!isLastAck){
                                //appending tuple if it is not lastAck
                                addToManualBatch(tuple, timestamp);
                                if(_numSentTuples % MyUtilities.getCompBatchSize(_ID, _conf) == 0){
                                    manualBatchSend();
                                }
                            }else{
                                //has to be sent separately, because of the BatchStreamGrouping logic
                                manualBatchSend(); // we need to send the last batch, if it is not empty
                                finalAckSend();
                            }
                        }
		}
        
                //non-ManualBatchMode
                private void regularTupleSend(List<String> tuple, long timestamp){
                    Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 
                                timestamp,
                                _componentIndex,
				_hashIndexes, 
                                _hashExpressions, 
                                _conf);
                    MyUtilities.sendTuple(stormTupleSnd, _collector, _conf);
                }        
        
                private void finalAckSend(){
                    Values values = MyUtilities.createUniversalFinalAckTuple(_conf);
                    _collector.emit(values);
                }
        
                //ManualBatchMode
                private void addToManualBatch(List<String> tuple, long timestamp){
                        String tupleHash = MyUtilities.createHashString(tuple, _hashIndexes, _hashExpressions, _conf);
                        int dstIndex = MyUtilities.chooseTargetIndex(tupleHash, _targetParallelism);

                        //we put in queueTuple based on tupleHash
                        //the same hash is used in BatchStreamGrouping for deciding where a particular targetBuffer is to be sent
                        String tupleString = MyUtilities.tupleToString(tuple, _conf);
                        
                        if(MyUtilities.isCustomTimestampMode(_conf)){
                            if(_targetBuffers[dstIndex].length() == 0){
                                //timestamp of the first tuple being added to a buffer is the timestamp of the buffer
                                _targetTimestamps[dstIndex] = timestamp;
                            }
                        }
                        _targetBuffers[dstIndex].append(tupleHash).append(SystemParameters.MANUAL_BATCH_HASH_DELIMITER)
                                .append(tupleString).append(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
                }
                
                private void manualBatchSend(){
                        for(int i=0; i<_targetParallelism; i++){
                            String tupleString = _targetBuffers[i].toString();
                            _targetBuffers[i] = new StringBuffer("");

                            if(!tupleString.isEmpty()){
                                //some buffers might be empty
                                if(MyUtilities.isCustomTimestampMode(_conf)){
                                    _collector.emit(new Values(_componentIndex, tupleString, _targetTimestamps[i]));
                                }else{
                                    _collector.emit(new Values(_componentIndex, tupleString));
                                }
                            }
                        }
                }
        
                
                //Other Stuff
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
							tupleSend(MyUtilities.stringToTuple(tuple, _conf), null, 0);
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
                        
                        _targetTaskIds = MyUtilities.findTargetTaskIds(tc);
                        _targetParallelism = _targetTaskIds.size();
                        _targetBuffers = new StringBuffer[_targetParallelism];
                        _targetTimestamps = new long[_targetParallelism];
                        for(int i=0; i<_targetParallelism; i++){
                            _targetBuffers[i] = new StringBuffer("");
                        }

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
                        
                        List<String> outputFields= new ArrayList<String>();
                        if(MyUtilities.isManualBatchingMode(_conf)){
                            outputFields.add("CompIndex");
                            outputFields.add("Tuple"); // string
                        }else{
                            outputFields.add("CompIndex");
                            outputFields.add("Tuple"); // list of string
                            outputFields.add("Hash");
                        }
                        if(MyUtilities.isCustomTimestampMode(_conf)){
                            outputFields.add("Timestamp");
                        }
			declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(outputFields));
		}
        
        //tupleSerialNum starts from 0
        @Override
        public void printTupleLatency(long tupleSerialNum, long timestamp) {
            int freqCompute = SystemParameters.getInt(_conf, "FREQ_TUPLE_LOG_COMPUTE");
            int freqWrite = SystemParameters.getInt(_conf, "FREQ_TUPLE_LOG_WRITE");
            int startupIgnoredTuples = SystemParameters.getInt(_conf, "INIT_IGNORED_TUPLES");
            
            if(tupleSerialNum >= startupIgnoredTuples){
                tupleSerialNum = tupleSerialNum - startupIgnoredTuples; // start counting from zero when computing starts
                if(tupleSerialNum % freqCompute == 0){
                    long latency = System.currentTimeMillis() - timestamp;
                    _totalLatency += latency;
                }
                if(tupleSerialNum % freqWrite == 0){
                    long numberOfSamples = (tupleSerialNum / freqCompute) + 1; // note that it is divisible
                    LOG.info("Taking into account every " + freqCompute + "th tuple, and printing every " + freqWrite + "th one.");
                    LOG.info("AVERAGE tuple latency so far is " + _totalLatency/numberOfSamples);
                }
            }
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
			return _ID;
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