package stormComponents;


import backtype.storm.Config;
import stormComponents.synchronization.TopologyKiller;
import stormComponents.synchronization.Flusher;
import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import utilities.MyUtilities;
import utilities.SerializableFileInputStream;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Tuple;
import expressions.ValueExpression;
import java.util.List;
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import utilities.SystemParameters;

import org.apache.log4j.Logger;
import utilities.CustomReader;

public class StormDataSource extends BaseRichSpout implements StormEmitter, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDataSource.class);

	private String _inputPath;
	private String _componentName;
        private int _hierarchyPosition;
        
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

        private boolean _hasReachedEOF=false;
	private long _pendingTuples=0;
	private int _ID;

        private CustomReader _reader=null;
        private Map _conf;
        private SpoutOutputCollector _collector;
        private ChainOperator _operatorChain;

        private int _numSentTuples=0;
        private boolean _alreadyPrintedContent;
        private boolean _printOut;

        private int _fileSection, _fileParts;
        private boolean _hasEmitted=false; //have this spout emitted at least one tuple

	private spoutBufferBolt _sbb;

        //NoAck
        private boolean _hasSentLastAck = false;

	public StormDataSource(String componentName,
                        String inputPath,
                        List<Integer> hashIndexes,
                        List<ValueExpression> hashExpressions,
                        SelectionOperator selection,
                        DistinctOperator distinct,
                        ProjectionOperator projection,
                        AggregateOperator aggregation,
                        int hierarchyPosition,
                        boolean printOut,
                        int parallelism,
                        TopologyBuilder	builder,
                        TopologyKiller killer,
                        Flusher flusher,
                        Config conf) {
                _conf = conf;
                _operatorChain = new ChainOperator(selection, distinct, projection, aggregation);
                _hierarchyPosition = hierarchyPosition;
		_ID=MyUtilities.getNextTopologyId();
		_componentName=componentName;
		_inputPath=inputPath;

                _hashIndexes=hashIndexes;
                _hashExpressions = hashExpressions;
		
                _printOut = printOut;

                _fileParts = parallelism;

		builder.setSpout(Integer.toString(_ID), this, parallelism);
                if(MyUtilities.isAckEveryTuple(conf)){
                    killer.registerComponent(this, 1);
                }
		if (flusher != null) {
		    _sbb = new spoutBufferBolt(builder, this, flusher);
		    _ID = _sbb.getID(); // Set the id of this spout to be the same
            						// as its _sbb, in order to connect it successfully
		}
	}

        // from IRichSpout interface
        @Override
	public void nextTuple() {
		if(_hasReachedEOF) {
                    if(!_alreadyPrintedContent){
                        _alreadyPrintedContent=true;
                        printContent();
                    }
                    if(MyUtilities.isAckEveryTuple(_conf)){
                        if(!_hasEmitted){
                                //we never emitted anything, and we reach end of the file
                                //nobody will call our ack method
                                _hasEmitted=true; // to ensure we will not send multiple EOF per single spout
                                _collector.emit(SystemParameters.EOFmessageStream, new Values("EOF"));
                        }
                    }else{
                        if(_hierarchyPosition == FINAL_COMPONENT){
                            if(!_hasEmitted){
                                //we never emitted anything, and we reach end of the file
                                //nobody will call our ack method
                                _hasEmitted=true; // to ensure we will not send multiple EOF per single spout
                                _collector.emit(SystemParameters.EOFmessageStream, new Values("EOF"));
                            }
                        } else {
                            if(!_hasSentLastAck){
                                _hasSentLastAck = true;
                                _collector.emit(new Values("N/A", "LAST_ACK", "N/A"));
                            }
                        }
                    }

                    Utils.sleep(SystemParameters.EOF_TIMEOUT_MILLIS);
                    return;
		}

		String line = readLine();
		if(line==null) {
                    _hasReachedEOF=true;
                    return;
		}

		List<String> tuple = MyUtilities.fileLineToTuple(line, _conf);

                // do selection and projection
                tuple = _operatorChain.process(tuple);
                if(tuple==null){
                    return;
                }
                _numSentTuples++;
                printTuple(tuple);

		String tupleString = MyUtilities.tupleToString(tuple, _conf);
		String tupleHash = MyUtilities.createHashString(tuple, _hashIndexes, _hashExpressions, _conf);

                _hasEmitted = true;
                _pendingTuples++;

                String msgId = null;
                if(MyUtilities.isAckEveryTuple(_conf)){
                    msgId = "TrackTupleAck";
                }
                _collector.emit(new Values(_componentName, tupleString, tupleHash), msgId);
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

	@Override
	public void ack(Object msgId) {
		_pendingTuples--;	    
		if (_hasReachedEOF) {
			if (_pendingTuples == 0) {
                            if(MyUtilities.isAckEveryTuple(_conf)){
                                _collector.emit(SystemParameters.EOFmessageStream, new Values("EOF"));
                            }else{
                                _collector.emit(new Values("N/A", "LAST_ACK", "N/A"));
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
                throw new RuntimeException("Failing tuple in " + _componentName);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
                if(MyUtilities.isAckEveryTuple(_conf) || _hierarchyPosition == FINAL_COMPONENT){
                    declarer.declareStream(SystemParameters.EOFmessageStream, new Fields("EOF"));
                }
		List<String> outputFields= new ArrayList<String>();
		outputFields.add("TableName");
		outputFields.add("Tuple");
		outputFields.add("Hash");		
		declarer.declareStream(SystemParameters.DatamessageStream, new Fields(outputFields));
	}

        private void printTuple(List<String> tuple){
            if(_printOut){
                if((_operatorChain == null) || !_operatorChain.isBlocking()){
                    StringBuilder sb = new StringBuilder();
                    sb.append("\nComponent ").append(_componentName);
                    sb.append("\nReceived tuples: ").append(_numSentTuples);
                    sb.append(" Tuple: ").append(MyUtilities.tupleToString(tuple, _conf));
                    LOG.info(sb.toString());
                }
            }
        }

        private void printContent() {
                if(_printOut){
                    if((_operatorChain != null) && _operatorChain.isBlocking()){
                        Operator lastOperator = _operatorChain.getLastOperator();
                        if (lastOperator instanceof AggregateOperator){
                            MyUtilities.printBlockingResult(_componentName,
                                                        (AggregateOperator) lastOperator,
                                                        _hierarchyPosition,
                                                        _conf,
                                                        LOG);
                        }else{
                            MyUtilities.printBlockingResult(_componentName,
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
	public int getID() {
		return _ID;
	}

        // from StormEmitter interface
        @Override
        public int[] getEmitterIDs() {
            return new int[]{_ID};
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return _componentName;
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
            sb.append("Table ").append(_componentName).append(" has ID: ").append(_ID);
            if(_sbb!=null){
                sb.append(_sbb.getInfoID());
            }
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
        
	/* Bolt that is used to _buffer tuples */
	public static class spoutBufferBolt extends BaseRichBolt implements StormComponent {
		private int _ID;
		private int _flusherId;
		private List<Tuple> _buffer;
		private boolean _isBuffering;
		private StormDataSource _motherSpout;
		private static int _streamnum = 0;

		OutputCollector _collector;
		private Tuple _ackTuple;

		public spoutBufferBolt(TopologyBuilder builder,
                                StormDataSource motherSpout,
				Flusher flusher) {
			_ID = MyUtilities.getNextTopologyId();
			_flusherId = flusher.getID();
			_isBuffering = false;
			_motherSpout = motherSpout;
			_buffer = new ArrayList<Tuple>();
			builder.setBolt(Integer.toString(_ID), this)
                                .allGrouping(Integer.toString(motherSpout.getID()))
                                .allGrouping(Integer.toString(flusher.getID()), SystemParameters.FlushmessageStream + _streamnum);
			_streamnum++;
		}

                //from IRichSpout interface
		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			// Is this a begin flush message?
					if (tuple.getSourceComponent().equalsIgnoreCase(Integer.toString(_flusherId))) {
						_isBuffering = true;
						_ackTuple = tuple;
						return;
					}

			// Are we buffering ?
					if (_isBuffering) {
						// Add this tuple to the list so that it is acked
						_buffer.add(tuple);
						// Shall we stop buffering?
						if (_motherSpout.getPendingTuples() == 0) {
							int bufferedElemsCount = _buffer.size();
							//            		LOG.info("Emitting from node " + getID()+" "+ bufferedElemsCount+" tuples buffered while flushing.");
							for (int i = 0; i < bufferedElemsCount; i++) {
								Tuple t = _buffer.remove(0);
								String tableName = t.getString(0);
								String tuplePayLoad = t.getString(1);
								String hash = t.getString(2);
								_collector.emit(t, new Values(tableName, tuplePayLoad, hash));
								_collector.ack(t);
							}
							_collector.ack(_ackTuple);
							_isBuffering = false;
						}
						return;
					}

					// Else, we can just propagate the input tuple
					String tableName = tuple.getString(0);
					String tuplePayLoad = tuple.getString(1);
					String hash = tuple.getString(2);
					_collector.emit(tuple, new Values(tableName, tuplePayLoad, hash));
					_collector.ack(tuple);
		}

		@Override
		public void cleanup() {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// The same output as the mother spout.
			List<String> outputFields = new ArrayList<String>();
			outputFields.add("TableName");
			outputFields.add("Tuple");
			outputFields.add("Hash");
			declarer.declareStream(SystemParameters.DatamessageStream, new Fields(outputFields));
		}

                // from StormComponent interface
                @Override
                public int getID() {
			return _ID;
		}

                public String getInfoID() {
                    String str = "SBB has ID: " + _ID;
                    return str;
                }
	}
}
