package stormComponents;


import backtype.storm.Config;
import stormComponents.synchronization.TopologyKiller;

import java.util.ArrayList;
import java.util.Map;

import utilities.MyUtilities;

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
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import utilities.SystemParameters;

import org.apache.log4j.Logger;
import utilities.PeriodicBatchSend;

public class StormRandomDataSource extends BaseRichSpout implements StormEmitter, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormRandomDataSource.class);

	private String _inputPath;
	private String _componentName;
        private int _hierarchyPosition;
        
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

        private boolean _hasReachedEOF=false;
	private long _pendingTuples=0;
	private int _ID;

        private Map _conf;
        private SpoutOutputCollector _collector;
        private ChainOperator _operatorChain;

        private int _numSentTuples=0;
        private boolean _alreadyPrintedContent;
        private boolean _printOut;

        private int _fileParts;
        private boolean _hasEmitted=false; //have this spout emitted at least one tuple

        //NoAck
        private boolean _hasSentLastAck = false;

        //for batch sending
        private final Semaphore _semAgg = new Semaphore(1, true);
        private boolean _firstTime = true;
        private PeriodicBatchSend _periodicBatch;
        private long _batchOutputMillis;

        private int _generatedMax = 1500000;
        private int _customerTotal = 1500000;
        private int _ordersTotal = 15000000;

        private Random _randomGenerator = new Random();
        private int _tuplesProduced = -1;
        private int _customerProduced;
        private int _ordersProduced;

	public StormRandomDataSource(String componentName,
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
		_ID=MyUtilities.getNextTopologyId();
		_componentName=componentName;
		_inputPath=inputPath;
                _batchOutputMillis = batchOutputMillis;

                _hashIndexes=hashIndexes;
                _hashExpressions = hashExpressions;
		
                _printOut = printOut;

                _fileParts = parallelism;

		builder.setSpout(Integer.toString(_ID), this, parallelism);
                if(MyUtilities.isAckEveryTuple(conf)){
                    killer.registerComponent(this, 1);
                }

                _customerProduced = _customerTotal/_fileParts;
                _ordersProduced = _ordersTotal/_fileParts;
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
                _hasEmitted = true;
                _numSentTuples++;
                _pendingTuples++;
                printTuple(tuple);

                if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis)){
                    tupleSend(tuple, null);
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
                        _collector.emit(new Values("N/A", SystemParameters.LAST_ACK, "N/A"));
                    }
                 }
            }
        }

        @Override
        public void tupleSend(List<String> tuple, Tuple stormTupleRcv) {
            Values stormTupleSnd = MyUtilities.createTupleValues(tuple, _componentName,
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
               
	}

	@Override
	public void ack(Object msgId) {
		_pendingTuples--;	    
		if (_hasReachedEOF) {
			if (_pendingTuples == 0) {
                            if(MyUtilities.isAckEveryTuple(_conf)){
                                _collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
                            }else{
                                _collector.emit(new Values("N/A", SystemParameters.LAST_ACK, "N/A"));
                            }
			}
		}
	}

	@Override
	public void close() {
		
	}

	@Override
	public void fail(Object msgId) {
                throw new RuntimeException("Failing tuple in " + _componentName);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
                if(MyUtilities.isAckEveryTuple(_conf) || _hierarchyPosition == FINAL_COMPONENT){
                    declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
                }
		List<String> outputFields= new ArrayList<String>();
		outputFields.add("TableName");
		outputFields.add("Tuple");
		outputFields.add("Hash");		
		declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(outputFields));
	}

        @Override
        public void printTuple(List<String> tuple){
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

        @Override
        public void printContent() {
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
            return sb.toString();
        }

        // Helper methods
	public long getPendingTuples() {
		return _pendingTuples;
	}

//        private String readLine(){
//            String text=null;
//            try {
//                text=_reader.readLine();
//            } catch (IOException e) {
//                String errMessage = MyUtilities.getStackTrace(e);
//		LOG.info(errMessage);
//            }
//            return text;
//        }
        private String readLine(){
            _tuplesProduced++;
            String res = null;
            if(_componentName.equalsIgnoreCase("Customer")){
                res = (_randomGenerator.nextInt(_generatedMax)) +"|Pera|Pazova|1|011|sdfa sdwe|FURNITURE|bla" ;
                if(_tuplesProduced == _customerProduced){
                    return null;
                }else{
                    return res;
                }
            }else {
                res = (_randomGenerator.nextInt(_generatedMax)) +"|1111|F|1022.34|1995-11-11|1-URGENT|mika|1-URGENT|ma kakva komentar";
                if(_tuplesProduced == _ordersProduced){
                    return null;
                }else{
                    return res;
                }
            }
        }
}
