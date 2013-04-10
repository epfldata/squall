package plan_runner.storm_components;

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
import java.util.ArrayList;
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
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicBatchSend;
import plan_runner.utilities.SystemParameters;

public class StormOperator extends BaseRichBolt implements StormEmitter, StormComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormOperator.class);
    
    private int _hierarchyPosition=INTERMEDIATE;
    private StormEmitter _emitter;
    private String _ID;
    private String _componentIndex; //a unique index in a list of all the components
                            //used as a shorter name, to save some network traffic
                            //it's of type int, but we use String to save more space

    //output has hash formed out of these indexes
    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private ChainOperator _operatorChain;
    private OutputCollector _collector;
    private boolean _printOut;
    private int _numSentTuples=0;
    private Map _conf;

    //if this is set, we receive using direct stream grouping
    private List<String> _fullHashList;

    //for No ACK: the total number of tasks of all the parent components
    private int _numRemainingParents;

    //for batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicBatchSend _periodicBatch;
    private long _batchOutputMillis;
    
    //for Send and Wait mode
    private double _totalLatency;
    private long _numberOfSamples;
    
    //for ManualBatch(Queuing) mode
    private List<Integer> _targetTaskIds;
    private int _targetParallelism;

    private StringBuffer[] _targetBuffers;
    private long[] _targetTimestamps;       

    public StormOperator(StormEmitter emitter,
            ComponentProperties cp,
            List<String> allCompNames,
            int hierarchyPosition,
            TopologyBuilder builder,
            TopologyKiller killer,
            Config conf) {

        _conf = conf;
        _emitter = emitter;
        _ID = cp.getName();
        _componentIndex = String.valueOf(allCompNames.indexOf(_ID));
        _batchOutputMillis = cp.getBatchOutputMillis();

        int parallelism = SystemParameters.getInt(conf, _ID+"_PAR");

//        if(parallelism > 1 && distinct != null){
//            throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
//        }
        _operatorChain = cp.getChainOperator();

        _hashIndexes = cp.getHashIndexes();
        _hashExpressions = cp.getHashExpressions();

        _hierarchyPosition = hierarchyPosition;

        InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);
        
        _fullHashList = cp.getFullHashList();
        
        if(MyUtilities.isManualBatchingMode(_conf)){
            currentBolt = MyUtilities.attachEmitterBatch(conf, _fullHashList, currentBolt, _emitter);
        }else{
            currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList, currentBolt, _emitter);
        }
        
        if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
            killer.registerComponent(this, parallelism);
        }

        _printOut= cp.getPrintOut();
        if (_printOut && _operatorChain.isBlocking()){
           currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
        }
    }

    //from IRichBolt
    @Override
    public void execute(Tuple stormTupleRcv) {
        if(_firstTime && MyUtilities.isBatchOutputMode(_batchOutputMillis)){
            _periodicBatch = new PeriodicBatchSend(_batchOutputMillis, this);
            _firstTime = false;
        }

	if (receivedDumpSignal(stormTupleRcv)) {
            MyUtilities.dumpSignal(this, stormTupleRcv, _collector);
            return;
        }
        
        if(!MyUtilities.isManualBatchingMode(_conf)){
            List<String> tuple = (List<String>) stormTupleRcv.getValue(1);

            if(processFinalAck(tuple, stormTupleRcv)){
                return;
            }
                            
            applyOperatorsAndSend(stormTupleRcv, tuple, true);
                            
        }else{
            String inputBatch = stormTupleRcv.getString(1);
                                
            String[] wholeTuples = inputBatch.split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
            int batchSize = wholeTuples.length;
            for(int i=0; i<batchSize; i++){
                //parsing
                String currentTuple = wholeTuples[i];
                String[] parts = currentTuple.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
                                    
                String inputTupleString = null;
                if(parts.length == 1){
                    //lastAck
                    inputTupleString = parts[0];
                }else{
                    inputTupleString = parts[1];
                }                                 
                List<String> tuple = MyUtilities.stringToTuple(inputTupleString, _conf);

                //final Ack check
                if(processFinalAck(tuple, stormTupleRcv)){
                    if(i !=  batchSize -1){
                        throw new RuntimeException("Should not be here. LAST_ACK is not the last tuple!");
                    }
                    return;
                }
                                    
                //processing a tuple
                if( i == batchSize - 1){
                    applyOperatorsAndSend(stormTupleRcv, tuple, true);
                }else{
                    applyOperatorsAndSend(stormTupleRcv, tuple, false);
                }
            }
        }
        _collector.ack(stormTupleRcv);
    }
    
    //if true, we should exit from method which called this method
    private boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv){
        if(MyUtilities.isFinalAck(tuple, _conf)){
            _numRemainingParents--;
            if(_numRemainingParents == 0 && MyUtilities.isManualBatchingMode(_conf)){
                //flushing before sending lastAck down the hierarchy
                manualBatchSend();
            }
            MyUtilities.processFinalAck(_numRemainingParents, 
                                        _hierarchyPosition, 
                                        _conf,
                                        stormTupleRcv, 
                                        _collector, 
                                        _periodicBatch);
            return true;
        }
        return false;
    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple, boolean isLastInBatch){
        if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
            try {
                _semAgg.acquire();
            } catch (InterruptedException ex) {}
        }
	tuple = _operatorChain.process(tuple);
        if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
            _semAgg.release();
        }

        if(tuple == null){
            _collector.ack(stormTupleRcv);
            return;
        }
        _numSentTuples++;
        printTuple(tuple);

        if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis)){
            if(MyUtilities.isCustomTimestampMode(_conf)){
                long timestamp;
                if(MyUtilities.isManualBatchingMode(_conf)){
                    timestamp = stormTupleRcv.getLong(2);
                }else{
                    timestamp = stormTupleRcv.getLong(3);
                }
                    tupleSend(tuple, stormTupleRcv, timestamp);
            }else{
                tupleSend(tuple, stormTupleRcv, 0);
            }		
	}
        if(MyUtilities.isPrintLatency(_hierarchyPosition, _conf)){
            long timestamp;
            if(MyUtilities.isManualBatchingMode(_conf)){
                if(isLastInBatch){
                    timestamp = stormTupleRcv.getLong(2);
                    printTupleLatency(_numSentTuples - 1, timestamp);
                }
            }else{
                timestamp = stormTupleRcv.getLong(3);
                printTupleLatency(_numSentTuples - 1, timestamp);
            }
        }
    }
  
    @Override
    public void tupleSend(List<String> tuple, Tuple stormTupleRcv, long timestamp) {                       
        if(!MyUtilities.isManualBatchingMode(_conf)){
            regularTupleSend(tuple, stormTupleRcv, timestamp);
        }else{   
            //appending tuple if it is not lastAck
            addToManualBatch(tuple, timestamp);
            if(_numSentTuples % MyUtilities.getCompBatchSize(_ID, _conf) == 0){
                manualBatchSend();
            }                        
        }
    }
        
    //non-ManualBatchMode
    private void regularTupleSend(List<String> tuple, Tuple stormTupleRcv, long timestamp){
        Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 
            timestamp,
            _componentIndex,
            _hashIndexes, 
            _hashExpressions, 
            _conf);
        MyUtilities.sendTuple(stormTupleSnd, stormTupleRcv, _collector, _conf);
    }
    
        //ManualBatchMode
                private void addToManualBatch(List<String> tuple, long timestamp){
                        String tupleHash = MyUtilities.createHashString(tuple, _hashIndexes, _hashExpressions, _conf);
                        int dstIndex = MyUtilities.chooseHashTargetIndex(tupleHash, _targetParallelism);

                        //we put in queueTuple based on tupleHash
                        //the same hash is used in BatchStreamGrouping for deciding where a particular targetBuffer is to be sent
                        String tupleString = MyUtilities.tupleToString(tuple, _conf);
                        
                        if(MyUtilities.isCustomTimestampMode(_conf)){
                            if(_targetBuffers[dstIndex].length() == 0){
                                //timestamp of the first tuple being added to a buffer is the timestamp of the buffer
                                _targetTimestamps[dstIndex] = timestamp;
                            }else{
                                _targetTimestamps[dstIndex] = MyUtilities.getMin(timestamp, _targetTimestamps[dstIndex]);
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
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
        _collector = collector;
        _numRemainingParents = MyUtilities.getNumParentTasks(tc, _emitter);
        
        _targetTaskIds = MyUtilities.findTargetTaskIds(tc);
        _targetParallelism = _targetTaskIds.size();
        _targetBuffers = new StringBuffer[_targetParallelism];
        _targetTimestamps = new long[_targetParallelism];
        for(int i=0; i<_targetParallelism; i++){
            _targetBuffers[i] = new StringBuffer("");
        }        
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if(_hierarchyPosition == FINAL_COMPONENT){ // then its an intermediate stage not the final one
            if(!MyUtilities.isAckEveryTuple(_conf)){
		declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
            }
	}else{
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
    }
    
        @Override
        public void printTupleLatency(long tupleSerialNum, long timestamp) {
            int freqCompute = SystemParameters.getInt(_conf, "FREQ_TUPLE_LOG_COMPUTE");
            int freqWrite = SystemParameters.getInt(_conf, "FREQ_TUPLE_LOG_WRITE");
            int startupIgnoredTuples = SystemParameters.getInt(_conf, "INIT_IGNORED_TUPLES");
            
            if(tupleSerialNum >= startupIgnoredTuples){
                tupleSerialNum = tupleSerialNum - startupIgnoredTuples; // start counting from zero when computing starts
                if(tupleSerialNum % freqCompute == 0){
                    long latency = System.currentTimeMillis() - timestamp;
                    if(latency < 0){
                        LOG.info("Exception! Current latency is " + latency + "ms! Ignoring a tuple!");
                        return;
                    }
                    if(_numberOfSamples < 0){
                        LOG.info("Exception! Number of samples is " + _numberOfSamples + "! Ignoring a tuple!");
                        return;
                    }
                    _totalLatency += latency;
                    _numberOfSamples++;
                }
                if(tupleSerialNum % freqWrite == 0){
                    LOG.info("Taking into account every " + freqCompute + "th tuple, and printing every " + freqWrite + "th one.");
                    LOG.info("AVERAGE tuple latency so far is " + _totalLatency/_numberOfSamples);
                }
            }
        } 
  
    @Override
    public void printTuple(List<String> tuple){
        if(_printOut){
            if(!_operatorChain.isBlocking()){
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
                 if(_operatorChain.isBlocking()){
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

    private boolean receivedDumpSignal(Tuple stormTuple) {
        return stormTuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.DUMP_RESULTS_STREAM);
    }

    //from StormComponent
    @Override
    public String getID() {
        return _ID;
    }

    //from StormEmitter
    @Override
    public String getName() {
        return _ID;
    }

    @Override
    public String[] getEmitterIDs() {
        return new String[]{_ID};
    }

    @Override
    public String getInfoID() {
        String str = "OperatorComponent " + _ID + " has ID: " + _ID;
        return str;
    }

}
