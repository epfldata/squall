package plan_runner.storm_components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.ComponentProperties;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class StormBoltComponent extends BaseRichBolt implements StormJoin, StormComponent{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormBoltComponent.class);

	private Map _conf;
	private OutputCollector _collector;
	private String _ID;
    private String _componentIndex; //a unique index in a list of all the components
    //used as a shorter name, to save some network traffic
    //it's of type int, but we use String to save more space
	private boolean _printOut;
	private int _hierarchyPosition;
	private StormEmitter[] _parentEmitters;
	
	//for No ACK: the total number of tasks of all the parent components
	private int _numRemainingParents;
    
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;    
	
    //for ManualBatch(Queuing) mode
    private List<Integer> _targetTaskIds;
    private int _targetParallelism;
    private StringBuffer[] _targetBuffers;
    private long[] _targetTimestamps;
    
    //for CustomTimestamp mode
    private double _totalLatency;
    private long _numberOfSamples;    
	
	public StormBoltComponent(ComponentProperties cp, 
			List<String> allCompNames, 
			int hierarchyPosition,
			Map conf){
		_conf = conf;
		_ID=cp.getName();
        _componentIndex = String.valueOf(allCompNames.indexOf(_ID));
		_printOut = cp.getPrintOut();
		_hierarchyPosition = hierarchyPosition;		
        
		_parentEmitters = cp.getParents();
		
		_hashIndexes=cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
	}
	
	protected OutputCollector getCollector(){
		return _collector;
	}
	
	protected Map getConf(){
		return _conf;
	}
	
	protected int getHierarchyPosition(){
		return _hierarchyPosition;
	}
	
    // invoked from StormSrcStorage only
	protected void setNumRemainingParents(int numParentTasks) {
		_numRemainingParents = numParentTasks;
	}	
	
	public abstract PeriodicAggBatchSend getPeriodicAggBatch();
	public abstract long getNumSentTuples();
	public abstract ChainOperator getChainOperator();
	
	//BaseRichSpout 	
	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		_collector=collector;
		_numRemainingParents = MyUtilities.getNumParentTasks(tc, Arrays.asList(_parentEmitters));
                    
        _targetTaskIds = MyUtilities.findTargetTaskIds(tc);
        _targetParallelism = _targetTaskIds.size();
        _targetBuffers = new StringBuffer[_targetParallelism];
        _targetTimestamps = new long[_targetParallelism];
        for(int i=0; i<_targetParallelism; i++){
        	_targetBuffers[i] = new StringBuffer("");
        }
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
	
	// StormComponent
	@Override
	public String getID() {
		return _ID;
	}

	// StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return new String[]{_ID};
	}

	@Override
	public String getName() {
		return _ID;
	}
	
	//if true, we should exit from method which called this method
    protected boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv){
      	if(MyUtilities.isFinalAck(tuple, getConf())){
       		_numRemainingParents--;
            if(_numRemainingParents == 0 && MyUtilities.isManualBatchingMode(getConf())){
               	//flushing before sending lastAck down the hierarchy
                manualBatchSend();
            }
            MyUtilities.processFinalAck(_numRemainingParents, 
            		getHierarchyPosition(), 
            		getConf(),
            		stormTupleRcv, 
            		getCollector(), 
            		getPeriodicAggBatch());
            return true;
      	}
        return false;
    }
    
	protected boolean receivedDumpSignal(Tuple stormTuple) {
		return stormTuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.DUMP_RESULTS_STREAM);
	}	

	//StormComponent
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
			if((getChainOperator() == null) || !getChainOperator().isBlocking()){
				StringBuilder sb = new StringBuilder();
				sb.append("\nComponent ").append(_ID);
				sb.append("\nReceived tuples: ").append(getNumSentTuples());
				sb.append(" Tuple: ").append(MyUtilities.tupleToString(tuple, _conf));
				LOG.info(sb.toString());
			}
		}
	}

    @Override
	public void printContent() {
		if(_printOut){
			if((getChainOperator() != null) && getChainOperator().isBlocking()){
				Operator lastOperator = getChainOperator().getLastOperator();
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

    @Override
	public void tupleSend(List<String> tuple, Tuple stormTupleRcv, long timestamp) {                       
    	if(!MyUtilities.isManualBatchingMode(_conf)){
    		regularTupleSend(tuple, stormTupleRcv, timestamp);
        }else{   
			//appending tuple if it is not lastAck
            addToManualBatch(tuple, timestamp);
            if(getNumSentTuples() % MyUtilities.getCompBatchSize(_ID, _conf) == 0){
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
            	// on a bolt, tuples might arrive out of order wrt timestamps
                _targetTimestamps[dstIndex] = MyUtilities.getMin(timestamp, _targetTimestamps[dstIndex]);
            }
        }
        _targetBuffers[dstIndex].append(tupleHash).append(SystemParameters.MANUAL_BATCH_HASH_DELIMITER)
        	.append(tupleString).append(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
    }
    
    protected void manualBatchSend(){
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
}