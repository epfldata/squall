package plan_runner.storm_components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.ComponentProperties;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class StormSpoutComponent extends BaseRichSpout implements StormComponent, StormEmitter{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormSpoutComponent.class);

	private Map _conf;
	private SpoutOutputCollector _collector;
	private String _ID;
    private String _componentIndex; //a unique index in a list of all the components
    //used as a shorter name, to save some network traffic
    //it's of type int, but we use String to save more space
	private boolean _printOut;
	private int _hierarchyPosition;	
    
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
	
	public StormSpoutComponent(ComponentProperties cp, 
			List<String> allCompNames, 
			int hierarchyPosition, 
			Map conf){
		_conf = conf;
		_ID=cp.getName();
        _componentIndex = String.valueOf(allCompNames.indexOf(_ID));
		_printOut = cp.getPrintOut();
		_hierarchyPosition = hierarchyPosition;		
        
		_hashIndexes=cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
	}
	
	protected SpoutOutputCollector getCollector(){
		return _collector;
	}
	
	protected Map getConf(){
		return _conf;
	}
	
	protected int getHierarchyPosition(){
		return _hierarchyPosition;
	}
	
	public abstract long getNumSentTuples();
	public abstract ChainOperator getChainOperator();
	
	//BaseRichSpout 	
	@Override
	public void open(Map map, TopologyContext tc, SpoutOutputCollector collector){
		_collector = collector;
                    
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
		if(MyUtilities.isAckEveryTuple(_conf) || _hierarchyPosition == FINAL_COMPONENT){
			declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
		}
                        
        List<String> outputFields= new ArrayList<String>();
        if(MyUtilities.isManualBatchingMode(_conf)){
        	outputFields.add(StormComponent.COMP_INDEX);
        	outputFields.add(StormComponent.TUPLE); // string
        }else{
        	outputFields.add(StormComponent.COMP_INDEX);
        	outputFields.add(StormComponent.TUPLE); // list of string
        	outputFields.add(StormComponent.HASH);
        }
        if(MyUtilities.isCustomTimestampMode(getConf())){
        	outputFields.add(StormComponent.TIMESTAMP);
        }
		declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(outputFields));
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
	public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
			long timestamp) {
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
                if(getNumSentTuples() % MyUtilities.getCompBatchSize(_ID, _conf) == 0){
                    manualBatchSend();
                }
            }else{
                //has to be sent separately, because of the BatchStreamGrouping logic
                manualBatchSend(); // we need to send the last batch, if it is not empty
                finalAckSend();
            }
        }
	}

	//HELPER METHODS
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
        int dstIndex = MyUtilities.chooseHashTargetIndex(tupleHash, _targetParallelism);

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
}