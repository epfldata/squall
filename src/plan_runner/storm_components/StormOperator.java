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
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;

public class StormOperator extends StormBoltComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormOperator.class);
    
    private ChainOperator _operatorChain;
    
    private int _numSentTuples=0;

    //if this is set, we receive using direct stream grouping
    private List<String> _fullHashList;

    //for agg batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicAggBatchSend _periodicAggBatch;
    private long _aggBatchOutputMillis;       

    public StormOperator(StormEmitter parentEmitter,
            ComponentProperties cp,
            List<String> allCompNames,
            int hierarchyPosition,
            TopologyBuilder builder,
            TopologyKiller killer,
            Config conf) {
		super(cp, allCompNames, hierarchyPosition, conf);
    	
        _aggBatchOutputMillis = cp.getBatchOutputMillis();

        int parallelism = SystemParameters.getInt(conf, getID()+"_PAR");

//        if(parallelism > 1 && distinct != null){
//            throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
//        }
        _operatorChain = cp.getChainOperator();

        InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
        
        _fullHashList = cp.getFullHashList();
        
        if(MyUtilities.isManualBatchingMode(getConf())){
            currentBolt = MyUtilities.attachEmitterBatch(conf, _fullHashList, currentBolt, parentEmitter);
        }else{
            currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList, currentBolt, parentEmitter);
        }
        
        if( getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
            killer.registerComponent(this, parallelism);
        }

        if (cp.getPrintOut() && _operatorChain.isBlocking()){
           currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
        }
    }

    //from IRichBolt
    @Override
    public void execute(Tuple stormTupleRcv) {
        if(_firstTime && MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)){
            _periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis, this);
            _firstTime = false;
        }

        if (receivedDumpSignal(stormTupleRcv)) {
            MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
            return;
        }
        
        if(!MyUtilities.isManualBatchingMode(getConf())){
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
                List<String> tuple = MyUtilities.stringToTuple(inputTupleString, getConf());

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
        getCollector().ack(stormTupleRcv);
    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple, boolean isLastInBatch){
        if(MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)){
            try {
                _semAgg.acquire();
            } catch (InterruptedException ex) {}
        }
	tuple = _operatorChain.process(tuple);
        if(MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)){
            _semAgg.release();
        }

        if(tuple == null){
            getCollector().ack(stormTupleRcv);
            return;
        }
        _numSentTuples++;
        printTuple(tuple);

        if(MyUtilities.isSending(getHierarchyPosition(), _aggBatchOutputMillis)){
            if(MyUtilities.isCustomTimestampMode(getConf())){
                long timestamp;
                if(MyUtilities.isManualBatchingMode(getConf())){
                    timestamp = stormTupleRcv.getLong(2);
                }else{
                    timestamp = stormTupleRcv.getLong(3);
                }
                    tupleSend(tuple, stormTupleRcv, timestamp);
            }else{
                tupleSend(tuple, stormTupleRcv, 0);
            }		
	}
        if(MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())){
            long timestamp;
            if(MyUtilities.isManualBatchingMode(getConf())){
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
    public void aggBatchSend(){
        if(MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)){
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
                        tupleSend(MyUtilities.stringToTuple(tuple, getConf()), null, 0);
                    }

                    //clearing
                    agg.clearStorage();

                    _semAgg.release();
                }
            }
        }
    }

    //from StormComponent
    @Override
    public String getInfoID() {
        String str = "OperatorComponent " + getID() + " has ID: " + getID();
        return str;
    }

	@Override
	public PeriodicAggBatchSend getPeriodicAggBatch() {
		return _periodicAggBatch;
	}

	@Override
	public long getNumSentTuples() {
		return _numSentTuples;
	}

	@Override
	public ChainOperator getChainOperator() {
		return _operatorChain;
	}
}