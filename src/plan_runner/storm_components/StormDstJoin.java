package plan_runner.storm_components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import plan_runner.components.ComponentProperties;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.operators.ProjectOperator;
import plan_runner.storage.AggregationStorage;
import plan_runner.storage.BasicStore;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;
import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;

public class StormDstJoin extends StormBoltComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDstJoin.class);

	private String _firstEmitterIndex, _secondEmitterIndex;
	
	private BasicStore<ArrayList<String>> _firstSquallStorage, _secondSquallStorage;
	private ProjectOperator _firstPreAggProj, _secondPreAggProj; // exists only for preaggregations
	// performed on the output of the aggregationStorage
	
	private ChainOperator _operatorChain;
	private List<Integer> _rightHashIndexes; //hash indexes from the right parent	
	
	private long _numSentTuples=0;

	//for load-balancing
	private List<String> _fullHashList;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
	private long _aggBatchOutputMillis;

	public StormDstJoin(StormEmitter firstEmitter,
			StormEmitter secondEmitter,
			ComponentProperties cp,
			List<String> allCompNames,
			BasicStore<ArrayList<String>> firstSquallStorage,
			BasicStore<ArrayList<String>> secondSquallStorage,
			ProjectOperator firstPreAggProj,
			ProjectOperator secondPreAggProj,
			int hierarchyPosition,
			TopologyBuilder builder,
			TopologyKiller killer,
			Config conf) {
		super(cp, allCompNames, hierarchyPosition, conf);
		
		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter.getName()));
		_rightHashIndexes = cp.getParents()[1].getHashIndexes();

		_firstSquallStorage = firstSquallStorage;
		_secondSquallStorage = secondSquallStorage;
		_firstPreAggProj = firstPreAggProj;
		_secondPreAggProj = secondPreAggProj;		

		_operatorChain = cp.getChainOperator();
		_fullHashList = cp.getFullHashList();
		
		_aggBatchOutputMillis = cp.getBatchOutputMillis();
		
		int parallelism = SystemParameters.getInt(getConf(), getID() +"_PAR");

		/*
		if(parallelism > 1 && distinct != null){
			throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
		}
		*/
		
		// connecting with previous level
		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
        if(MyUtilities.isManualBatchingMode(getConf())){
        	currentBolt = MyUtilities.attachEmitterBatch(conf, _fullHashList, currentBolt, firstEmitter, secondEmitter);
        }else{
        	currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList, currentBolt, firstEmitter, secondEmitter);
        }

        // connecting with Killer
		if( getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}
		if (cp.getPrintOut() && _operatorChain.isBlocking()){
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
		}
	}

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
        	String inputComponentIndex = stormTupleRcv.getStringByField(StormComponent.COMP_INDEX); // getString(0);
            List<String> tuple = (List<String>) stormTupleRcv.getValueByField(StormComponent.TUPLE);  //getValue(1);
            String inputTupleHash = stormTupleRcv.getStringByField(StormComponent.HASH) ;//getString(2);

            if(processFinalAck(tuple, stormTupleRcv)){
            	return;
            }
                            
            processNonLastTuple(inputComponentIndex, tuple, inputTupleHash, stormTupleRcv, true);
                            
        }else{
        	String inputComponentIndex = stormTupleRcv.getStringByField(StormComponent.COMP_INDEX); // getString(0);
            String inputBatch = stormTupleRcv.getStringByField(StormComponent.TUPLE) ;//getString(1);
                                
            String[] wholeTuples = inputBatch.split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
            int batchSize = wholeTuples.length;
            for(int i=0; i<batchSize; i++){
             	//parsing
                String currentTuple = wholeTuples[i];
                String[] parts = currentTuple.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
                                    
                String inputTupleHash = null;
                String inputTupleString = null;
                if(parts.length == 1){
                   	//lastAck
                    inputTupleString = parts[0];
                }else{
                  	inputTupleHash = parts[0];
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
                  	processNonLastTuple(inputComponentIndex, tuple, inputTupleHash, stormTupleRcv, true);
                }else{
                  	processNonLastTuple(inputComponentIndex, tuple, inputTupleHash, stormTupleRcv, false);
                }                      
           }
        }
		getCollector().ack(stormTupleRcv);
	}
        
    private void processNonLastTuple(String inputComponentIndex, 
    		List<String> tuple, 
            String inputTupleHash,
            Tuple stormTupleRcv, 
            boolean isLastInBatch){
                  
    	boolean isFromFirstEmitter = false;
		BasicStore<ArrayList<String>> affectedStorage, oppositeStorage;
		ProjectOperator projPreAgg;
		if(_firstEmitterIndex.equals(inputComponentIndex)){
			//R update
			isFromFirstEmitter = true;
			affectedStorage = _firstSquallStorage;
			oppositeStorage = _secondSquallStorage;
			projPreAgg = _secondPreAggProj;
		}else if(_secondEmitterIndex.equals(inputComponentIndex)){
			//S update
			isFromFirstEmitter = false;
			affectedStorage = _secondSquallStorage;
			oppositeStorage = _firstSquallStorage;
			projPreAgg = _firstPreAggProj;
		}else{
			throw new RuntimeException("InputComponentName " + inputComponentIndex +
					" doesn't match neither " + _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");
		}

		//add the stormTuple to the specific storage
        if(affectedStorage instanceof AggregationStorage){
         	//For preaggregations, we have to update the storage, not to insert to it
          	affectedStorage.update(tuple, inputTupleHash);            	
        }else{
        	String inputTupleString = MyUtilities.tupleToString(tuple, getConf());
    		affectedStorage.insert(inputTupleHash, inputTupleString);
        }
        performJoin(stormTupleRcv,
        		tuple,
				inputTupleHash,
				isFromFirstEmitter,
				oppositeStorage,
				projPreAgg, 
				isLastInBatch);
    } 
        

	protected void performJoin(Tuple stormTupleRcv,
			List<String> tuple,
			String inputTupleHash,
			boolean isFromFirstEmitter,
			BasicStore<ArrayList<String>> oppositeStorage,
			ProjectOperator projPreAgg,
            boolean isLastInBatch){

		List<String> oppositeStringTupleList = oppositeStorage.access(inputTupleHash);

		if(oppositeStringTupleList!=null)
			for (int i = 0; i < oppositeStringTupleList.size(); i++) {
				// ValueOf is because of preaggregations, and it does not hurt in normal case
				String oppositeStringTuple= String.valueOf(oppositeStringTupleList.get(i));
				List<String> oppositeTuple= MyUtilities.stringToTuple(oppositeStringTuple, getComponentConfiguration());

				List<String> firstTuple, secondTuple;
				if(isFromFirstEmitter){
					firstTuple = tuple;
					secondTuple = oppositeTuple;
				}else{
					firstTuple = oppositeTuple;
					secondTuple = tuple;
				}

				List<String> outputTuple;
				//Before fixing preaggregations, here was instanceof BasicStore
				if(oppositeStorage instanceof AggregationStorage){
					//preaggregation
					outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);
				}else{
					outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple, _rightHashIndexes);					
				}

				if(projPreAgg != null){
					//preaggregation
					outputTuple = projPreAgg.process(outputTuple);				
				}

				applyOperatorsAndSend(stormTupleRcv, outputTuple, isLastInBatch);
			}
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
			return;
		}
		_numSentTuples++;
		printTuple(tuple);

		if(MyUtilities.isSending(getHierarchyPosition(), _aggBatchOutputMillis)){
			long timestamp = 0;
			if(MyUtilities.isCustomTimestampMode(getConf())){
				timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
            }
			tupleSend(tuple, stormTupleRcv, timestamp);
		}
        if(MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())){
        	long timestamp;
            if(MyUtilities.isManualBatchingMode(getConf())){
            	if(isLastInBatch){
            		timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
                    printTupleLatency(_numSentTuples - 1, timestamp);
                }
            }else{
            	timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
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
					if (tuples != null) {
						String columnDelimiter = MyUtilities.getColumnDelimiter(getConf());
						//	LOG.info("TUPLES: " + tuples + " - " + tuples.size());
						for(String tuple: tuples){
							tuple = tuple.replaceAll(" = ", columnDelimiter);
							//	LOG.info("BATCH SEND: tuple = " + tuple + " - (after processing: "+ MyUtilities.stringToTuple(tuple, _conf) + ")");
							//	LOG.info("tuple = " + tuple + "/"+ MyUtilities.stringToTuple(tuple, _conf));
							//	List<String> tupleSend = MyUtilities.stringToTuple(tuple, _conf);
							//	Collections.reverse(tupleSend);
							tupleSend(MyUtilities.stringToTuple(tuple, getConf()), null, 0);
						}
					}

					//clearing
					agg.clearStorage();

					_semAgg.release();
				}
			}
		}
	}

	// from IRichBolt
	@Override
	public Map<String,Object> getComponentConfiguration(){
		return getConf();
	}

	// from StormComponent interface
	@Override
	public String getInfoID() {
		String str = "DestinationStorage " + getID() + " has ID: " + getID();
		return str;
	}

	//HELPER
	
	@Override
	public long getNumSentTuples() {
		return _numSentTuples;
	}

	@Override
	public ChainOperator getChainOperator() {
		return _operatorChain;
	}

	@Override
	public PeriodicAggBatchSend getPeriodicAggBatch() {
		return _periodicAggBatch;
	}

}