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
import gnu.trove.list.array.TIntArrayList;
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
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.Predicate;
import plan_runner.storage.TupleStorage;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.thetajoin.indexes.Index;
import plan_runner.thetajoin.matrix_mapping.Matrix;
import plan_runner.thetajoin.matrix_mapping.OptimalPartition;
import plan_runner.thetajoin.predicate_analyser.PredicateAnalyser;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicBatchSend;
import plan_runner.utilities.SystemParameters;
import plan_runner.visitors.PredicateCreateIndexesVisitor;
import plan_runner.visitors.PredicateUpdateIndexesVisitor;

public class StormThetaJoin extends BaseRichBolt implements StormJoin, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormThetaJoin.class);

	private int _hierarchyPosition=INTERMEDIATE;

	private StormEmitter _firstEmitter, _secondEmitter;
	private TupleStorage _firstRelationStorage, _secondRelationStorage;
	
	private String _ID;
        private String _componentIndex; //a unique index in a list of all the components
                            //used as a shorter name, to save some network traffic
                            //it's of type int, but we use String to save more space
        private String _firstEmitterIndex, _secondEmitterIndex;

	private int _numSentTuples=0;
	private boolean _printOut;

	private ChainOperator _operatorChain;
	private OutputCollector _collector;
	private Map _conf;

	//position to test for equality in first and second emitter
	//join params of current storage then other relation interchangably !!
	List<Integer> _joinParams;

	//output has hash formed out of these indexes
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;
	
	private Predicate _joinPredicate;
	private OptimalPartition _partitioning;
	
	private List<Index> _firstRelationIndexes, _secondRelationIndexes;
	private List<Integer> _operatorForIndexes;
	private List<Object> _typeOfValueIndexed;
		
	private boolean _existIndexes = false;

	//for No ACK: the total number of tasks of all the parent compoonents
	private int _numRemainingParents;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicBatchSend _periodicBatch;
	private long _batchOutputMillis;
        
        //for Send and Wait mode
        private double _totalLatency;        

	public StormThetaJoin(StormEmitter firstEmitter,
			StormEmitter secondEmitter,
			ComponentProperties cp,
                        List<String> allCompNames,
			Predicate joinPredicate,
			int hierarchyPosition,
			TopologyBuilder builder,
			TopologyKiller killer,
			Config conf) {
		_conf = conf;
		_firstEmitter = firstEmitter;
		_secondEmitter = secondEmitter;
		_ID = cp.getName();
                _componentIndex = String.valueOf(allCompNames.indexOf(_ID));
		_batchOutputMillis = cp.getBatchOutputMillis();
		
                _firstEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter.getName()));
                _secondEmitterIndex = String.valueOf(allCompNames.indexOf(_secondEmitter.getName()));

		int firstCardinality=SystemParameters.getInt(conf, firstEmitter.getName()+"_CARD");
		int secondCardinality=SystemParameters.getInt(conf, secondEmitter.getName()+"_CARD");

		int parallelism = SystemParameters.getInt(conf, _ID+"_PAR");

		//            if(parallelism > 1 && distinct != null){
		//                throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
		//            }

		_operatorChain = cp.getChainOperator();

		_hashIndexes = cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
		_joinPredicate = joinPredicate;

		_hierarchyPosition = hierarchyPosition;

		InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);
		
		Matrix makides = new Matrix(firstCardinality, secondCardinality);
		_partitioning = new OptimalPartition (makides, parallelism);
		
		currentBolt = MyUtilities.thetaAttachEmitterComponents(currentBolt, firstEmitter, secondEmitter,allCompNames,_partitioning,conf);
		
		if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		_printOut= cp.getPrintOut();
		if (_printOut && _operatorChain.isBlocking()){
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
		}

		_firstRelationStorage = new TupleStorage();
		_secondRelationStorage = new TupleStorage();


		if(_joinPredicate != null){
			PredicateAnalyser predicateAnalyser = new PredicateAnalyser();
			
			Predicate modifiedPredicate = predicateAnalyser.analyse(_joinPredicate);
	
			
			if (modifiedPredicate == _joinPredicate) { //cannot create index
				_existIndexes = false;
			} else {
				_joinPredicate = modifiedPredicate;
				createIndexes();
				_existIndexes = true;
			}
		}else{
			_existIndexes = false;
		}
		

	}
	
	private void createIndexes(){
		PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
		
		_joinPredicate.accept(visitor);
		
		_firstRelationIndexes = new ArrayList<Index>(visitor._firstRelationIndexes);
		_secondRelationIndexes = new ArrayList<Index>(visitor._secondRelationIndexes);
		_operatorForIndexes = new ArrayList<Integer>(visitor._operatorForIndexes);
		_typeOfValueIndexed = new ArrayList<Object>(visitor._typeOfValueIndexed);
		
	}
	

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

		String inputComponentIndex=stormTupleRcv.getString(0);
                List<String> tuple = (List<String>)stormTupleRcv.getValue(1);
		String inputTupleString=MyUtilities.tupleToString(tuple, _conf);
		String inputTupleHash=stormTupleRcv.getString(2);

		if(MyUtilities.isFinalAck(tuple, _conf)){
			_numRemainingParents--;
			MyUtilities.processFinalAck(_numRemainingParents, 
                                _hierarchyPosition, 
                                _conf,
                                stormTupleRcv, 
                                _collector, 
                                _periodicBatch);
			return;
		}

		boolean isFromFirstEmitter = false;
		
		TupleStorage affectedStorage, oppositeStorage;
		List<Index> affectedIndexes, oppositeIndexes;
		
		if(_firstEmitterIndex.equals(inputComponentIndex)){
			//R update
			isFromFirstEmitter = true;
			affectedStorage = _firstRelationStorage;
			oppositeStorage = _secondRelationStorage;
			affectedIndexes = _firstRelationIndexes;
			oppositeIndexes = _secondRelationIndexes;
		}else if(_secondEmitterIndex.equals(inputComponentIndex)){
			//S update
			isFromFirstEmitter = false;
			affectedStorage = _secondRelationStorage;
			oppositeStorage = _firstRelationStorage;
			affectedIndexes = _secondRelationIndexes;
			oppositeIndexes = _firstRelationIndexes;
		}else{
			throw new RuntimeException("InputComponentName " + inputComponentIndex +
					" doesn't match neither " + _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");
		}

		//add the stormTuple to the specific storage
		int row_id = affectedStorage.insert(inputTupleString);

		List<String> valuesToApplyOnIndex = null;
		
		if(_existIndexes){
			valuesToApplyOnIndex = updateIndexes(stormTupleRcv, affectedIndexes, row_id);
		}

		performJoin( stormTupleRcv,
				tuple,
				inputTupleHash,
				isFromFirstEmitter,
				oppositeIndexes,
				valuesToApplyOnIndex,
				oppositeStorage);

		_collector.ack(stormTupleRcv);
	}
	
	private List<String> updateIndexes(Tuple stormTupleRcv, List<Index> affectedIndexes, int row_id){

		String inputComponentIndex = stormTupleRcv.getString(0); // Table name
		List<String> tuple = (List<String>) stormTupleRcv.getValue(1); //INPUT TUPLE
		// Get a list of tuple attributes and the key value
		
		boolean comeFromFirstEmitter;
		
		if(inputComponentIndex.equals(_firstEmitterIndex)){
			comeFromFirstEmitter = true;
		}else{
			comeFromFirstEmitter = false;
		}

		PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(comeFromFirstEmitter, tuple);
		_joinPredicate.accept(visitor);

		List<String> valuesToIndex = new ArrayList<String>(visitor._valuesToIndex);
		List<Object> typesOfValuesToIndex = new ArrayList<Object>(visitor._typesOfValuesToIndex);
		
		for(int i=0; i<affectedIndexes.size(); i++){
			if(typesOfValuesToIndex.get(i) instanceof Integer ){
				affectedIndexes.get(i).put(Integer.parseInt(valuesToIndex.get(i)), row_id);
			}else if(typesOfValuesToIndex.get(i) instanceof Double ){
				affectedIndexes.get(i).put(Double.parseDouble(valuesToIndex.get(i)), row_id);
			}else if(typesOfValuesToIndex.get(i) instanceof String){
				affectedIndexes.get(i).put(valuesToIndex.get(i), row_id);
			}else{
				throw new RuntimeException("non supported type");
			}
			
		}
		
		return valuesToIndex;
		
	}
	

	protected void performJoin(Tuple stormTupleRcv,
			List<String> tuple,
			String inputTupleHash,
			boolean isFromFirstEmitter,
			List<Index> oppositeIndexes,
			List<String> valuesToApplyOnIndex,
			TupleStorage oppositeStorage){

		TupleStorage tuplesToJoin = new TupleStorage();
		selectTupleToJoin(oppositeStorage, oppositeIndexes, isFromFirstEmitter, valuesToApplyOnIndex, tuplesToJoin);
		join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin);
	}
	
	private void selectTupleToJoin(TupleStorage oppositeStorage,
			List<Index> oppositeIndexes, boolean isFromFirstEmitter,
			List<String> valuesToApplyOnIndex, TupleStorage tuplesToJoin){
				
		if(!_existIndexes ){
			tuplesToJoin.copy(oppositeStorage);
			return;
		}
		
		TIntArrayList rowIds = new TIntArrayList();
		// If there is atleast one index (so we have single join conditions with 1 index per condition)
		// Get the row indices in the storage of the opposite relation that
		// satisfy each join condition (equijoin / inequality)
		// Then take the intersection of the returned row indices since each join condition
		// is separated by AND
		
		for (int i = 0; i < oppositeIndexes.size(); i ++) 
		{
			TIntArrayList currentRowIds = null;

			Index currentOpposIndex = oppositeIndexes.get(i);
			String value = valuesToApplyOnIndex.get(i);
				
			int currentOperator = _operatorForIndexes.get(i);
			// Switch inequality operator if the tuple coming is from the other relation
			if (isFromFirstEmitter){
				int operator = currentOperator;
				
				if (operator == ComparisonPredicate.GREATER_OP){
					currentOperator = ComparisonPredicate.LESS_OP;
				}else if (operator == ComparisonPredicate.NONGREATER_OP){
					currentOperator = ComparisonPredicate.NONLESS_OP;
				}else if (operator == ComparisonPredicate.LESS_OP){
					currentOperator = ComparisonPredicate.GREATER_OP;
				}else if (operator == ComparisonPredicate.NONLESS_OP){
					currentOperator = ComparisonPredicate.NONGREATER_OP;	
					
				//then it is an equal or not equal so we dont switch the operator
				}else{
					currentOperator = operator;		
				}	
			}

			// Get the values from the index (check type first)
			if(_typeOfValueIndexed.get(i) instanceof String){
				currentRowIds = currentOpposIndex.getValues(value, currentOperator );
			//Even if valueIndexed is at first time an integer with precomputation a*col +b, it become a double
			}else if(_typeOfValueIndexed.get(i) instanceof Double){
				currentRowIds = currentOpposIndex.getValues(Double.parseDouble(value), currentOperator );
			}else if(_typeOfValueIndexed.get(i) instanceof Integer){
				currentRowIds = currentOpposIndex.getValues(Integer.parseInt(value), currentOperator );
			}else{
				throw new RuntimeException("non supported type");
			}
				
			
			//System.out.println("currentIDS:"+currentRowIds);
			
			// Compute the intersection
			// TODO: Search only within the ids that are in rowIds from previous join conditions
			// If nothing returned (and since we want intersection), no need to proceed.
			if (currentRowIds == null)
				return ;
			
			// If it's the first index, add everything. Else keep the intersection
			if (i == 0)
				rowIds.addAll(currentRowIds);				
			else
				rowIds.retainAll(currentRowIds);
			
			// If empty after intersection, return
			if(rowIds.isEmpty())
				return ;
		}
		
	
		//generate tuplestorage
		for(int i = 0; i < rowIds.size(); i++){
			int id = rowIds.get(i);
			tuplesToJoin.insert(oppositeStorage.get(id));
			
		}
		
	}
	
	private void join(Tuple stormTuple, 
            List<String> tuple,
            boolean isFromFirstEmitter,
            TupleStorage oppositeStorage){
		
		if (oppositeStorage == null || oppositeStorage.size() == 0) {
			return;
		}
 
		for (int i=0; i<oppositeStorage.size(); i++) {
			String oppositeTupleString = oppositeStorage.get(i);
			
			List<String> oppositeTuple= MyUtilities.stringToTuple(oppositeTupleString, getComponentConfiguration());
			List<String> firstTuple, secondTuple;
			if(isFromFirstEmitter){
			    firstTuple = tuple;
			    secondTuple = oppositeTuple;
			}else{
			    firstTuple = oppositeTuple;
			    secondTuple = tuple;
			}
			
			// Check joinCondition
			//if existIndexes == true, the join condition is already checked before
			if (_joinPredicate == null || _existIndexes || _joinPredicate.test(firstTuple, secondTuple)) { //if null, cross product
				
				// Create the output tuple by omitting the oppositeJoinKeys (ONLY for equi-joins since they are added 
				// by the first relation), if any (in case of cartesian product there are none)
				List<String> outputTuple = null;
				
				
				// Cartesian product - Outputs all attributes
				outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);
				

				applyOperatorsAndSend(stormTuple, outputTuple);

			}
		}	
		
	}

	protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple){
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
			return;
		}
		_numSentTuples++;
		printTuple(tuple);

		if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis)){
                    if(MyUtilities.isCustomTimestampMode(_conf)){
                        tupleSend(tuple, stormTupleRcv, stormTupleRcv.getLong(3));
                    }else{
                        tupleSend(tuple, stormTupleRcv, 0);
                    }
		}
                if(MyUtilities.isPrintLatency(_hierarchyPosition, _conf)){
                    printTupleLatency(_numSentTuples - 1, stormTupleRcv.getLong(3));
                }
	}

	@Override
		public void tupleSend(List<String> tuple, Tuple stormTupleRcv, long timestamp) {
			Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 
                                timestamp,
                                _componentIndex,
				_hashIndexes, 
                                _hashExpressions, 
                                _conf);
			MyUtilities.sendTuple(stormTupleSnd, stormTupleRcv, _collector, _conf);
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

	// from IRichBolt
	@Override
		public void cleanup() {
			// TODO Auto-generated method stub

		}

	@Override
		public Map<String,Object> getComponentConfiguration(){
			return _conf;
		}

	@Override
		public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
			_collector=collector;
			_numRemainingParents = MyUtilities.getNumParentTasks(tc, _firstEmitter, _secondEmitter);
		}

	@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			if(_hierarchyPosition!=FINAL_COMPONENT){ // then its an intermediate stage not the final one
                            if(MyUtilities.isCustomTimestampMode(_conf)){
                                declarer.declareStream(SystemParameters.DATA_STREAM, new Fields("CompIndex", "Tuple", "Hash", "Timestamp"));
                            }else{
                                declarer.declareStream(SystemParameters.DATA_STREAM, new Fields("CompIndex", "Tuple", "Hash"));
                            }
			}else{
				if(!MyUtilities.isAckEveryTuple(_conf)){
					declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
				}
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
				if((_operatorChain!=null) && _operatorChain.isBlocking()){
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
			String str = "DestinationStorage " + _ID + " has ID: " + _ID;
			return str;
		}

}
