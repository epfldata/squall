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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
import plan_runner.thetajoin.matrix_mapping.EquiMatrixAssignment;
import plan_runner.thetajoin.matrix_mapping.Matrix;
import plan_runner.thetajoin.matrix_mapping.OptimalPartition;
import plan_runner.thetajoin.predicate_analyser.PredicateAnalyser;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;
import plan_runner.visitors.PredicateCreateIndexesVisitor;
import plan_runner.visitors.PredicateUpdateIndexesVisitor;

public class StormThetaJoin extends StormBoltComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormThetaJoin.class);

	private TupleStorage _firstRelationStorage, _secondRelationStorage;
    private String _firstEmitterIndex, _secondEmitterIndex;

	private long _numSentTuples=0;
	
	private ChainOperator _operatorChain;
	

	//position to test for equality in first and second emitter
	//join params of current storage then other relation interchangably !!
	List<Integer> _joinParams;
	
	private Predicate _joinPredicate;
//	private OptimalPartition _partitioning;
	
	private List<Index> _firstRelationIndexes, _secondRelationIndexes;
	private List<Integer> _operatorForIndexes;
	private List<Object> _typeOfValueIndexed;
		
	private boolean _existIndexes = false;

	//for agg batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
	private long _aggBatchOutputMillis;
	
	protected SimpleDateFormat _format = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");	

	public StormThetaJoin(StormEmitter firstEmitter,
			StormEmitter secondEmitter,
			ComponentProperties cp,
            List<String> allCompNames,
			Predicate joinPredicate,
			int hierarchyPosition,
			TopologyBuilder builder,
			TopologyKiller killer,
			Config conf) {
		
		super(cp, allCompNames, hierarchyPosition, conf);

        _firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter.getName()));
        _secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter.getName()));		
		
		_aggBatchOutputMillis = cp.getBatchOutputMillis();

		int firstCardinality=SystemParameters.getInt(conf, firstEmitter.getName()+"_CARD");
		int secondCardinality=SystemParameters.getInt(conf, secondEmitter.getName()+"_CARD");

		int parallelism = SystemParameters.getInt(conf, getID()+"_PAR");

		//if(parallelism > 1 && distinct != null){
		//	throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
		//}

		_operatorChain = cp.getChainOperator();

		_joinPredicate = joinPredicate;

		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
		
		//Matrix makides = new Matrix(firstCardinality, secondCardinality);
		//_partitioning = new OptimalPartition (makides, parallelism);

		EquiMatrixAssignment _currentMappingAssignment= new EquiMatrixAssignment(firstCardinality, secondCardinality, parallelism, -1);
		String dim = _currentMappingAssignment.getMappingDimensions();
		LOG.info(getID()+ " Initial Dimensions is: "+ dim);
		
		currentBolt = MyUtilities.thetaAttachEmitterComponents(currentBolt, firstEmitter, secondEmitter,allCompNames,_currentMappingAssignment,conf);
		
		if( getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}
		if (cp.getPrintOut() && _operatorChain.isBlocking()){
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
		}

		_firstRelationStorage = new TupleStorage();
		_secondRelationStorage = new TupleStorage();

		if(_joinPredicate != null){
				createIndexes();
				_existIndexes = true;
		
			//TODO
			/*
			if(false){
				PredicateAnalyser predicateAnalyser = new PredicateAnalyser();
				Predicate modifiedPredicate = predicateAnalyser.analyse(_joinPredicate);
				if (modifiedPredicate == _joinPredicate) { //cannot create index
					_existIndexes = false;
				} else {
					_joinPredicate = modifiedPredicate;
					createIndexes();
					_existIndexes = true;
				}
			}
			 */		
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
		if(_firstTime && MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)){
			_periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis, this);
			_firstTime = false;
		}

		if (receivedDumpSignal(stormTupleRcv)) {
			MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
			return;
		}

		String inputComponentIndex=stormTupleRcv.getStringByField(StormComponent.COMP_INDEX); //getString(0);
        List<String> tuple = (List<String>)stormTupleRcv.getValueByField(StormComponent.TUPLE) ;//getValue(1);
		String inputTupleString=MyUtilities.tupleToString(tuple, getConf());
		String inputTupleHash=stormTupleRcv.getStringByField(StormComponent.HASH);//getString(2);

        if(processFinalAck(tuple, stormTupleRcv)){
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

		performJoin(stormTupleRcv,
				tuple,
				inputTupleHash,
				isFromFirstEmitter,
				oppositeIndexes,
				valuesToApplyOnIndex,
				oppositeStorage);

		getCollector().ack(stormTupleRcv);
	}
	
	private List<String> updateIndexes(Tuple stormTupleRcv, List<Index> affectedIndexes, int row_id){
		String inputComponentIndex = stormTupleRcv.getStringByField(StormComponent.COMP_INDEX) ; //getString(0); // Table name
		List<String> tuple = (List<String>) stormTupleRcv.getValueByField(StormComponent.TUPLE) ; //.getValue(1); //INPUT TUPLE
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
					affectedIndexes.get(i).put(row_id, Integer.parseInt(valuesToIndex.get(i)));
				}else if(typesOfValuesToIndex.get(i) instanceof Double ){
					affectedIndexes.get(i).put(row_id, Double.parseDouble(valuesToIndex.get(i)));
				}else if(typesOfValuesToIndex.get(i) instanceof Date){
					try {
						affectedIndexes.get(i).put(row_id, _format.parse(valuesToIndex.get(i)));
					} catch (ParseException e) {
						throw new RuntimeException("Parsing problem in StormThetaJoin.updatedIndexes " + e.getMessage());
					}
				}else if(typesOfValuesToIndex.get(i) instanceof String){
					affectedIndexes.get(i).put(row_id, valuesToIndex.get(i));
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
		
		for (int i = 0; i < oppositeIndexes.size(); i ++) {
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
					currentRowIds = currentOpposIndex.getValues(currentOperator, value );
				//Even if valueIndexed is at first time an integer with precomputation a*col +b, it become a double
				}else if(_typeOfValueIndexed.get(i) instanceof Double){
					currentRowIds = currentOpposIndex.getValues(currentOperator, Double.parseDouble(value) );
				}else if(_typeOfValueIndexed.get(i) instanceof Integer){
					currentRowIds = currentOpposIndex.getValues(currentOperator, Integer.parseInt(value) );
				}else if(_typeOfValueIndexed.get(i) instanceof Date){
					try {
						currentRowIds = currentOpposIndex.getValues(currentOperator, _format.parse(value));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}else{
					throw new RuntimeException("non supported type");
				}
			
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
        	long timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
        	printTupleLatency(_numSentTuples - 1, timestamp);
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

	// from IRichBolt
	@Override
	public Map<String,Object> getComponentConfiguration(){
		return getConf();
	}

	@Override
	public String getInfoID() {
		String str = "DestinationStorage " + getID() + " has ID: " + getID();
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