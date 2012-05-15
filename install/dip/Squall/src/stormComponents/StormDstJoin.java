package stormComponents;

import backtype.storm.Config;
import java.util.ArrayList;
import java.util.Map;

import utilities.MyUtilities;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import expressions.ValueExpression;
import java.util.List;
import java.util.concurrent.Semaphore;
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import utilities.SystemParameters;
import storage.SquallStorage;

import org.apache.log4j.Logger;
import stormComponents.synchronization.TopologyKiller;
import utilities.PeriodicBatchSend;

public class StormDstJoin extends BaseRichBolt implements StormJoin, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDstJoin.class);

	private int _hierarchyPosition=INTERMEDIATE;

	private StormEmitter _firstEmitter, _secondEmitter;
	private SquallStorage _firstSquallStorage, _secondSquallStorage;
	private ProjectionOperator _firstPreAggProj, _secondPreAggProj;
	private String _componentName;
	private int _ID;

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

	//for load-balancing
	private List<String> _fullHashList;

	//for No ACK: the total number of tasks of all the parent compoonents
	private int _numRemainingParents;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicBatchSend _periodicBatch;
	private long _batchOutputMillis;

	public StormDstJoin(StormEmitter firstEmitter,
			StormEmitter secondEmitter,
			String componentName,
			SelectionOperator selection,
			DistinctOperator distinct,
			ProjectionOperator projection,
			AggregateOperator aggregation,
			SquallStorage firstPreAggStorage,
			SquallStorage secondPreAggStorage,
			ProjectionOperator firstPreAggProj,
			ProjectionOperator secondPreAggProj,
			List<Integer> hashIndexes,
			List<ValueExpression> hashExpressions,
			int hierarchyPosition,
			boolean printOut,
			long batchOutputMillis,
			List<String> fullHashList,
			TopologyBuilder builder,
			TopologyKiller killer,
			Config conf) {
		_conf = conf;
		_firstEmitter = firstEmitter;
		_secondEmitter = secondEmitter;
		_componentName = componentName;
		_batchOutputMillis = batchOutputMillis;

		int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");

		//            if(parallelism > 1 && distinct != null){
		//                throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
		//            }

		_operatorChain = new ChainOperator(selection, distinct, projection, aggregation);

		_hashIndexes = hashIndexes;
		_hashExpressions = hashExpressions;
		_joinParams = MyUtilities.combineHashIndexes(_firstEmitter, _secondEmitter);

		_hierarchyPosition = hierarchyPosition;

		_fullHashList = fullHashList;
		_ID=MyUtilities.getNextTopologyId();
		InputDeclarer currentBolt = builder.setBolt(Integer.toString(_ID), this, parallelism);
		currentBolt = MyUtilities.attachEmitterCustom(conf, _fullHashList, currentBolt, firstEmitter, secondEmitter);

		if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		_printOut= printOut;
		if (_printOut && _operatorChain.isBlocking()){
			currentBolt.allGrouping(Integer.toString(killer.getID()), SystemParameters.DUMP_RESULTS_STREAM);
		}

		_firstSquallStorage = firstPreAggStorage;
		_secondSquallStorage = secondPreAggStorage;

		_firstPreAggProj = firstPreAggProj;
		_secondPreAggProj = secondPreAggProj;

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

			String inputComponentName=stormTupleRcv.getString(0);
                        List<String> tuple = (List<String>) stormTupleRcv.getValue(1);
                        String inputTupleString = MyUtilities.tupleToString(tuple, _conf);
			String inputTupleHash=stormTupleRcv.getString(2);

			if(MyUtilities.isFinalAck(tuple, _conf)){
				_numRemainingParents--;
				MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, stormTupleRcv, _collector, _periodicBatch);
				return;
			}

			String firstEmitterName = _firstEmitter.getName();
			String secondEmitterName = _secondEmitter.getName();

			boolean isFromFirstEmitter = false;
			SquallStorage affectedStorage, oppositeStorage;
			ProjectionOperator projPreAgg;
			if(firstEmitterName.equals(inputComponentName)){
				//R update
				isFromFirstEmitter = true;
				affectedStorage = _firstSquallStorage;
				oppositeStorage = _secondSquallStorage;
				projPreAgg = _secondPreAggProj;
			}else if(secondEmitterName.equals(inputComponentName)){
				//S update
				isFromFirstEmitter = false;
				affectedStorage = _secondSquallStorage;
				oppositeStorage = _firstSquallStorage;
				projPreAgg = _firstPreAggProj;
			}else{
				throw new RuntimeException("InputComponentName " + inputComponentName +
						" doesn't match neither " + firstEmitterName + " nor " + secondEmitterName + ".");
			}

			//add the stormTuple to the specific storage
			affectedStorage.put(inputTupleHash, inputTupleString);

			performJoin(stormTupleRcv,
					tuple,
					inputTupleHash,
					isFromFirstEmitter,
					oppositeStorage,
					projPreAgg);

			_collector.ack(stormTupleRcv);
		}

	protected void performJoin(Tuple stormTupleRcv,
			List<String> tuple,
			String inputTupleHash,
			boolean isFromFirstEmitter,
			SquallStorage oppositeStorage,
			ProjectionOperator projPreAgg){

		List<String> oppositeStringTupleList = (ArrayList<String>)oppositeStorage.get(inputTupleHash);

		if(oppositeStringTupleList!=null)
			for (int i = 0; i < oppositeStringTupleList.size(); i++) {
				String oppositeStringTuple= oppositeStringTupleList.get(i);
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
				if(oppositeStorage instanceof SquallStorage){
					outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple, _joinParams);
				}else{
					outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);
				}

				if(projPreAgg != null){
					outputTuple = projPreAgg.process(outputTuple);
				}

				applyOperatorsAndSend(stormTupleRcv, outputTuple);
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
			tupleSend(tuple, stormTupleRcv);
		}
	}

	@Override
		public void tupleSend(List<String> tuple, Tuple stormTupleRcv) {
			Values stormTupleSnd = MyUtilities.createTupleValues(tuple, _componentName,
					_hashIndexes, _hashExpressions, _conf);
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
							tupleSend(MyUtilities.stringToTuple(tuple, _conf), null);
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
				List<String> outputFields= new ArrayList<String>();
				outputFields.add("TableName");
				outputFields.add("Tuple");
				outputFields.add("Hash");
				declarer.declare(new Fields(outputFields) );
			}else{
				if(!MyUtilities.isAckEveryTuple(_conf)){
					declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
				}
			}
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
				if((_operatorChain!=null) && _operatorChain.isBlocking()){
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

	private boolean receivedDumpSignal(Tuple stormTuple) {
		return stormTuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.DUMP_RESULTS_STREAM);
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
			String str = "DestinationStorage " + _componentName + " has ID: " + _ID;
			return str;
		}

}
