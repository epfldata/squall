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
import operators.ProjectOperator;
import operators.SelectOperator;
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
	private ProjectOperator _firstPreAggProj, _secondPreAggProj;
	private String _ID;
        private List<String> _compIds; // a sorted list of all the components
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
                        List<String> allCompNames,
			ChainOperator chain,
			SquallStorage firstPreAggStorage,
			SquallStorage secondPreAggStorage,
			ProjectOperator firstPreAggProj,
			ProjectOperator secondPreAggProj,
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
		_ID = componentName;
                _componentIndex = String.valueOf(allCompNames.indexOf(componentName));
                _firstEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter.getName()));
                _secondEmitterIndex = String.valueOf(allCompNames.indexOf(_secondEmitter.getName()));
		_batchOutputMillis = batchOutputMillis;

		int parallelism = SystemParameters.getInt(conf, _ID+"_PAR");

		//            if(parallelism > 1 && distinct != null){
		//                throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
		//            }

		_operatorChain = chain;

		_hashIndexes = hashIndexes;
		_hashExpressions = hashExpressions;
		_joinParams = MyUtilities.combineHashIndexes(_firstEmitter, _secondEmitter);

		_hierarchyPosition = hierarchyPosition;

		_fullHashList = fullHashList;
		InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);
		currentBolt = MyUtilities.attachEmitterCustom(conf, _fullHashList, currentBolt, firstEmitter, secondEmitter);

		if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		_printOut= printOut;
		if (_printOut && _operatorChain.isBlocking()){
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
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

			String inputComponentIndex=stormTupleRcv.getString(0);
                        List<String> tuple = (List<String>) stormTupleRcv.getValue(1);
                        String inputTupleString = MyUtilities.tupleToString(tuple, _conf);
			String inputTupleHash=stormTupleRcv.getString(2);

			if(MyUtilities.isFinalAck(tuple, _conf)){
				_numRemainingParents--;
				MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, stormTupleRcv, _collector, _periodicBatch);
				return;
			}

			boolean isFromFirstEmitter = false;
			SquallStorage affectedStorage, oppositeStorage;
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
			ProjectOperator projPreAgg){

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
			Values stormTupleSnd = MyUtilities.createTupleValues(tuple, _componentIndex,
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
				declarer.declare(new Fields("CompIndex", "Tuple", "Hash") );
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
		public List<Integer> getHashIndexes(){
			return _hashIndexes;
		}

	@Override
		public List<ValueExpression> getHashExpressions() {
			return _hashExpressions;
		}

	@Override
		public String getInfoID() {
			String str = "DestinationStorage " + _ID + " has ID: " + _ID;
			return str;
		}

}
