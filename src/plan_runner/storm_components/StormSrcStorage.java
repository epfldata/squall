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
import plan_runner.operators.ProjectOperator;
import plan_runner.storage.BasicStore;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicBatchSend;
import plan_runner.utilities.SystemParameters;

public class StormSrcStorage extends BaseRichBolt implements StormEmitter, StormComponent{
	private static Logger LOG = Logger.getLogger(StormSrcStorage.class);
	private static final long serialVersionUID = 1L;

	private String _componentName;
	private String _tableName;
        private String _componentIndex; //a unique index in a list of all the components
                            //used as a shorter name, to save some network traffic
                            //it's of type int, but we use String to save more space
	private boolean _isFromFirstEmitter; // receive R updates
	private boolean _printOut;
	private int _hierarchyPosition=INTERMEDIATE;
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;
        private List<Integer> _rightHashIndexes; // hashIndexes from the righ parent

	private ChainOperator _operatorChain;

	private BasicStore<ArrayList<String>> _joinStorage;
	private ProjectOperator _preAggProj;

	private StormSrcHarmonizer _harmonizer;
	private OutputCollector _collector;
	private int _numSentTuples;
	private String _ID;
	private Map _conf;

        private String _firstEmitterIndex, _secondEmitterIndex; 
                    //a unique id of the components which sends to StormSrcJoin

	private int _numRemainingParents;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicBatchSend _periodicBatch;
	private long _batchOutputMillis;

	public StormSrcStorage(StormEmitter firstEmitter,
                        StormEmitter secondEmitter,
                        ComponentProperties cp,
                        List<String> allCompNames,
			StormSrcHarmonizer harmonizer,
			boolean isFromFirstEmitter,
			BasicStore<ArrayList<String>> preAggStorage,
			ProjectOperator preAggProj,
			int hierarchyPosition,
			TopologyBuilder builder,
			TopologyKiller killer,
			Config conf) {

               _firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter.getName()));
               _secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter.getName()));

		_conf = conf;
		_componentName = cp.getName();
                _componentIndex = String.valueOf(allCompNames.indexOf(_componentName));
		_tableName = (isFromFirstEmitter ? firstEmitter.getName(): secondEmitter.getName());
		_isFromFirstEmitter=isFromFirstEmitter;
		_harmonizer = harmonizer;
		_batchOutputMillis = cp.getBatchOutputMillis();

		_operatorChain = cp.getChainOperator();
		_hashIndexes=cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
		_hierarchyPosition=hierarchyPosition;
                _rightHashIndexes = cp.getParents()[1].getHashIndexes();

		_printOut = cp.getPrintOut();

		int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
		_ID = _componentName + "_" + _tableName;
		InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);
		currentBolt.fieldsGrouping(_harmonizer.getID(), new Fields("Hash"));

		if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		if (_printOut && _operatorChain.isBlocking()){
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
		}

		_joinStorage = preAggStorage;
		_preAggProj = preAggProj;
	}

	// from IRichBolt
	@Override
		public void cleanup() {
			// TODO Auto-generated method stub

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

                        //inside StormSrcJoin, we have inputComponentName, not inputComponentIndex
			String inputComponentIndex=stormTupleRcv.getString(0);
                        List<String> tuple = (List<String>)stormTupleRcv.getValue(1);
			String inputTupleString=MyUtilities.tupleToString(tuple, _conf);
			String inputTupleHash=stormTupleRcv.getString(2);

			if(MyUtilities.isFinalAck(tuple, _conf)){
				_numRemainingParents--;
				MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, stormTupleRcv, _collector, _periodicBatch);
				return;
			}

			if((_isFromFirstEmitter && (inputComponentIndex.equals(_firstEmitterIndex))) ||
                            (!_isFromFirstEmitter && (inputComponentIndex.equals(_secondEmitterIndex)))){//add the tuple into the datastructure!!
				_joinStorage.insert(inputTupleHash, inputTupleString);
			} else {//JOIN
				List<String> oppositeTupleStringList = _joinStorage.access(inputTupleHash);

				// do stuff
				if(oppositeTupleStringList!=null)
					for (int i = 0; i < oppositeTupleStringList.size(); i++) {
						String oppositeTupleString= oppositeTupleStringList.get(i);
						List<String> oppositeTuple= MyUtilities.stringToTuple(oppositeTupleString, _conf);

						List<String> firstTuple, secondTuple;
						if(_isFromFirstEmitter){
							//we receive R updates in the Storage which is responsible for S
							firstTuple=oppositeTuple;
							secondTuple=tuple;
						}else{
							firstTuple=tuple;
							secondTuple=oppositeTuple;    
						}

						List<String> outputTuple;
						if(_joinStorage instanceof BasicStore){

							outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple, _rightHashIndexes);
						}else{
							outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);
						}

						if(_preAggProj != null){
							outputTuple = _preAggProj.process(outputTuple);
						}

						applyOperatorsAndSend(stormTupleRcv, outputTuple);
					}
			}
			_collector.ack(stormTupleRcv);
		}

	private void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple){
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

	@Override
		public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
			_collector = collector;
			_numRemainingParents = MyUtilities.getNumParentTasks(tc, _harmonizer);
		}

	@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			if(_hierarchyPosition!=FINAL_COMPONENT){ // then its an intermediate stage not the final one
				declarer.declare(new Fields("CompIndex", "Tuple", "Hash") );
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
				if((_operatorChain!=null) &&_operatorChain.isBlocking()){
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
		public String getID() {
			return _ID;
		}

	// from StormEmitter interface
	@Override
		public String[] getEmitterIDs(){
			return new String[]{_ID};
		}

	@Override
		public String getName() {
			return _componentName;
		}

	@Override
		public String getInfoID() {
			String name = _componentName +":" + _tableName;
			String str = "SourceStorage " + name + " has ID: "+ _ID;
			return str;
		}

}
