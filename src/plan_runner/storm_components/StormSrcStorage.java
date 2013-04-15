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
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;

public class StormSrcStorage extends StormBoltComponent{
	private static Logger LOG = Logger.getLogger(StormSrcStorage.class);
	private static final long serialVersionUID = 1L;

	private String _tableName;
	private boolean _isFromFirstEmitter; // receive R updates

    //a unique id of the components which sends to StormSrcJoin	
    private String _firstEmitterIndex, _secondEmitterIndex; 	
    private List<Integer> _rightHashIndexes; // hashIndexes from the right parent

    private String _full_ID; // Contains the name of the component and tableName
    
	private ChainOperator _operatorChain;

	private BasicStore<ArrayList<String>> _joinStorage;
	private ProjectOperator _preAggProj;
	private StormSrcHarmonizer _harmonizer;

	private int _numSentTuples;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
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
		super(cp, allCompNames, hierarchyPosition, conf);

        _firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter.getName()));
        _secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter.getName()));

		_tableName = (isFromFirstEmitter ? firstEmitter.getName(): secondEmitter.getName());
		_isFromFirstEmitter=isFromFirstEmitter;
		_harmonizer = harmonizer;
		_batchOutputMillis = cp.getBatchOutputMillis();

		_operatorChain = cp.getChainOperator();
        _rightHashIndexes = cp.getParents()[1].getHashIndexes();

		int parallelism = SystemParameters.getInt(conf, getID()+"_PAR");
        _full_ID = getID() + "_" + _tableName;
		InputDeclarer currentBolt = builder.setBolt(_full_ID, this, parallelism);
		currentBolt.fieldsGrouping(_harmonizer.getID(), new Fields("Hash"));

		if( getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		if (cp.getPrintOut() && _operatorChain.isBlocking()){
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
		}

		_joinStorage = preAggStorage;
		_preAggProj = preAggProj;
	}

	// from IRichBolt
	@Override
	public void execute(Tuple stormTupleRcv) {
		if(_firstTime && MyUtilities.isAggBatchOutputMode(_batchOutputMillis)){
			_periodicAggBatch = new PeriodicAggBatchSend(_batchOutputMillis, this);
			_firstTime = false;
		}

		if (receivedDumpSignal(stormTupleRcv)) {
			MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
			return;
		}

        //inside StormSrcJoin, we have inputComponentName, not inputComponentIndex
		String inputComponentIndex=stormTupleRcv.getString(0);
        List<String> tuple = (List<String>)stormTupleRcv.getValue(1);
		String inputTupleString=MyUtilities.tupleToString(tuple, getConf());
		String inputTupleHash=stormTupleRcv.getString(2);

		if(processFinalAck(tuple, stormTupleRcv)){
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
					List<String> oppositeTuple= MyUtilities.stringToTuple(oppositeTupleString, getConf());

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
		getCollector().ack(stormTupleRcv);
	}

	private void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple){
		if(MyUtilities.isAggBatchOutputMode(_batchOutputMillis)){
			try {
				_semAgg.acquire();
			} catch (InterruptedException ex) {}
		}
		tuple = _operatorChain.process(tuple);
		if(MyUtilities.isAggBatchOutputMode(_batchOutputMillis)){
			_semAgg.release();
		}

		if(tuple == null){
			return;
		}
		_numSentTuples++;
		printTuple(tuple);

		if(MyUtilities.isSending(getHierarchyPosition(), _batchOutputMillis)){
			if(MyUtilities.isCustomTimestampMode(getConf())){
				tupleSend(tuple, stormTupleRcv, stormTupleRcv.getLong(3));
            }else{
            	tupleSend(tuple, stormTupleRcv, 0);
            }
		}
        if(MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())){
        	printTupleLatency(_numSentTuples - 1, stormTupleRcv.getLong(3));
        }
	}

	@Override
	public void aggBatchSend(){
		if(MyUtilities.isAggBatchOutputMode(_batchOutputMillis)){
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

	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		super.prepare(map, tc, collector);
		super.setNumRemainingParents(MyUtilities.getNumParentTasks(tc, _harmonizer));
	}

	// from StormEmitter interface
	@Override
	public String getName() {
		return _full_ID;
	}

	@Override
	public String getInfoID() {
		String str = "SourceStorage " + _full_ID + " has ID: " + getID();
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
