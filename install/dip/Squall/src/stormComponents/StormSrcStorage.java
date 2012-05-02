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
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.Operator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import utilities.SystemParameters;

import org.apache.log4j.Logger;
import stormComponents.synchronization.TopologyKiller;

public class StormSrcStorage extends BaseRichBolt implements StormEmitter, StormComponent{
	private static Logger LOG = Logger.getLogger(StormSrcStorage.class);
	private static final long serialVersionUID = 1L;

        private String _componentName;
        private String _tableName;
	private boolean _isFromFirstEmitter; // receive R updates
        private boolean _printOut;
        private int _hierarchyPosition=INTERMEDIATE;
        private List<Integer> _joinParams; //join params of current storage then other relation interchangably !!
        private List<Integer> _hashIndexes;
        private List<ValueExpression> _hashExpressions;

	private ChainOperator _operatorChain;

	private JoinStorage _joinStorage;
        private ProjectionOperator _preAggProj;

        private StormSrcHarmonizer _harmonizer;
	private OutputCollector _collector;
        private int _numSentTuples;
	private int _ID;
	private Map _conf;

        private int _numRemainingParents;

	public StormSrcStorage(String componentName,
                String tableName,
                StormSrcHarmonizer harmonizer,
                List<Integer> joinParams,
                boolean isFromFirstEmitter,
                SelectionOperator selection,
                ProjectionOperator projection,
                AggregateOperator aggregation,
                JoinStorage preAggStorage,
                ProjectionOperator preAggProj,
                List<Integer> hashIndexes,
                List<ValueExpression> hashExpressions,
                int hierarchyPosition,
                boolean printOut,
                TopologyBuilder builder,
                TopologyKiller killer,
                Config conf) {

            _conf = conf;
            _componentName = componentName;
            _tableName = tableName;
            _joinParams= joinParams;
            _isFromFirstEmitter=isFromFirstEmitter;
            _harmonizer = harmonizer;

            _operatorChain = new ChainOperator(selection, projection, aggregation);
            _hashIndexes=hashIndexes;
            _hashExpressions = hashExpressions;
            _hierarchyPosition=hierarchyPosition;

            _printOut = printOut;

            int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
            _ID = MyUtilities.getNextTopologyId();
            InputDeclarer currentBolt = builder.setBolt(Integer.toString(_ID), this, parallelism);
            currentBolt.fieldsGrouping(Integer.toString(_harmonizer.getID()), new Fields("Hash"));

            if (_printOut && _operatorChain.isBlocking()){
                currentBolt.allGrouping(Integer.toString(killer.getID()), SystemParameters.DumpResults);
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
                if (receivedDumpSignal(stormTupleRcv)) {
                    MyUtilities.dumpSignal(this, stormTupleRcv, _collector);
                    return;
                }
		
		String inputComponentName=stormTupleRcv.getString(0);
		String inputTupleString=stormTupleRcv.getString(1);   //INPUT TUPLE
		String inputTupleHash=stormTupleRcv.getString(2);

                if(MyUtilities.isFinalAck(inputTupleString, _conf)){
                    _numRemainingParents--;
                    MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, stormTupleRcv, _collector);
                    return;
                }

		if(_tableName.equals(inputComponentName)) {//add the tuple into the datastructure!!
                        _joinStorage.put(inputTupleHash, inputTupleString);
		} else {//JOIN
			List<String> affectedTuple = MyUtilities.stringToTuple(inputTupleString, _conf);
			List<String> oppositeTupleStringList = _joinStorage.get(inputTupleHash);

                        // do stuff
			if(oppositeTupleStringList!=null)
				for (int i = 0; i < oppositeTupleStringList.size(); i++) {
					String oppositeTupleString= oppositeTupleStringList.get(i);
					List<String> oppositeTuple= MyUtilities.stringToTuple(oppositeTupleString, _conf);

                                        List<String> firstTuple, secondTuple;
                                        if(_isFromFirstEmitter){
                                            //we receive R updates in the Storage which is responsible for S
                                            firstTuple=oppositeTuple;
                                            secondTuple=affectedTuple;
                                        }else{
                                            firstTuple=affectedTuple;
                                            secondTuple=oppositeTuple;    
                                        }

                                        List<String> outputTuple;
                                        if(_joinStorage instanceof JoinHashStorage){
                                            outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple, _joinParams);
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
		tuple = _operatorChain.process(tuple);
                if(tuple == null){
                    return;
                }
                _numSentTuples++;
                printTuple(tuple);

                if(_hierarchyPosition!=FINAL_COMPONENT){
			String outputTupleString=MyUtilities.tupleToString(tuple, _conf);
                        String outputTupleHash = MyUtilities.createHashString(tuple, _hashIndexes, _hashExpressions, _conf);
			//evaluate the hash string BASED ON THE PROJECTED resulted values
                        Values stormTupleSnd = new Values(_componentName,outputTupleString,outputTupleHash);
                        MyUtilities.sendTuple(stormTupleSnd, stormTupleRcv, _collector, _conf);
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
			List<String> outputFields= new ArrayList<String>();
			outputFields.add("TableName");
			outputFields.add("Tuple");
			outputFields.add("Hash");
			declarer.declare(new Fields(outputFields) );
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
            return stormTuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.DumpResults);
        }

        // from StormComponent interface
        @Override
        public int getID() {
            return _ID;
	}

        // from StormEmitter interface
        @Override
        public int[] getEmitterIDs(){
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
            String name = _componentName +":" + _tableName;
            String str = "SourceStorage " + name + " has ID: "+ _ID;
            return str;
        }

}