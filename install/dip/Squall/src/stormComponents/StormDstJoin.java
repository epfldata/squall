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
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import utilities.SystemParameters;

import org.apache.log4j.Logger;
import stormComponents.synchronization.TopologyKiller;

public class StormDstJoin extends BaseRichBolt implements StormJoin, StormComponent {
        private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(StormDstJoin.class);

        private int _hierarchyPosition=INTERMEDIATE;

        private StormEmitter _firstEmitter, _secondEmitter;
        private JoinStorage _firstPreAggStorage, _secondPreAggStorage;
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

        public StormDstJoin(StormEmitter firstEmitter,
                    StormEmitter secondEmitter,
                    String componentName,
                    SelectionOperator selection,
                    DistinctOperator distinct,
                    ProjectionOperator projection,
                    AggregateOperator aggregation,
                    JoinStorage firstPreAggStorage,
                    JoinStorage secondPreAggStorage,
                    ProjectionOperator firstPreAggProj,
                    ProjectionOperator secondPreAggProj,
                    List<Integer> hashIndexes,
                    List<ValueExpression> hashExpressions,
                    int hierarchyPosition,
                    boolean printOut,
                    TopologyBuilder builder,
                    TopologyKiller killer,
                    Config conf) {

            _firstEmitter = firstEmitter;
            _secondEmitter = secondEmitter;
            _componentName = componentName;

            int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
            if(parallelism > 1 && distinct != null){
                throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
            }
            _operatorChain = new ChainOperator(selection, distinct, projection, aggregation);

            _hashIndexes = hashIndexes;
            _hashExpressions = hashExpressions;
            _joinParams = MyUtilities.combineHashIndexes(_firstEmitter, _secondEmitter);

            _hierarchyPosition = hierarchyPosition;

            _ID=MyUtilities.getNextTopologyId();
            InputDeclarer currentBolt = builder.setBolt(Integer.toString(_ID), this, parallelism);
            currentBolt = MyUtilities.attachEmitterComponents(currentBolt, firstEmitter, secondEmitter);

            _printOut= printOut;
            if (_printOut && _operatorChain.isBlocking()){
                currentBolt.allGrouping(Integer.toString(killer.getID()), SystemParameters.DumpResults);
            }

            _firstPreAggStorage = firstPreAggStorage;
            _secondPreAggStorage = secondPreAggStorage;

            _firstPreAggProj = firstPreAggProj;
            _secondPreAggProj = secondPreAggProj;
        }

        // from IRichBolt
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple stormTuple) {
                if (receivedDumpSignal(stormTuple)) {
                    printContent();
                    return;
                }

		String inputComponentName=stormTuple.getString(0);
		String inputTupleString=stormTuple.getString(1);   //INPUT TUPLE
		String inputTupleHash=stormTuple.getString(2);

                String firstEmitterName = _firstEmitter.getName();
                String secondEmitterName = _secondEmitter.getName();

                boolean isFromFirstEmitter = false;
                JoinStorage affectedStorage, oppositeStorage;
                ProjectionOperator projPreAgg;
                if(firstEmitterName.equals(inputComponentName)){
                    //R update
                    isFromFirstEmitter = true;
                    affectedStorage = _firstPreAggStorage;
                    oppositeStorage = _secondPreAggStorage;
                    projPreAgg = _secondPreAggProj;
                }else if(secondEmitterName.equals(inputComponentName)){
                    //S update
                    isFromFirstEmitter = false;
                    affectedStorage = _secondPreAggStorage;
                    oppositeStorage = _firstPreAggStorage;
                    projPreAgg = _firstPreAggProj;
                }else{
                    throw new RuntimeException("InputComponentName " + inputComponentName +
                        " doesn't match neither " + firstEmitterName + " nor " + secondEmitterName + ".");
                }

                //add the stormTuple to the specific storage
                affectedStorage.put(inputTupleHash, inputTupleString);

		performJoin(stormTuple,
                        inputTupleString,
                        inputTupleHash,
                        isFromFirstEmitter,
                        oppositeStorage,
                        projPreAgg);

                _collector.ack(stormTuple);
	}

        protected void performJoin(Tuple stormTuple, 
                String inputTupleString, 
                String inputTupleHash,
                boolean isFromFirstEmitter,
                JoinStorage oppositeStorage,
                ProjectionOperator projPreAgg){

            List<String> affectedTuple = MyUtilities.stringToTuple(inputTupleString, getComponentConfiguration());
            List<String> oppositeStringTupleList = oppositeStorage.get(inputTupleHash);

            if(oppositeStringTupleList!=null)
		  for (int i = 0; i < oppositeStringTupleList.size(); i++) {
			String oppositeStringTuple= oppositeStringTupleList.get(i);
			List<String> oppositeTuple= MyUtilities.stringToTuple(oppositeStringTuple, getComponentConfiguration());

                        List<String> firstTuple, secondTuple;
                        if(isFromFirstEmitter){
                            firstTuple = affectedTuple;
                            secondTuple = oppositeTuple;
                        }else{
                            firstTuple = oppositeTuple;
                            secondTuple = affectedTuple;
                        }

                        List<String> outputTuple;
                        if(oppositeStorage instanceof JoinHashStorage){
                            outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple, _joinParams);
                        }else{
                            outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);
                        }

                        if(projPreAgg != null){
                            outputTuple = projPreAgg.process(outputTuple);
                        }

                        String outputTupleString = MyUtilities.tupleToString(outputTuple, _conf);
			applyOperatorsAndSend(stormTuple, outputTupleString);
		  }
        }

        protected void applyOperatorsAndSend(Tuple stormTuple, String inputTupleString){
		List<String> tuple = MyUtilities.stringToTuple(inputTupleString, _conf);
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
			_collector.emit(stormTuple, new Values(_componentName,outputTupleString,outputTupleHash));
                }
        }

        @Override
        public Map<String,Object> getComponentConfiguration(){
            return _conf;
        }

        @Override
	public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
		_collector=collector;
                _conf=map;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(_hierarchyPosition!=FINAL_COMPONENT){ // then its an intermediate stage not the final one
			ArrayList<String> outputFields= new ArrayList<String>();
			outputFields.add("TableName");
			outputFields.add("Tuple");
			outputFields.add("Hash");
			declarer.declare(new Fields(outputFields) );
		}
	}

        private void printTuple(List<String> tuple){
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

        private void printContent() {
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
            return stormTuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.DumpResults);
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
