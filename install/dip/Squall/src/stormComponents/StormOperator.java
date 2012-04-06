/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package stormComponents;


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
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.DistinctOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import org.apache.log4j.Logger;
import stormComponents.synchronization.TopologyKiller;
import utilities.MyUtilities;
import utilities.SystemParameters;

public class StormOperator extends BaseRichBolt implements StormEmitter, StormComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormOperator.class);
    
    private int _hierarchyPosition=INTERMEDIATE;
    private StormEmitter _emitter;
    private String _componentName;
    private int _ID;
    //output has hash formed out of these indexes
    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private ChainOperator _operatorChain;
    private OutputCollector _collector;
    private boolean _printOut;
    private int _receivedTuples;
    private Map _conf;

    public StormOperator(StormEmitter emitter,
            String componentName,
            SelectionOperator selection,
            DistinctOperator distinct,
            ProjectionOperator projection,
            AggregateOperator aggregation,
            List<Integer> hashIndexes,
            List<ValueExpression> hashExpressions,
            int hierarchyPosition,
            boolean printOut,
            TopologyBuilder builder,
            TopologyKiller killer,
            Config conf) {

        _emitter = emitter;
        _componentName = componentName;

        int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
        if(parallelism > 1 && distinct != null){
            throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
        }
        _operatorChain = new ChainOperator(selection, distinct, projection, aggregation);

        _hashIndexes = hashIndexes;
        _hashExpressions = hashExpressions;

        _hierarchyPosition = hierarchyPosition;

        _ID=MyUtilities.getNextTopologyId();
        InputDeclarer currentBolt = builder.setBolt(Integer.toString(_ID), this, parallelism);
        currentBolt = MyUtilities.attachEmitterComponents(currentBolt, _emitter);

        _printOut= printOut;
        if (_printOut && _operatorChain.isBlocking()){
           currentBolt.allGrouping(Integer.toString(killer.getID()), SystemParameters.DumpResults);
        }
    }
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
        _collector=collector;
        _conf=map;
    }

    //from IRichBolt
    @Override
    public void execute(Tuple stormTuple) {
	if (receivedDumpSignal(stormTuple)) {
                    printContent();
                    return;
        }

        String inputTupleString = stormTuple.getString(1);
        List<String> tuple = MyUtilities.stringToTuple(inputTupleString, _conf);

        tuple = _operatorChain.process(tuple);
        if(tuple == null){
            _collector.ack(stormTuple);
            return;
        }
        printTuple(tuple);

        if(_hierarchyPosition != FINAL_COMPONENT){
            //we are emitting tuple unless we are the very last component in the hirerachy
            String outputTupleString=MyUtilities.tupleToString(tuple, _conf);
            String outputTupleHash = MyUtilities.createHashString(tuple, _hashIndexes, _hashExpressions, _conf);
            //evaluate the hash string BASED ON THE PROJECTED resulted values
            _collector.emit(stormTuple, new Values(_componentName,outputTupleString,outputTupleHash));
        }
        _collector.ack(stormTuple);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
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
            if(!_operatorChain.isBlocking()){
                _receivedTuples++;
                StringBuilder sb = new StringBuilder();
                sb.append("\nComponent ").append(_componentName);
                sb.append("\nReceived tuples: ").append(_receivedTuples);
                sb.append(" Tuple: ").append(MyUtilities.tupleToString(tuple, _conf));
                LOG.info(sb.toString());
            }
        }
    }

    private void printContent() {
             if(_printOut){
                 if(_operatorChain.isBlocking()){
                        MyUtilities.printBlockingResult(_componentName, (AggregateOperator)_operatorChain.getLastOperator(), _hierarchyPosition, _conf, LOG);
                 }
             }
    }

    private boolean receivedDumpSignal(Tuple stormTuple) {
        return stormTuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.DumpResults);
    }

    //from StormComponent
    @Override
    public int getID() {
        return _ID;
    }

    //from StormEmitter
    @Override
    public String getName() {
        return _componentName;
    }

    @Override
    public int[] getEmitterIDs() {
        return new int[]{_ID};
    }

    @Override
    public List<Integer> getHashIndexes() {
        return _hashIndexes;
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
        return _hashExpressions;
    }

    @Override
    public String getInfoID() {
        String str = "OperatorComponent " + _componentName + " has ID: " + _ID;
        return str;
    }

}