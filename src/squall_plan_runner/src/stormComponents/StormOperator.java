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
import components.ComponentProperties;
import expressions.ValueExpression;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.Operator;
import org.apache.log4j.Logger;
import stormComponents.synchronization.TopologyKiller;
import utilities.MyUtilities;
import utilities.PeriodicBatchSend;
import utilities.SystemParameters;

public class StormOperator extends BaseRichBolt implements StormEmitter, StormComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormOperator.class);
    
    private int _hierarchyPosition=INTERMEDIATE;
    private StormEmitter _emitter;
    private String _ID;
    private String _componentIndex; //a unique index in a list of all the components
                            //used as a shorter name, to save some network traffic
                            //it's of type int, but we use String to save more space

    //output has hash formed out of these indexes
    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private ChainOperator _operatorChain;
    private OutputCollector _collector;
    private boolean _printOut;
    private int _numSentTuples=0;
    private Map _conf;

    //if this is set, we receive using direct stream grouping
    private List<String> _fullHashList;

    //for No ACK: the total number of tasks of all the parent compoonents
    private int _numRemainingParents;

    //for batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicBatchSend _periodicBatch;
    private long _batchOutputMillis;

    public StormOperator(StormEmitter emitter,
            ComponentProperties cp,
            List<String> allCompNames,
            int hierarchyPosition,
            TopologyBuilder builder,
            TopologyKiller killer,
            Config conf) {

        _conf = conf;
        _emitter = emitter;
        _ID = cp.getName();
        _componentIndex = String.valueOf(allCompNames.indexOf(_ID));
        _batchOutputMillis = cp.getBatchOutputMillis();

        int parallelism = SystemParameters.getInt(conf, _ID+"_PAR");

//        if(parallelism > 1 && distinct != null){
//            throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
//        }
        _operatorChain = cp.getChainOperator();

        _hashIndexes = cp.getHashIndexes();
        _hashExpressions = cp.getHashExpressions();

        _hierarchyPosition = hierarchyPosition;

        InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);
        
        _fullHashList = cp.getFullHashList();
        currentBolt = MyUtilities.attachEmitterCustom(conf, _fullHashList, currentBolt, _emitter);

        if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
            killer.registerComponent(this, parallelism);
        }

        _printOut= cp.getPrintOut();
        if (_printOut && _operatorChain.isBlocking()){
           currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
        }
    }

    //from IRichBolt
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

        List<String> tuple = (List<String>) stormTupleRcv.getValue(1);

        if(MyUtilities.isFinalAck(tuple, _conf)){
            _numRemainingParents--;
            MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, stormTupleRcv, _collector, _periodicBatch);
            return;
        }

        applyOperatorsAndSend(stormTupleRcv, tuple);
        _collector.ack(stormTupleRcv);
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
            _collector.ack(stormTupleRcv);
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
        _numRemainingParents = MyUtilities.getNumParentTasks(tc, _emitter);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
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
            if(!_operatorChain.isBlocking()){
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
                 if(_operatorChain.isBlocking()){
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

    //from StormComponent
    @Override
    public String getID() {
        return _ID;
    }

    //from StormEmitter
    @Override
    public String getName() {
        return _ID;
    }

    @Override
    public String[] getEmitterIDs() {
        return new String[]{_ID};
    }

    @Override
    public String getInfoID() {
        String str = "OperatorComponent " + _ID + " has ID: " + _ID;
        return str;
    }

}