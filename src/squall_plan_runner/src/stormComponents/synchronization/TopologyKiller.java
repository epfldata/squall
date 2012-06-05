/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package stormComponents.synchronization;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.List;
import stormComponents.StormComponent;

import java.util.Map;

import utilities.SystemParameters;

import org.apache.log4j.Logger;
import utilities.StormWrapper;

/*
 * If NoAck is set up, we receive EOF from the last component
 * Otherwise, we receive from the Spouts when all of sent tuples are fully processed and acked
 */
public class TopologyKiller extends BaseRichBolt implements StormComponent {
    private static Logger LOG = Logger.getLogger(TopologyKiller.class);

    private String _ID;
    private int _numberRegisteredTasks;
    private transient InputDeclarer _inputDeclarer;
    private Map _conf;
    private OutputCollector _collector;
    
    public TopologyKiller(TopologyBuilder builder) {
        _ID = "KILLER";
        _numberRegisteredTasks = 0;
        _inputDeclarer = builder.setBolt(_ID, this);
    }
    
    // from IRichBolt
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
    	_collector=oc;
        _conf=map;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("TopologyKiller: Received EOF message from componentId: " + tuple.getSourceComponent() +
                ", taskId: " + tuple.getSourceTask());
        _numberRegisteredTasks--;
        LOG.info("TopologyKiller: " + _numberRegisteredTasks + " remaining");
        if (_numberRegisteredTasks == 0) {
            LOG.info("TopologyKiller: Received EOF from all the registered tasks. Killing cluster...");
            //EVENT WHEN ALL THE SPOUTS FINISHED EMITTING AND ACKED or
            //  WHEN ALL THE TASKS FROM THE LAST COMPONENTS SENT EOF SIGNAL
            // Instrument all the components for which printOut is set to dump their results
            _collector.emit(SystemParameters.DUMP_RESULTS_STREAM, new Values(SystemParameters.DUMP_RESULTS));
            //write down statistics (the same which is shown in Storm UI web interface)
            if(SystemParameters.getBoolean(_conf, "DIP_DISTRIBUTED")){
                StormWrapper.writeStats(_conf);
            }
            if(SystemParameters.getBoolean(_conf, "DIP_KILL_AT_THE_END")){
                /*  Give enough time to dump the results
                *  We couldn't use Storm ack mechanism for dumping results,
                *    since our final result might be on Spout (StormDataSource).
                *    Spouts cannot send ack to other spout (TopologyKiller spout).
                *    They use EOF boolean to indicate when done.
                */
                Utils.sleep(SystemParameters.SLEEP_BEFORE_KILL_MILLIS);
                StormWrapper.killExecution(_conf);
            }

        }
    }

    @Override
    public void cleanup() {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declareStream(SystemParameters.DUMP_RESULTS_STREAM, new Fields(SystemParameters.DUMP_RESULTS));
    }

    //Helper methods
    public void registerComponent(StormComponent component, int parallelism) {
    	LOG.info("registering new component");
        _numberRegisteredTasks += parallelism;
        _inputDeclarer.allGrouping(component.getID(), SystemParameters.EOF_STREAM);
    }

    @Override
    public String getID() {
    	return _ID;
    }
    
    @Override
    public String getInfoID() {
        String str = "TopologyKiller has ID: " + _ID;
        return str;
    }

    public void printTuple(List<String> tuple) {
        throw new UnsupportedOperationException("These methods are not ment to be invoked for synchronizationStormComponents");
    }

    public void printContent() {
        throw new UnsupportedOperationException("These methods are not ment to be invoked for synchronizationStormComponents");
    }

    public void tupleSend(List<String> tuple, Tuple stormTupleRcv) {
        throw new UnsupportedOperationException("These methods are not ment to be invoked for synchronizationStormComponents");
    }

    public void batchSend() {
        throw new UnsupportedOperationException("These methods are not ment to be invoked for synchronizationStormComponents");
    }
}
