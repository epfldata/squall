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
import stormComponents.StormComponent;

import java.util.Map;

import utilities.SystemParameters;
import utilities.MyUtilities;

import org.apache.log4j.Logger;
import utilities.StormWrapper;

/**
 *
 * @author klonatos
 */
public class TopologyKiller extends BaseRichBolt implements StormComponent {
    private static Logger LOG = Logger.getLogger(TopologyKiller.class);

    private int _ID;
    private int _numberRegisteredSpouts;
    private transient InputDeclarer _inputDeclarer;
    private Map conf;
    private OutputCollector _collector;
    
    public TopologyKiller(TopologyBuilder builder) {
        _ID = MyUtilities.getNextTopologyId();
        _numberRegisteredSpouts = 0;
        _inputDeclarer = builder.setBolt(Integer.toString(_ID), this);
    }
    
    // from IRichBolt
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
    	_collector=oc;
        conf=map;
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("TopologyKiller: Received EOF message from: " + tuple.getSourceComponent());
        _numberRegisteredSpouts--;
        LOG.info("TopologyKiller: " + _numberRegisteredSpouts + " remaining");
        if (_numberRegisteredSpouts == 0) {
            LOG.info("TopologyKiller: Received EOF from all spouts. Killing cluster..."); 
            //EVENT WHEN ALL THE SPOUTS FINISHED EMITTING AND ACKED ..
            // Instrument all the components for which printOut is set to dump their results
            _collector.emit(SystemParameters.DumpResults, new Values("DumpResults"));
            //write down statistics (the same which is shown in Storm UI web interface)
            if(SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED")){
                StormWrapper.writeStats(conf);            	
            }
            if(SystemParameters.getBoolean(conf, "DIP_KILL_AT_THE_END")){
                /*  Give enough time to dump the results
                *  We couldn't use Storm ack mechanism for dumping results,
                *    since our final result might be on Spout (StormDataSource).
                *    Spouts cannot send ack to other spout (TopologyKiller spout).
                *    They use EOF boolean to indicate when done.
                */
                Utils.sleep(SystemParameters.SLEEP_BEFORE_KILL_MILLIS);
                StormWrapper.killExecution(conf);
            }

        }
    }

    @Override
    public void cleanup() {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declareStream(SystemParameters.DumpResults, new Fields("DumpResults"));
    }

    //Helper methods
    public void registerSpout(StormComponent spout) {
    	LOG.info("registering new spout");
        _numberRegisteredSpouts++;
        _inputDeclarer.allGrouping(Integer.toString(spout.getID()), SystemParameters.EOFmessageStream);
    }

    @Override
    public int getID() {
    	return _ID;
    }
    
    @Override
    public String getInfoID() {
        String str = "TopologyKiller has ID: " + _ID;
        return str;
    }
}
