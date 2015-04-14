/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.storm_components.synchronization;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

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
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.utilities.StormWrapper;
import ch.epfl.data.squall.utilities.SystemParameters;

/*
 * If NoAck is set up, we receive EOF from the last component
 * Otherwise, we receive from the Spouts when all of sent tuples are fully processed and acked
 */
public class TopologyKiller extends BaseRichBolt implements StormComponent {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    private static Logger LOG = Logger.getLogger(TopologyKiller.class);

    private final String _ID;
    private int _numberRegisteredTasks;
    private transient InputDeclarer _inputDeclarer;
    private Map _conf;
    private OutputCollector _collector;

    public TopologyKiller(TopologyBuilder builder) {
	_ID = "KILLER";
	_numberRegisteredTasks = 0;
	_inputDeclarer = builder.setBolt(_ID, this);
    }

    @Override
    public void aggBatchSend() {
	throw new UnsupportedOperationException(
		"These methods are not ment to be invoked for synchronizationStormComponents");
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declareStream(SystemParameters.DUMP_RESULTS_STREAM,
		new Fields(SystemParameters.DUMP_RESULTS));
    }

    @Override
    public void execute(Tuple tuple) {
	LOG.info("TopologyKiller: Received EOF message from componentId: "
		+ tuple.getSourceComponent() + ", taskId: "
		+ tuple.getSourceTask());
	_numberRegisteredTasks--;
	LOG.info("TopologyKiller: " + _numberRegisteredTasks + " remaining");
	if (_numberRegisteredTasks == 0) {
	    LOG.info("TopologyKiller: Received EOF from all the registered tasks. Killing cluster...");
	    // EVENT WHEN ALL THE SPOUTS FINISHED EMITTING AND ACKED or
	    // WHEN ALL THE TASKS FROM THE LAST COMPONENTS SENT EOF SIGNAL
	    // Instruct all the components for which printOut is set to dump
	    // their results
	    _collector.emit(SystemParameters.DUMP_RESULTS_STREAM, new Values(
		    SystemParameters.DUMP_RESULTS));

	    long timeout = SystemParameters.LOCAL_SLEEP_BEFORE_KILL_MILLIS;
	    if (SystemParameters.getBoolean(_conf, "DIP_DISTRIBUTED")) {
		// write down statistics (the same which is shown in Storm UI
		// web interface)
		StormWrapper.writeStormStats(_conf);
		timeout = SystemParameters.CLUSTER_SLEEP_BEFORE_KILL_MILLIS;
	    }
	    if (SystemParameters.getBoolean(_conf, "DIP_KILL_AT_THE_END")) {
		/*
		 * Give enough time to dump the results We couldn't use Storm
		 * ack mechanism for dumping results, since our final result
		 * might be on Spout (StormDataSource). Spouts cannot send ack
		 * to other spout (TopologyKiller spout). They use EOF boolean
		 * to indicate when done.
		 */
		Utils.sleep(timeout);
		StormWrapper.killExecution(_conf);
	    }

	}
    }

    @Override
    public String getID() {
	return _ID;
    }

    @Override
    public String getInfoID() {
	final String str = "TopologyKiller has ID: " + _ID;
	return str;
    }

    // from IRichBolt
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
	_collector = oc;
	_conf = map;
    }

    @Override
    public void printContent() {
	throw new UnsupportedOperationException(
		"These methods are not ment to be invoked for synchronizationStormComponents");
    }

    @Override
    public void printTuple(List<String> tuple) {
	throw new UnsupportedOperationException(
		"These methods are not ment to be invoked for synchronizationStormComponents");
    }

    @Override
    public void printTupleLatency(long numSentTuples, long timestamp) {
	throw new UnsupportedOperationException(
		"These methods are not ment to be invoked for synchronizationStormComponents");
    }

    public void registerComponent(BaseRichBolt component, String componentName,
	    int parallelism) {
	LOG.info("registering new component " + componentName
		+ " with parallelism " + parallelism);
	_numberRegisteredTasks += parallelism;
	_inputDeclarer.allGrouping(componentName, SystemParameters.EOF_STREAM);
    }

    // Helper methods
    public void registerComponent(StormComponent component, int parallelism) {
	LOG.info("registering new component");
	_numberRegisteredTasks += parallelism;
	_inputDeclarer.allGrouping(component.getID(),
		SystemParameters.EOF_STREAM);
    }

    @Override
    public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
	    long timestamp) {
	throw new UnsupportedOperationException(
		"These methods are not ment to be invoked for synchronizationStormComponents");
    }

}
