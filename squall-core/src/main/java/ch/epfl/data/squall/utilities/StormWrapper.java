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

package ch.epfl.data.squall.utilities;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.components.Component;

public class StormWrapper {
  private static boolean _waiting;
  private static Semaphore _semWait = new Semaphore(0, true);
  private static LocalCluster localCluster = null;

    private static void clusterKillTopology(Map conf, String topologyName) {
	final Client client = getNimbusStub(conf);
	try {
	    client.killTopology(topologyName);
	    // //Killing a topology right after all the processing is completed
	    // KillOptions options = new KillOptions();
	    // options.set_wait_secs(0);
	    // client.killTopologyWithOpts(topologyName, options);
	} catch (final NotAliveException ex) {
	    LOG.info(MyUtilities.getStackTrace(ex));
	} catch (final TException ex) {
	    LOG.info("killTopology:" + MyUtilities.getStackTrace(ex));
	}

    }

    // all below are only for cluster execution
    private static Client getNimbusStub(Map conf) {
	final boolean distributed = SystemParameters.getBoolean(conf,
		"DIP_DISTRIBUTED");

	final String nimbusHost = SystemParameters.getString(conf,
		Config.NIMBUS_HOST);
	final int nimbusThriftPort = SystemParameters.NIMBUS_THRIFT_PORT;

	if (distributed) {
	    NimbusClient nimbus = null;
	    try {
		Map<String, String> securityMap = new HashMap<String, String>();
		securityMap.put("storm.thrift.transport",
			"org.apache.storm.security.auth.SimpleTransportPlugin");
		nimbus = new NimbusClient(securityMap, nimbusHost,
			nimbusThriftPort);
	    } catch (TTransportException e) {
		LOG.info(MyUtilities.getStackTrace(e));
		System.exit(1);
	    }
	    final Client client = nimbus.getClient();
	    return client;
	} else
	    throw new RuntimeException(
		    "Call getNimbusStub only in cluster mode.");
    }

    // both local and clustered execution

    private static boolean isEmptyMap(Map<String, List<ErrorInfo>> map) {
	for (final Map.Entry<String, List<ErrorInfo>> outerEntry : map
		.entrySet()) {
	    final List<ErrorInfo> errors = outerEntry.getValue();
	    if (errors != null && !errors.isEmpty())
		return false;
	}
	return true;
    }

    private static <T> boolean isEmptyMapMap(Map<String, Map<T, Long>> mapMap) {
	for (final Map.Entry<String, Map<T, Long>> outerEntry : mapMap
		.entrySet())
	    for (final Map.Entry<T, Long> innerEntry : outerEntry.getValue()
		    .entrySet()) {
		final long value = innerEntry.getValue();
		if (value != 0)
		    return false;
	    }
	return true;
    }

    public static void killExecution(Map conf) {
	final boolean distributed = SystemParameters.getBoolean(conf,
		"DIP_DISTRIBUTED");
	final String topologyName = SystemParameters.getString(conf,
		"DIP_TOPOLOGY_NAME");
	if (distributed) {
          Utils.sleep(SystemParameters.CLUSTER_SLEEP_BEFORE_KILL_MILLIS);
          clusterKillTopology(conf, topologyName);
        } else if (_waiting) {
          _semWait.release();
        } else {
          Utils.sleep(SystemParameters.LOCAL_SLEEP_BEFORE_KILL_MILLIS);
          localKillTopology(conf, topologyName);
        }
    }

  public static void shutdown() {
    if (localCluster != null) {
      localCluster.shutdown();
      localCluster = null;
    }
  }


    // all the staff below are only for local execution
    private static void localKillTopology(Map conf, String topologyName) {
	final long endTime = System.currentTimeMillis();
	LOG.info("Running time (sec):" + ((endTime - startTime) / 1000));
	int result = LocalMergeResults.localPrintAndCompare(conf);
	// Should be killed with the following two lines
	// cluster.killTopology(topologyName);
	// cluster.shutdown();
	// However, it will never stop, so we use:
	System.exit(result);
    }

  public static BasicStore<Object> localSubmitAndWait(SquallContext context, QueryBuilder plan) throws InterruptedException {
    Config conf = context.getConfiguration();
    StormTopology topology = plan.createTopology(context).createTopology();
    final String topologyName = SystemParameters.getString(conf,
                                                           "DIP_TOPOLOGY_NAME");

    LocalMergeResults.reset();
    if (localCluster != null && localCluster.getClusterInfo().get_topologies_size() > 0) {
      LOG.error("There are already running topologies, localSubmitAndWait should only be called for an empty local cluster.");
      throw new RuntimeException();
    }

    _waiting = true;
    submitTopology(context, plan.createTopology(context).createTopology());
    _semWait.acquire();

    LOG.info("Waiting for results from topology '" + topologyName +"'");
    LocalMergeResults.waitForResults(plan.getNumberFinalTasks(conf));

    LOG.info("Killing topology '" + topologyName +"'");
    KillOptions options = new KillOptions();
    options.set_wait_secs(0);
    localCluster.killTopologyWithOpts(topologyName, options);
    if (localCluster.getClusterInfo().get_topologies_size() > 0) {
      LOG.info("Blocking until '" + topologyName +"' is killed");
      while (localCluster.getClusterInfo().get_topologies_size() > 0) {
        Thread.sleep(500);
      }
    }
    LOG.info("'" + topologyName +"' was succesfully killed");

    _waiting = false;

    return LocalMergeResults.getResults();
  }

    public static void submitTopology(SquallContext context, TopologyBuilder builder) {
      submitTopology(context, builder.createTopology());
    }

    public static void submitTopology(SquallContext context, StormTopology topology) {
        Config conf = context.getConfiguration();

	// transform mine parameters into theirs
	final boolean distributed = SystemParameters.getBoolean(conf,
		"DIP_DISTRIBUTED");
	final String topologyName = SystemParameters.getString(conf,
		"DIP_TOPOLOGY_NAME");

	// conf.setDebug(false);
	if (MyUtilities.isAckEveryTuple(conf))
	    // Storm throttling mode
	    if (MyUtilities.isThrottlingMode(conf)) {
		final int tp = SystemParameters.getInt(conf, "BATCH_SIZE");
		conf.setMaxSpoutPending(tp);
	    }

	if (distributed) {
	    if (SystemParameters.isExisting(conf, "DIP_NUM_WORKERS")) {
		// by default we use existing value from storm.yaml
		// still, a user can specify other total number of workers
		final int numParallelism = SystemParameters.getInt(conf,
			"DIP_NUM_WORKERS");
		conf.setNumWorkers(numParallelism);
	    }
	    if (SystemParameters.isExisting(conf, "DIP_NUM_ACKERS")) {
		// if not set, it's by default the value from storm.yaml
		final int numAckers = SystemParameters.getInt(conf,
			"DIP_NUM_ACKERS");
		conf.setNumAckers(numAckers);
	    }

	    try {
		StormSubmitter.submitTopology(topologyName, conf, topology);
	    } catch (final AlreadyAliveException aae) {
		final String error = MyUtilities.getStackTrace(aae);
		LOG.info(error);
	    } catch (final Exception ex) {
		final String error = MyUtilities.getStackTrace(ex);
		LOG.info(error);
	    }
	} else {
	    // number of ackers has to be specified in Local Mode
	    final int numAckers = SystemParameters.getInt(conf,
		    "DIP_NUM_ACKERS");
	    conf.setNumAckers(numAckers);

	    conf.setFallBackOnJavaSerialization(false);
            if (localCluster == null) {
              localCluster = new LocalCluster();
            }

	    startTime = System.currentTimeMillis();
	    localCluster.submitTopology(topologyName, conf, topology);
	}
    }

    // if we are in local mode, we cannot obtain these information
    public static void writeStormStats(Map conf) {
	final Client client = getNimbusStub(conf);
	final StringBuilder sb = new StringBuilder("");

	try {
	    final ClusterSummary clusterInfo = client.getClusterInfo();
	    final int numOfTopologies = clusterInfo.get_topologies_size();
	    sb.append("In total there is ").append(numOfTopologies)
		    .append(" topologies.\n");

	    final Iterator<TopologySummary> topologyIter = clusterInfo
		    .get_topologies_iterator();
	    while (topologyIter.hasNext()) {
		final TopologySummary topologySummary = topologyIter.next();

		// print out basic information about topologies
		final String topologyName = topologySummary.get_name();
		sb.append("For topology ").append(topologyName).append(":\n");
		final int numTasks = topologySummary.get_num_tasks();
		sb.append(numTasks).append(" tasks, ");
		final int numWorkers = topologySummary.get_num_workers();
		sb.append(numWorkers).append(" workers, ");
		final int uptimeSecs = topologySummary.get_uptime_secs();
		sb.append(uptimeSecs).append(" uptime seconds.\n");

		final String topologyID = topologySummary.get_id();
		final String topologyConf = client.getTopologyConf(topologyID);
		sb.append("Topology configuration is \n");
		sb.append(topologyConf);
		sb.append("\n");

		final TopologyInfo topologyInfo = client
			.getTopologyInfo(topologyID);

		// print more about each task
		final Iterator<ExecutorSummary> execIter = topologyInfo
			.get_executors_iterator();
		boolean globalFailed = false;
		while (execIter.hasNext()) {
		    final ExecutorSummary execSummary = execIter.next();
		    final String componentId = execSummary.get_component_id();
		    sb.append("component_id:").append(componentId).append(", ");
		    final ExecutorInfo execInfo = execSummary
			    .get_executor_info();
		    final int taskStart = execInfo.get_task_start();
		    final int taskEnd = execInfo.get_task_end();
		    sb.append("task_id(s) for this executor:")
			    .append(taskStart).append("-").append(taskEnd)
			    .append(", ");
		    final String host = execSummary.get_host();
		    sb.append("host:").append(host).append(", ");
		    final int port = execSummary.get_port();
		    sb.append("port:").append(port).append(", ");
		    final int uptime = execSummary.get_uptime_secs();
		    sb.append("uptime:").append(uptime).append("\n");
		    sb.append("\n");

		    // printing failing statistics, if there are failed tuples
		    final ExecutorStats es = execSummary.get_stats();
		    if (es == null)
			sb.append("No info about failed tuples\n");
		    else {
			final ExecutorSpecificStats stats = es.get_specific();
			boolean isEmpty;
			Object objFailed;
			if (stats.is_set_spout()) {
			    final Map<String, Map<String, Long>> failed = stats
				    .get_spout().get_failed();
			    objFailed = failed;
			    isEmpty = isEmptyMapMap(failed);
			} else {
			    final Map<String, Map<GlobalStreamId, Long>> failed = stats
				    .get_bolt().get_failed();
			    objFailed = failed;
			    isEmpty = isEmptyMapMap(failed);
			}
			if (!isEmpty) {
			    sb.append("ERROR: There are some failed tuples: ")
				    .append(objFailed).append("\n");
			    globalFailed = true;
			}
		    }
		}

		// is there at least one component where something failed
		if (!globalFailed)
		    sb.append("OK: No tuples failed so far.\n");
		else
		    sb.append("ERROR: Some tuples failed!\n");

		// print topology errors
		final Map<String, List<ErrorInfo>> errors = topologyInfo
			.get_errors();
		if (!isEmptyMap(errors))
		    sb.append("ERROR: There are some errors in topology: ")
			    .append(errors).append("\n");
		else
		    sb.append("OK: No errors in the topology.\n");

	    }
	    sb.append("\n\n");

	    final String strStats = sb.toString();
	    LOG.info(strStats);
	} catch (final NotAliveException ex) {
	    LOG.info(MyUtilities.getStackTrace(ex));
	} catch (final TException ex) {
	    LOG.info("writeStats:" + MyUtilities.getStackTrace(ex));
	}
		}

    public static ClusterSummary getClusterInfo(boolean local, Map conf) {
      if (local) {
        return localCluster.getClusterInfo();
      } else {
        Client client = getNimbusStub(conf);
        try {
          return client.getClusterInfo();
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    }


  public static void killTopology(boolean local, Map conf, String name) throws NotAliveException {
    if (local) {
      localCluster.killTopology(name);
    } else {
        Client client = getNimbusStub(conf);
        try {
          client.killTopology(name);
        } catch (TException e) {
          throw new RuntimeException(e);
        }
    }
  }

  public static TopologyInfo getTopology(boolean local, Map conf, String name) throws NotAliveException {
    if (local) {
      return localCluster.getTopologyInfo(name);
    } else {
        Client client = getNimbusStub(conf);
        try {
          return client.getTopologyInfo(name);
        } catch (TException e) {
          throw new RuntimeException(e);
        }
    }
  }

    private static Logger LOG = Logger.getLogger(StormWrapper.class);

    private static long startTime;
}
