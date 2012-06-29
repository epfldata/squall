/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package utilities;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.*;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;


public class StormWrapper {
    private static Logger LOG = Logger.getLogger(StormWrapper.class);
    private static long startTime;

    // both local and clustered execution

    public static void submitTopology(Config conf, TopologyBuilder builder) {
        //transform mine parameters into theirs
        boolean distributed = SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED");
        String topologyName = MyUtilities.getFullTopologyName(conf);

        conf.setDebug(false);
        if(MyUtilities.isAckEveryTuple(conf)){
            //otherwise this parameter is used only at the end,
            //  and represents the time topology is shown as killed (will be set to default: 30s)
            //Messages are failling if we do not specify timeout (proven for TPCH8)
            conf.setMessageTimeoutSecs(SystemParameters.MESSAGE_TIMEOUT_SECS);
            conf.setMaxSpoutPending(SystemParameters.MAX_SPOUT_PENDING);
        }

        if(distributed){
            if(SystemParameters.isExisting(conf, "DIP_NUM_WORKERS")){
                //by default we use existing value from storm.yaml
                // still, a user can specify other total number of workers
                int numParallelism = SystemParameters.getInt(conf, "DIP_NUM_WORKERS");
                conf.setNumWorkers(numParallelism);
            }
            if(SystemParameters.isExisting(conf, "DIP_NUM_ACKERS")){
                //if not set, it's by default the value from storm.yaml
                int numAckers = SystemParameters.getInt(conf, "DIP_NUM_ACKERS");
                conf.setNumAckers(numAckers);
            }

            try{
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }catch(AlreadyAliveException aae){
                String error=MyUtilities.getStackTrace(aae);
                LOG.info(error);
	     }catch(Exception ex){
                 String error=MyUtilities.getStackTrace(ex);
                 LOG.info(error);
            }
        }else{
            //number of ackers has to be specified in Local Mode
            int numAckers = SystemParameters.getInt(conf, "DIP_NUM_ACKERS");
            conf.setNumAckers(numAckers);

            conf.setFallBackOnJavaSerialization(false);
            LocalCluster cluster = new LocalCluster();
            startTime = System.currentTimeMillis();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
        }
    }

    public static void killExecution(Map conf){
        boolean distributed = SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED");
        String topologyName = MyUtilities.getFullTopologyName(conf);
        if(!distributed){
            localKillCluster(conf, topologyName);
        }else{
            clusterKillTopology(conf, topologyName);
        }
    }
    
    // all the staff below are only for local execution
    private static void localKillCluster(Map conf, String topologyName){
    	long endTime = System.currentTimeMillis();
        LOG.info("Running time (sec):" + ((endTime - startTime) / 1000));
        LocalMergeResults.localPrintAndCompare(conf);
        //Should be killed with the following two lines
        //cluster.killTopology(topologyName);
        //cluster.shutdown();
        //However, it will never stop, so we use:
        System.exit(0);
    }

   // all below are only for cluster execution
    private static Client getNimbusStub(Map conf){
        boolean distributed = SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED");

        String nimbusHost = SystemParameters.getString(conf, Config.NIMBUS_HOST);
        int nimbusThriftPort = SystemParameters.NIMBUS_THRIFT_PORT;
        
        if(distributed){
            NimbusClient nimbus = new NimbusClient(nimbusHost, nimbusThriftPort);
            Client client=nimbus.getClient();
            return client;
        }else{
            throw new RuntimeException("Call getNimbusStub only in cluster mode.");
        }
    }

    private static void clusterKillTopology(Map conf, String topologyName) {
        Client client = getNimbusStub(conf);
        try {
           client.killTopology(topologyName);
//           //Killing a topology right after all the processing is completed
//           KillOptions options = new KillOptions();
//           options.set_wait_secs(0);
//           client.killTopologyWithOpts(topologyName, options);
        } catch (NotAliveException ex) {
            LOG.info(MyUtilities.getStackTrace(ex));
        } catch (TException ex) {
            LOG.info("killTopology:" + MyUtilities.getStackTrace(ex));
        }

    }

    // if we are in local mode, we cannot obtain these information
    public static void writeStats(Map conf){
        Client client=getNimbusStub(conf);
        StringBuilder stats=new StringBuilder("");

        try {
            ClusterSummary clusterInfo = client.getClusterInfo();
            int numOfTopologies = clusterInfo.get_topologies_size();
            stats.append("In total there is ").append(numOfTopologies).append(" topologies.\n");

            Iterator<TopologySummary> topologyIter = clusterInfo.get_topologies_iterator();
            while(topologyIter.hasNext()){
                TopologySummary topologySummary= topologyIter.next();

                //print out basic information about topologies
                String topologyName = topologySummary.get_name();
                stats.append("For topology ").append(topologyName).append(":\n");
                int numTasks = topologySummary.get_num_tasks();
                stats.append(numTasks).append(" tasks, ");
                int numWorkers = topologySummary.get_num_workers();
                stats.append(numWorkers).append(" workers, ");
                int uptimeSecs = topologySummary.get_uptime_secs();
                stats.append(uptimeSecs).append(" uptime seconds.\n");

                String topologyID = topologySummary.get_id();
                String topologyConf = client.getTopologyConf(topologyID);
                stats.append("Topology configuration is \n");
                stats.append(topologyConf);
                stats.append("\n");

                TopologyInfo topologyInfo = client.getTopologyInfo(topologyID);
                //print more about each task
                Iterator<TaskSummary> infoIter = topologyInfo.get_tasks_iterator();
                while(infoIter.hasNext()){
                    TaskSummary summary = infoIter.next();

                    String componentId = summary.get_component_id();
                    stats.append("component_id:").append(componentId).append(", ");
                    int taskId = summary.get_task_id();
                    stats.append("task_id:").append(taskId).append(", ");
                    String host = summary.get_host();
                    stats.append("host:").append(host).append(", ");
                    int port = summary.get_port();
                    stats.append("port:").append(port).append(", ");
                    int uptime = summary.get_uptime_secs();
                    stats.append("uptime:").append(uptime).append("\n");
                    String errors = summary.get_errors().toString();
                    stats.append("Errors: ").append(errors).append("\n");

//                    //TaskStats ts = (TaskStats)summary.getFieldValue(7);
//                    TaskStats ts = summary.get_stats();
//                    if(ts!=null){
//                        stats.append(ts.toString());
//                        /* Furter decomposition: the interface changed from that time
//                            TasksStats
//                            1: required map<string, map<i32, i64>> emitted;
//                            2: required map<string, map<i32, i64>> transferred;
//                            3: required TaskSpecificStats specific;
//                        */
//                        stats.append("\n");
//                    }
                    stats.append("\n");
                }
            }

            String strStats = stats.toString();
            LOG.info(strStats);
        } catch (TException ex) {
            LOG.info("writeStats:" + MyUtilities.getStackTrace(ex));
        } catch (NotAliveException ex) {
            LOG.info(MyUtilities.getStackTrace(ex));
        }
    }

}
