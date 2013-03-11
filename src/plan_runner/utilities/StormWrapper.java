package plan_runner.utilities;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.*;
import java.util.Iterator;
import java.util.List;
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
        String topologyName = SystemParameters.getString(conf, "DIP_TOPOLOGY_NAME");

        conf.setDebug(false);
        if(MyUtilities.isAckEveryTuple(conf)){
            //otherwise this parameter is used only at the end,
            //  and represents the time topology is shown as killed (will be set to default: 30s)
            //Default also works here
            //conf.setMessageTimeoutSecs(SystemParameters.MESSAGE_TIMEOUT_SECS);
            
            //Storm throttling mode
            if(MyUtilities.isThrottlingMode(conf)){
                int tp = SystemParameters.getInt(conf, "BATCH_SIZE");
                conf.setMaxSpoutPending(tp);
            }
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
        String topologyName = SystemParameters.getString(conf, "DIP_TOPOLOGY_NAME");
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
    public static void writeStormStats(Map conf){
        Client client=getNimbusStub(conf);
        StringBuilder sb=new StringBuilder("");

        try {
            ClusterSummary clusterInfo = client.getClusterInfo();
            int numOfTopologies = clusterInfo.get_topologies_size();
            sb.append("In total there is ").append(numOfTopologies).append(" topologies.\n");

            Iterator<TopologySummary> topologyIter = clusterInfo.get_topologies_iterator();
            while(topologyIter.hasNext()){
                TopologySummary topologySummary= topologyIter.next();

                //print out basic information about topologies
                String topologyName = topologySummary.get_name();
                sb.append("For topology ").append(topologyName).append(":\n");
                int numTasks = topologySummary.get_num_tasks();
                sb.append(numTasks).append(" tasks, ");
                int numWorkers = topologySummary.get_num_workers();
                sb.append(numWorkers).append(" workers, ");
                int uptimeSecs = topologySummary.get_uptime_secs();
                sb.append(uptimeSecs).append(" uptime seconds.\n");

                String topologyID = topologySummary.get_id();
                String topologyConf = client.getTopologyConf(topologyID);
                sb.append("Topology configuration is \n");
                sb.append(topologyConf);
                sb.append("\n");

                TopologyInfo topologyInfo = client.getTopologyInfo(topologyID);
                //print more about each task
                Iterator<ExecutorSummary> execIter = topologyInfo.get_executors_iterator();
                boolean globalFailed = false;
                while(execIter.hasNext()){
                    ExecutorSummary execSummary = execIter.next();

                    String componentId = execSummary.get_component_id();
                    sb.append("component_id:").append(componentId).append(", ");
                    ExecutorInfo execInfo = execSummary.get_executor_info();
                    int taskStart = execInfo.get_task_start();
                    int taskEnd = execInfo.get_task_end();
                    sb.append("task_id(s) for this executor:").append(taskStart).append("-").append(taskEnd).append(", ");
                    String host = execSummary.get_host();
                    sb.append("host:").append(host).append(", ");
                    int port = execSummary.get_port();
                    sb.append("port:").append(port).append(", ");
                    int uptime = execSummary.get_uptime_secs();
                    sb.append("uptime:").append(uptime).append("\n");
                    sb.append("\n");
                    
                    //printing failing statistics, if there are failed tuples
                    ExecutorSpecificStats stats = execSummary.get_stats().get_specific();
                    boolean isEmpty;
                    Object objFailed;
                    if(stats.is_set_spout()){
                        Map<String, Map<String, Long>> failed = stats.get_spout().get_failed();
                        objFailed = failed;
                        isEmpty = isEmptyMapMap(failed);
                    }else{
                        Map<String, Map<GlobalStreamId, Long>> failed = stats.get_bolt().get_failed();
                        objFailed = failed;
                        isEmpty = isEmptyMapMap(failed);
                    }
                    if(!isEmpty){
                        sb.append("ERROR: There are some failed tuples: ").append(objFailed).append("\n");
                        globalFailed = true;
                    }   
                }
                
                //is there at least one component where something failed
                if(!globalFailed){
                    sb.append("\n\nOK: No tuples failed so far.");
                }else{
                    sb.append("\n\nERROR: Some tuples failed!");
                }
                
                //print topology errors
                Map<String, List<ErrorInfo>> errors = topologyInfo.get_errors();
                if(!isEmptyMap(errors)){
                    sb.append("\n\nERROR: There are some errors in topology: ").append(errors);
                }else{
                    sb.append("\n\nOK: No errors in the topology.");
                }
                
            }

            String strStats = sb.toString();
            LOG.info(strStats);
        } catch (TException ex) {
            LOG.info("writeStats:" + MyUtilities.getStackTrace(ex));
        } catch (NotAliveException ex) {
            LOG.info(MyUtilities.getStackTrace(ex));
        }
    }
    
    private static <T> boolean isEmptyMapMap(Map<String, Map<T, Long>> mapMap) {
        for(Map.Entry<String, Map<T, Long>> outerEntry: mapMap.entrySet()){
            for(Map.Entry<T, Long> innerEntry: outerEntry.getValue().entrySet()){
                long value = innerEntry.getValue();
                if(value != 0){
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isEmptyMap(Map<String, List<ErrorInfo>> map) {
        for(Map.Entry<String, List<ErrorInfo>> outerEntry: map.entrySet()){
            List<ErrorInfo> errors = outerEntry.getValue();
            if(errors != null && !errors.isEmpty()){
                return false;
            }
        }
        return true;
    }    
}
