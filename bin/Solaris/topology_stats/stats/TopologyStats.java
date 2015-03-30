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


package stats;

import backtype.storm.generated.ExecutorStats; 
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
import backtype.storm.utils.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.thrift7.TException;


public class TopologyStats {
    private static final String NIMBUS_HOST = "icdatasrv5";
    private static final int NIMBUS_THRIFT_PORT = 6627;

    private static Client getNimbusStub(){
        NimbusClient nimbus = new NimbusClient(NIMBUS_HOST, NIMBUS_THRIFT_PORT);
        Client client=nimbus.getClient();
        return client;
    }
    
    private static TopologySummary getTopologySummary(Client client) {
        TopologySummary topologySummary = null;
        try {
            ClusterSummary clusterInfo = client.getClusterInfo();
            int numOfTopologies = clusterInfo.get_topologies_size();
            if(numOfTopologies > 1){
                throw new RuntimeException("For multiple topologies in the cluster, statistics would not be gathered correctly.");
            }

            topologySummary = clusterInfo.get_topologies().get(0);
        }catch (TException ex) {
            ex.printStackTrace();
        }
        return topologySummary;
    }    

    public static String writeTopologyInfo(){
        Client client=getNimbusStub();
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
                    ExecutorStats es = execSummary.get_stats();
		    		if(es == null){
				      sb.append("No info about failed tuples\n");
				    }else{
				      ExecutorSpecificStats stats = es.get_specific();
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
                }
                
                //is there at least one component where something failed
                if(!globalFailed){
                    sb.append("OK: No tuples failed so far.\n");
                }else{
                    sb.append("ERROR: Some tuples failed!\n");
                }
                
                //print topology errors
                Map<String, List<ErrorInfo>> errors = topologyInfo.get_errors();
                if(!isEmptyMap(errors)){
                    sb.append("ERROR: There are some errors in topology: ").append(errors).append("\n");
                }else{
                    sb.append("OK: No errors in the topology.\n");
                }
                
		        boolean withAckers = getAckMode(topologyInfo);
         		int topologyUptime = topologySummary.get_uptime_secs();
		        sb.append(writeStatistics(topologyInfo, topologyUptime, withAckers));
		        sb.append(writeBoltLatencies(topologyInfo, topologyUptime, withAckers));
            }
            sb.append("\n\n");
        
        } catch (TException ex) {
            ex.printStackTrace();
        } catch (NotAliveException ex) {
            ex.printStackTrace();
        }
        return sb.toString();
    }
    
    private static String writeStatistics(TopologyInfo topologyInfo, int topologyUptime, boolean withAckers) {
        StringBuilder sb=new StringBuilder("");
                                
        //Map<ComponentName, List<AckedTuples, Latency>>
        Map<String, List<TuplesInfo>> spoutsInfo = new HashMap<String, List<TuplesInfo>>();
                        
        //more about each executor/task
        Iterator<ExecutorSummary> execIter = topologyInfo.get_executors_iterator();              
        sb.append("\n\n").append("Raw data:\n");
        while(execIter.hasNext()){
            ExecutorSummary execSummary = execIter.next();                                    
            String componentId = execSummary.get_component_id();
                    
            ExecutorSpecificStats stats = execSummary.get_stats().get_specific();
            if(stats.is_set_spout()){
                //ACKED_TUPLES
                //Map<TimeWindow, <Stream, NumTuplesAcked>>,
                //  TimeWindow takes one of the following: ":all-time", "600" (10mins), "10800" (3h), "86400" (1d)
                Map<String, Map<String, Long>> ackedMap = getNumTuplesSpout(execSummary, withAckers);
                sb.append("An executor of spout ").append(componentId).append(" has tuples acked \n").append(ackedMap).append("\n");

                //TODO: For now, for both throughput and latency, we count only on "default" stream.
                long executorAckedTuples = 0L;
                Long executorAckedTuplesObj = ackedMap.get(":all-time").get("default");
                if(executorAckedTuplesObj!=null){
                	executorAckedTuples = executorAckedTuplesObj;
                }
                
                //LATENCIES
                double executorLatency = 0;
                if(withAckers){
                    //Map<TimeWindow, <Stream, Latency>>, 
                    //  TimeWindow takes one of the following: ":all-time", "600" (10mins), "10800" (3h), "86400" (1d)
                    Map<String, Map<String, Double>> completeMsAvg = getLatency(execSummary, withAckers);
                    sb.append(" and latency \n").append(completeMsAvg).append("\n");
                    executorLatency = completeMsAvg.get(":all-time").get("default");
                }
                sb.append("\n");
                
                //KEEPING BOTH
                TuplesInfo ti = new TuplesInfo(executorAckedTuples, executorLatency);
                appendLatency(spoutsInfo, componentId, ti);
            }else if(stats.is_set_bolt()){
            	//ACKED_TUPLES
                //Map<TimeWindow, <Stream, NumTuplesAcked>>,
                //  TimeWindow takes one of the following: ":all-time", "600" (10mins), "10800" (3h), "86400" (1d)
                Map<String, Map<String, Long>> ackedMap = getNumTuplesBoltEmitted(execSummary, withAckers);
                sb.append("An executor of bolt ").append(componentId).append(" has emitted tuples \n").append(ackedMap).append("\n");
            }
        }
        
        if(!withAckers){
            sb.append("\n\nWARNING: Note that throughputs are based on how many tuples are *transferred* from spouts, rather than fully processed.\n");
        }
                
        List<TuplesInfo> allCompTuplesLatency = new ArrayList<TuplesInfo>();
                
        //AVERAGES PER COMPONENT
        sb.append("\n").append("Per component:");
        for(Map.Entry<String, List<TuplesInfo>> componentInfos: spoutsInfo.entrySet()){
            String componentId = componentInfos.getKey();
            List<TuplesInfo> tiList = componentInfos.getValue();
            
            if(withAckers){
                double avgLatency = getAvgLatency(tiList);
                sb.append("\nAverage latency for default stream for spout ").append(componentId).append(" is ").append(avgLatency).append("ms.");
            }

            double totalThroughput = getTotalThroughput(tiList, topologyUptime);
            sb.append("\nTotal throughput for default stream for spout ").append(componentId).append(" is ").append(totalThroughput).append("t/s.");
                    
            allCompTuplesLatency.addAll(tiList);
        }
                
        //AVERAGES IN TOTAL
        sb.append("\n\n").append("In total:");
        if(withAckers){
            sb.append("\nAverage latency for default stream for all the spouts is ").append(getAvgLatency(allCompTuplesLatency)).append("ms.");
        }
        sb.append("\nTotal throughput for default stream for all the spouts is ").append(getTotalThroughput(allCompTuplesLatency, topologyUptime)).append("t/s.");   
                
        return sb.toString();
    }
    
    private static boolean getAckMode(TopologyInfo topologyInfo) {
        List<ExecutorSummary> execSummaries = topologyInfo.get_executors();
        for(ExecutorSummary execSummary: execSummaries){
            String componentId = execSummary.get_component_id();
            if(componentId.equals("__acker")){
                return true;
            }
        }
        return false;
    }
    
    private static void appendLatency(Map<String, List<TuplesInfo>> componentInfo, String componentId, TuplesInfo ti) {
        if(componentInfo.containsKey(componentId)){
            List<TuplesInfo> latencies = componentInfo.get(componentId);
            latencies.add(ti);
        }else{
            List<TuplesInfo> latencies = new ArrayList<TuplesInfo>();
            latencies.add(ti);
            componentInfo.put(componentId, latencies);
        }
    }

    private static Map<String, Map<String, Long>> getNumTuplesSpout(ExecutorSummary execSummary, boolean withAckers) {
        boolean isSpout = execSummary.get_stats().get_specific().is_set_spout();
        if(!isSpout){
            throw new RuntimeException("Developer error. This method should be called only from spouts!");
        }
            
        Map<String, Map<String, Long>> ackedMap;
        if(withAckers){
            ackedMap = execSummary.get_stats().get_specific().get_spout().get_acked();
        }else{
            //spouts do not set number of acked tuples when working in nonAckers mode
            // this is not very precise, because it does not correspond in number of tuples *fully processed*
            ackedMap = execSummary.get_stats().get_transferred();
        }
        return ackedMap;
    }

    private static Map<String, Map<String, Long>> getNumTuplesBoltEmitted(ExecutorSummary execSummary, boolean withAckers) {
        boolean isBolt = execSummary.get_stats().get_specific().is_set_bolt();
        if(!isBolt){
            throw new RuntimeException("Developer error. This method should be called only from bolts!");
        }
            
        Map<String, Map<String, Long>> ackedMap = null;
        if(withAckers){
            throw new RuntimeException("A TODO has to be fixed!");
        	//TODO
        	/*
        	 * Map<String, Map<GlobalStreamId, Long>> ackedMap = execSummary.get_stats().get_specific().get_bolt().get_acked(); 
        	 * 
        	 * struct GlobalStreamId {
  				1: required string componentId;
  				2: required string streamId;
  				#Going to need to add an enum for the stream type (NORMAL or FAILURE)
		   	   }
        	 */
        }else{
            //bolts do not set number of acked tuples when working in nonAckers mode
            // this is not very precise, because it does not correspond in number of tuples *fully processed*
        	// but at the end of execution (status of a topology is KILLED) it's exactly what we need
            ackedMap = execSummary.get_stats().get_emitted(); //.get_transferred()
        }
        return ackedMap;
    }    
    
    private static Map<String, Map<String, Double>> getLatency(ExecutorSummary execSummary, boolean withAckers) {
        boolean isSpout = execSummary.get_stats().get_specific().is_set_spout();
        if(!isSpout){
            throw new RuntimeException("Developer error. This method should be called only from spouts!");
        }
        if(!withAckers){
            throw new RuntimeException("Developer error. This method should be called only when withAckers = true.");
        }
        return execSummary.get_stats().get_specific().get_spout().get_complete_ms_avg();
    }
    
    //Average weight by the number of tuples having certain complete_ms_avg(average latency)
    private static double getAvgLatency(List<TuplesInfo> tiList) {
       long totalNumTuples = 0;
       double sumLatency = 0;
       for(TuplesInfo ti: tiList){
           long currentNumTuples = ti.getNumTuples();
           sumLatency += currentNumTuples * ti.getLatency();
           totalNumTuples += currentNumTuples;
       }
       return sumLatency / totalNumTuples;
    }
    
    private static double getTotalThroughput(List<TuplesInfo> tiList, int topologyUptime) {
        long numTuples = 0;
        for(TuplesInfo ti: tiList){
            numTuples += ti.getNumTuples();
        }
        return ((double)numTuples) / topologyUptime;
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
    
    /*
     * Bolt latencies    
     */
    private static String writeBoltLatencies(TopologyInfo topologyInfo, int topologyUptime, boolean withAckers) {
        StringBuilder sb=new StringBuilder("");
                                
      //Map<ComponentName, List<AckedTuples, Latency>>
        Map<String, List<TuplesInfo>> boltsInfo = new HashMap<String, List<TuplesInfo>>();
                        
        //more about each executor/task
        Iterator<ExecutorSummary> execIter = topologyInfo.get_executors_iterator();              
        sb.append("\n\n").append("BOLT LATENCIES:\n");
        while(execIter.hasNext()){
            ExecutorSummary execSummary = execIter.next();                                    
            String componentId = execSummary.get_component_id();
                    
            ExecutorSpecificStats stats = execSummary.get_stats().get_specific();
            if(stats.is_set_bolt()){
                //EXECUTED_TUPLES
                //Map<TimeWindow, <GlobalStreamId, NumTuplesExecuted>>,
                //  TimeWindow takes one of the following: ":all-time", "600" (10mins), "10800" (3h), "86400" (1d)
                //LATENCIES
                //Map<TimeWindow, <GlobalStreamId, Latency>>, 
                //  TimeWindow takes one of the following: ":all-time", "600" (10mins), "10800" (3h), "86400" (1d)            	
            	               
                Map<GlobalStreamId, Long> executedMapAllTime = getNumTuplesBolt(execSummary);
                //let's sum up all the executed tuples during the entire execution for all the streams separately
                for (Map.Entry<GlobalStreamId, Long> entry : executedMapAllTime.entrySet()) {
                	GlobalStreamId streamId = entry.getKey();
                	long executedTuples = entry.getValue();
                	double execLatency = getBoltLatency(execSummary).get(streamId);

                	//adding to the collection
                	TuplesInfo ti = new TuplesInfo(executedTuples, execLatency);
                    appendLatency(boltsInfo, componentId, ti);       	
                }
            }    
        }
        
        //AVERAGES PER COMPONENT
        sb.append("\n").append("Per component:");
        for(Map.Entry<String, List<TuplesInfo>> componentInfos: boltsInfo.entrySet()){
            String componentId = componentInfos.getKey();
            List<TuplesInfo> tiList = componentInfos.getValue();
            double avgLatency = getAvgLatency(tiList);
            sb.append("\nAverage execute latency for bolt ").append(componentId).append(" is ").append(avgLatency).append("ms.");
        }
                
        return sb.toString();
    }
    
	//over all time
    private static Map<GlobalStreamId, Long> getNumTuplesBolt(ExecutorSummary execSummary) {
        boolean isBolt = execSummary.get_stats().get_specific().is_set_bolt();
        if(!isBolt){
            throw new RuntimeException("Developer error. This method should be called only from bolts!");
        }
            
        return execSummary.get_stats().get_specific().get_bolt().get_executed().get(":all-time");
    }
    
    //over all time
    private static Map<GlobalStreamId, Double> getBoltLatency(ExecutorSummary execSummary) {
        boolean isBolt = execSummary.get_stats().get_specific().is_set_bolt();
        if(!isBolt){
            throw new RuntimeException("Developer error. This method should be called only from bolts!");
        }
        return execSummary.get_stats().get_specific().get_bolt().get_execute_ms_avg().get(":all-time");
    }
    

    
    /*
     * Auxiliary data structure
     */
    private static class TuplesInfo {
        private long _numTuples;
        private double _latency;
        
        public TuplesInfo(long numTuples, double latency) {
            _numTuples = numTuples;
            _latency = latency;
        }
        
        public long getNumTuples(){
            return _numTuples;
        }
        
        public double getLatency(){
            return _latency;
        }
    }      
    
    public static void main(String[] args){
        System.out.println(writeTopologyInfo());
    }
}
