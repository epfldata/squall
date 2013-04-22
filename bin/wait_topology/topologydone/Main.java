/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package topologydone;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import java.util.Iterator;
import org.apache.thrift7.TException;

public class Main {
    private static final String NIMBUS_HOST = "icdatasrv5";
    private static final int NIMBUS_THRIFT_PORT = 6627;
    
    private static final int FINISHED = 0;
    private static final int NOT_FINISHED = 1;
    private static final int KILLED = 2; //KILLED, BUT NOT REMOVED FROM THE UI

    public static void main(String[] args) {
        String topName = args[0];
        int status = topDone(topName);
        if(status == FINISHED){
        	System.out.println("FINISHED");
        }else if (status == NOT_FINISHED){
        	System.out.println("NOT_FINISHED");
        }else {
        	System.out.println("KILLED");
        }
    }
     
    private static int topDone(String topName){
        Client client=getNimbusStub();
        try {
            ClusterSummary clusterInfo = client.getClusterInfo();
            Iterator<TopologySummary> topologyIter = clusterInfo.get_topologies_iterator();
            while(topologyIter.hasNext()){
                TopologySummary topologySummary= topologyIter.next();
                String topologyName = topologySummary.get_name();
                if (topologyName.equals(topName)){
                	String status = topologySummary.get_status();
                	if(status.equalsIgnoreCase("ACTIVE")){
                		return NOT_FINISHED;
                	}else{
                		return KILLED;
                	}
                }
            }
        } catch (TException ex) {
            ex.printStackTrace();
        }
        return FINISHED;
    }

    private static Client getNimbusStub(){
        NimbusClient nimbus = new NimbusClient(NIMBUS_HOST, NIMBUS_THRIFT_PORT);
        Client client=nimbus.getClient();
        return client;
    }
}