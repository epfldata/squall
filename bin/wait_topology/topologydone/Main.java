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

    public static void main(String[] args) {
        String topName = args[0];
        if(topDone(topName)){
        	System.out.println("FINISHED");
        }else{
        	System.out.println("NOT_FINISHED");
        }
    }
     
    private static boolean topDone(String topName){
        Client client=getNimbusStub();
        try {
            ClusterSummary clusterInfo = client.getClusterInfo();
            Iterator<TopologySummary> topologyIter = clusterInfo.get_topologies_iterator();
            while(topologyIter.hasNext()){
                TopologySummary topologySummary= topologyIter.next();
                String topologyName = topologySummary.get_name();
                if (topologyName.equals(topName)){
                    return false;
                }
            }
        } catch (TException ex) {
            ex.printStackTrace();
        }
        return true;
    }

    private static Client getNimbusStub(){
        NimbusClient nimbus = new NimbusClient(NIMBUS_HOST, NIMBUS_THRIFT_PORT);
        Client client=nimbus.getClient();
        return client;
    }
}