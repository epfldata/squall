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

    private static final int WAIT_SUBMIT = 3000;
    private static final int TIMEOUT_INVOKE = 1000;

    private static boolean _firstInvocation = true;

    public static void main(String[] args) {
        String topName = args[0];
        sleep(WAIT_SUBMIT);
        while(!topDone(topName)){
            _firstInvocation = false;
            sleep(TIMEOUT_INVOKE);
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
	if(_firstInvocation){
	  System.out.println("... Most probably topology " + topName + " did not even start.");
	}
        return true;
    }

    private static Client getNimbusStub(){
        NimbusClient nimbus = new NimbusClient(NIMBUS_HOST, NIMBUS_THRIFT_PORT);
        Client client=nimbus.getClient();
        return client;
    }

    private static void sleep(long timeout){
        try{
            Thread.sleep(timeout);
        } catch(InterruptedException ie){
            //If this thread was intrrupted by nother thread
        }
    }

}