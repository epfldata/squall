package theta_schedulers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.query_plans.HyracksPlan;
import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * To make this work on the cluster:
 *   a) on the cluster: change storm.yaml such that it contains "storm.scheduler" line
 *   b) on the cluster: jar file with the scheduler code put on storm/lib directory
 * 
* This demo scheduler make sure a spout named <code>special-spout</code> in topology <code>special-topology</code> runs
* on a supervisor named <code>special-supervisor</code>. supervisor does not have name? You can configure it through
* the config: <code>supervisor.scheduler.meta</code> -- actually you can put any config you like in this config item.
*
* In our example, we need to put the following config in supervisor's <code>storm.yaml</code>:
* <pre>
* # give our supervisor a name: "special-supervisor"
* supervisor.scheduler.meta:
* name: "special-supervisor"
* </pre>
*
* Put the following config in <code>nimbus</code>'s <code>storm.yaml</code>:
* <pre>
* # tell nimbus to use this custom scheduler
* storm.scheduler: "storm.DemoScheduler"
* </pre>
* @author xumingmingv May 19, 2012 11:10:43 AM
*/
public class ReschufflerJoinerCollocateScheduler implements IScheduler {
	private static Logger LOG = Logger.getLogger(ReschufflerJoinerCollocateScheduler.class);

	public static final String RESHUFFLER_EXT="_RESHUFFLER";	
	
    public void schedule(Topologies topologies, Cluster cluster) {
    	LOG.info("ReschufflerJoinerCollocateScheduler: begin scheduling");

        Iterator<TopologyDetails> topIterator = topologies.getTopologies().iterator();
        while(topIterator.hasNext()){
        	TopologyDetails topology = topIterator.next();
            String topologyName = topology.getName();
        	LOG.info("Topology name is: " +  topologyName);

            boolean needsScheduling = cluster.needsScheduling(topology);
            if (!needsScheduling) {
            	LOG.info("Topology " + topologyName + " DOES NOT NEED scheduling.");
            } else {
            	LOG.info("Topology " + topologyName + " needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                LOG.info("needs scheduling(component->executor): " + componentToExecutors);
                // LOG.info("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName(topologyName).getId());
            	if (currentAssignment != null) {
            		LOG.info("current assignments: " + currentAssignment.getExecutorToSlot());
            	} else {
            		LOG.info("current assignments: {}");
            	}
            	
            	collocateAll(cluster, topology);
            }        
        }
        
        // check if we have at least one available slot
        if(cluster.getAvailableSlots().isEmpty()){
        	throw new RuntimeException("Scheduler: No available slots for EvenScheduler!");
        }
        
        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }

    /*
     * Collocates all the reshufflers and the corresponding joiners
     * This methods is mostly about finding the corresponding ones
     */
	private void collocateAll(Cluster cluster, TopologyDetails topology) {
		Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
		if(!componentToExecutors.isEmpty()){		
			Iterator<String> compNameIterator = componentToExecutors.keySet().iterator();
			while(compNameIterator.hasNext()){
				String compName = compNameIterator.next();
				if (compName.endsWith(RESHUFFLER_EXT)){
					List<ExecutorDetails> reshufflers = componentToExecutors.get(compName);
					String correspondingJoinerName = compName.substring(0, compName.indexOf(RESHUFFLER_EXT));
					List<ExecutorDetails> joiners = componentToExecutors.get(correspondingJoinerName);
		        	collocate(cluster, topology, reshufflers, joiners);
				}
			}
        }
	}

	/*
	 * Collocate one executor from first and one from second on the same worker.
	 * The size of first and second has to be the same.
	 */
	private void collocate(Cluster cluster, TopologyDetails topology,
			List<ExecutorDetails> first, List<ExecutorDetails> second) {
		for(int i=0; i < first.size(); i++){
    		//collocate a joiner and reshuffler executor
    		ExecutorDetails reshuffler = first.get(i);
    		ExecutorDetails joiner = second.get(i);
    		List<ExecutorDetails> collocatedExecutors = new ArrayList<ExecutorDetails>(
    			Arrays.asList(reshuffler, joiner));
    	
    		//find available workers
    		List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
    		if(availableSlots.isEmpty()){
    			throw new RuntimeException("Scheduler: No available slots for ReschufflerJoinerCollocateScheduler!");
    		}	
    		WorkerSlot availableWorker = availableSlots.get(0);
   	
    		//assign the collocation to the worker
    		cluster.assign(availableWorker, topology.getId(), collocatedExecutors);
    		LOG.info("We assigned executors:" + collocatedExecutors + 
   				" to slot: [" + availableWorker.getNodeId() + ", " + availableWorker.getPort() + "]");
    	}
	}

	@Override
	public void prepare(Map conf) {
		// TODO Auto-generated method stub
		
	}

}