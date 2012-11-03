package plan_runner.main;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.Component;
import plan_runner.query_plans.HyracksPlan;
import plan_runner.query_plans.HyracksPreAggPlan;
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.RSTPlan;
import plan_runner.query_plans.TPCH10Plan;
import plan_runner.query_plans.TPCH3Plan;
import plan_runner.query_plans.TPCH4Plan;
import plan_runner.query_plans.TPCH5Plan;
import plan_runner.query_plans.debug.TPCH5PlanAvg;
import plan_runner.query_plans.TPCH7Plan;
import plan_runner.query_plans.TPCH8Plan;
import plan_runner.query_plans.TPCH9Plan;
import plan_runner.query_plans.ThetaHyracksPlan;
import plan_runner.query_plans.ThetaInputDominatedPlan;
import plan_runner.query_plans.ThetaMultipleJoinPlan;
import plan_runner.query_plans.ThetaOutputDominatedPlan;
import plan_runner.query_plans.ThetaTPCH7Plan;
import plan_runner.query_plans.debug.HyracksL1Plan;
import plan_runner.query_plans.debug.HyracksL3BatchPlan;
import plan_runner.query_plans.debug.HyracksL3Plan;
import plan_runner.query_plans.debug.TPCH3L1Plan;
import plan_runner.query_plans.debug.TPCH3L23Plan;
import plan_runner.query_plans.debug.TPCH3L2Plan;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormJoin;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.StormWrapper;
import plan_runner.utilities.SystemParameters;

public class Main {
	private static Logger LOG = Logger.getLogger(Main.class);
        
        public static void main(String[] args) {
           new Main(args);
        }

        public Main(String[] args){
            String confPath = args[0];
            Config conf = SystemParameters.fileToStormConfig(confPath);
            QueryPlan queryPlan = chooseQueryPlan(conf);
            
            addVariablesToMap(conf, confPath);
            putBatchSizes(queryPlan, conf);
	    TopologyBuilder builder = createTopology(queryPlan, conf);
            StormWrapper.submitTopology(conf, builder);
        }

        public Main(QueryPlan queryPlan, Map map, String confPath){
            Config conf = SystemParameters.mapToStormConfig(map);
            
            addVariablesToMap(conf, confPath);
            putBatchSizes(queryPlan, conf);
            TopologyBuilder builder = createTopology(queryPlan, conf);
            StormWrapper.submitTopology(conf, builder);
        }
        
        private static void addVariablesToMap(Map map, String confPath){
            //setting topologyName: DIP_TOPOLOGY_NAME_PREFIX + CONFIG_FILE_NAME
            String confFilename = MyUtilities.getPartFromEnd(confPath, 0);
            String prefix = SystemParameters.getString(map, "DIP_TOPOLOGY_NAME_PREFIX");
            String topologyName = prefix + "_" + confFilename;
            SystemParameters.putInMap(map, "DIP_TOPOLOGY_NAME", topologyName);
        }
        
        //this method is a skeleton for more complex ones
        //  an optimizer should do this in a smarter way
        private static void putBatchSizes(QueryPlan plan, Map map) {
            if(SystemParameters.isExisting(map, "BATCH_SIZE")){
                
                //if the batch mode is specified, but nothing is put in map yet (because other than MANUAL_BATCH optimizer is used)
                String firstBatch = plan.getComponentNames().get(0) + "_BS";
                if(!SystemParameters.isExisting(map, firstBatch)){
                    String batchSize = SystemParameters.getString(map, "BATCH_SIZE");
                    for(String compName: plan.getComponentNames()){
                        String batchStr = compName + "_BS";                
                        SystemParameters.putInMap(map, batchStr, batchSize);
                    }
                }
                
                //no matter where this is set, we print out batch sizes of components
                for(String compName: plan.getComponentNames()){
                        String batchStr = compName + "_BS";
                        String batchSize = SystemParameters.getString(map, batchStr);
                        LOG.info("Batch size for " + compName + " is " + batchSize);
                }
            }
            if(!MyUtilities.checkSendMode(map)){
                throw new RuntimeException("BATCH_SEND_MODE value is not recognized.");
            }
        } 

        private static TopologyBuilder createTopology(QueryPlan qp, Config conf) {
            TopologyBuilder builder = new TopologyBuilder();
            TopologyKiller killer = new TopologyKiller(builder);

            //DST_ORDERING is the optimized version, so it's used by default
            int partitioningType = StormJoin.DST_ORDERING;

            List<Component> queryPlan = qp.getPlan();
            List<String> allCompNames = qp.getComponentNames();
            Collections.sort(allCompNames);
            int planSize = queryPlan.size();
            for(int i=0;i<planSize;i++){
                Component component = queryPlan.get(i);
                if(component.getChild() == null){
                    //a last component (it might be multiple of them)
                    component.makeBolts(builder, killer, allCompNames, conf, partitioningType, StormComponent.FINAL_COMPONENT);
                }else{
                    component.makeBolts(builder, killer, allCompNames, conf, partitioningType, StormComponent.INTERMEDIATE);
                }  
            }

            // printing infoID information and returning the result
            //printInfoID(killer, queryPlan); commented out because IDs are now desriptive names
            return builder;
        }

        private static void printInfoID(TopologyKiller killer,
                List<Component> queryPlan) {

            StringBuilder infoID = new StringBuilder("\n");
            if(killer!=null){
                infoID.append(killer.getInfoID());
                infoID.append("\n");
            }
            infoID.append("\n");

            // after creating bolt, ID of a component is known
            int planSize = queryPlan.size();
            for(int i=0;i<planSize;i++){
                Component component = queryPlan.get(i);
                infoID.append(component.getInfoID());
                infoID.append("\n\n");
            }

            LOG.info(infoID.toString());
        }


        public static QueryPlan chooseQueryPlan(Map conf){
            String queryName = SystemParameters.getString(conf, "DIP_QUERY_NAME");
            //if "/" is the last character, adding one more is not a problem
            String dataPath = SystemParameters.getString(conf, "DIP_DATA_PATH") + "/";
            String extension = SystemParameters.getString(conf, "DIP_EXTENSION");

            QueryPlan queryPlan = null;

            // change between this and ...
            if(queryName.equalsIgnoreCase("rst")){
                queryPlan = new RSTPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("hyracks")){
                queryPlan = new HyracksPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("hyracks_pre_agg")){
                queryPlan = new HyracksPreAggPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("hyracks_l1")){
                queryPlan = new HyracksL1Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("hyracks_l3")){
                queryPlan = new HyracksL3Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("hyracks_l3_batch")){
                queryPlan = new HyracksL3BatchPlan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch3")){
                queryPlan = new TPCH3Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tcph3_l1")){
                queryPlan = new TPCH3L1Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch3_l2")){
                queryPlan = new TPCH3L2Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch3_l23")){
                queryPlan = new TPCH3L23Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch4")){
                queryPlan = new TPCH4Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch5")){
                queryPlan = new TPCH5Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch5avg")){
                queryPlan = new TPCH5PlanAvg(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch7")){
                queryPlan = new TPCH7Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch8")){
                queryPlan = new TPCH8Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch9")){
                queryPlan = new TPCH9Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("tpch10")){
                queryPlan = new TPCH10Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("theta_tpch7")){
            	queryPlan = new ThetaTPCH7Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("theta_input_dominated")){
            	queryPlan = new ThetaInputDominatedPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("theta_output_dominated")){
            	queryPlan = new ThetaOutputDominatedPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("theta_multiple_join")){
            	queryPlan = new ThetaMultipleJoinPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("theta_hyracks")){
            	queryPlan = new ThetaHyracksPlan(dataPath, extension, conf).getQueryPlan();
            }
            // ... this line

            if (queryPlan == null){
                throw new RuntimeException("QueryPlan " + queryName + " doesn't exist in Main.java");
            }
            return queryPlan;
        }
}
