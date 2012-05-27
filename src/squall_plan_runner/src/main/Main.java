package main;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import org.apache.log4j.Logger;

import components.Component;
import java.util.List;
import java.util.Map;
import queryPlans.debug.HyracksL1Plan;
import queryPlans.debug.HyracksL3BatchPlan;
import queryPlans.debug.HyracksL3Plan;
import queryPlans.HyracksPlan;
import queryPlans.HyracksPreAggPlan;
import queryPlans.QueryPlan;
import queryPlans.RSTPlan;
import queryPlans.TPCH10Plan;
import queryPlans.debug.TPCH3L1Plan;
import queryPlans.debug.TPCH3L23Plan;
import queryPlans.debug.TPCH3L2Plan;
import queryPlans.TPCH3Plan;
import queryPlans.TPCH4Plan;
import queryPlans.TPCH5Plan;
import queryPlans.TPCH7Plan;
import queryPlans.TPCH8Plan;
import queryPlans.TPCH9Plan;
import queryPlans.TestThetaJoin;
import stormComponents.StormJoin;
import stormComponents.StormComponent;
import stormComponents.synchronization.TopologyKiller;
import utilities.StormWrapper;
import utilities.SystemParameters;


public class Main {
	private static Logger LOG = Logger.getLogger(Main.class);

        public static void main(String[] args) {
           new Main(args);
        }

        public Main(String[] args){
            String propertiesPath = args[0];
            Config conf = SystemParameters.fileToStormConfig(propertiesPath);

            QueryPlan queryPlan = chooseQueryPlan(conf);
            TopologyBuilder builder = createTopology(queryPlan, conf);
            StormWrapper.submitTopology(conf, builder);
        }

        public Main(QueryPlan queryPlan, Map map){
            Config conf = SystemParameters.mapToStormConfig(map);
            TopologyBuilder builder = createTopology(queryPlan, conf);
            StormWrapper.submitTopology(conf, builder);
        }

        private static TopologyBuilder createTopology(QueryPlan qp, Config conf) {
            TopologyBuilder builder = new TopologyBuilder();
            TopologyKiller killer = new TopologyKiller(builder);

            //DST_ORDERING is the optimized version, so it's used by default
            int partitioningType = StormJoin.DST_ORDERING;

            List<Component> queryPlan = qp.getPlan();
            int planSize = queryPlan.size();
            for(int i=0;i<planSize;i++){
                Component component = queryPlan.get(i);
                if(component.getChild() == null){
                    //a last component (it might be multiple of them)
                    component.makeBolts(builder, killer, conf, partitioningType, StormComponent.FINAL_COMPONENT);
                }else{
                    component.makeBolts(builder, killer, conf, partitioningType, StormComponent.INTERMEDIATE);
                }  
            }

            // printing infoID information and returning the result
            printInfoID(killer, queryPlan);
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
            if(queryName.equalsIgnoreCase("RST")){
                queryPlan = new RSTPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("Hyracks")){
                queryPlan = new HyracksPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("HyracksPreAgg")){
                queryPlan = new HyracksPreAggPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("HyracksL1")){
                queryPlan = new HyracksL1Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("HyracksL3")){
                queryPlan = new HyracksL3Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("HyracksL3Batch")){
                queryPlan = new HyracksL3BatchPlan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH3")){
                queryPlan = new TPCH3Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH3L1")){
                queryPlan = new TPCH3L1Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH3L2")){
                queryPlan = new TPCH3L2Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH3L23")){
                queryPlan = new TPCH3L23Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH4")){
                queryPlan = new TPCH4Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH5")){
                queryPlan = new TPCH5Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH7")){
                queryPlan = new TPCH7Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH8")){
                queryPlan = new TPCH8Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH9")){
                queryPlan = new TPCH9Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH10")){
                queryPlan = new TPCH10Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("TestTheta")){
            	queryPlan = new TestThetaJoin(dataPath, extension, conf).getQueryPlan();
            }
            // ... this line

            if (queryPlan == null){
                throw new RuntimeException("QueryPlan " + queryName + " doesn't exist in Main.java");
            }
            return queryPlan;
        }
}
