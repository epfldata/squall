package main;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import org.apache.log4j.Logger;

import components.Component;
import java.util.List;
import java.util.Map;
import queryPlans.HyracksPlan;
import queryPlans.HyracksPreAggPlan;
import queryPlans.QueryPlan;
import queryPlans.RSTPlan;
import queryPlans.TPCH3Plan;
import queryPlans.TPCH4Plan;
import queryPlans.TPCH5Plan;
import queryPlans.TPCH7Plan;
import queryPlans.TPCH8Plan;
import stormComponents.StormJoin;
import stormComponents.StormComponent;
import stormComponents.synchronization.Flusher;
import stormComponents.synchronization.TopologyKiller;
import stormComponents.synchronization.TrafficLight;
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
            TopologyKiller killer= new TopologyKiller(builder);
            Flusher flusher = null;
            TrafficLight trafficLight= null;

            //DST_ORDERING is the optimized version, so it's used by default
            int partitioningType = StormJoin.DST_ORDERING;

            List<Component> queryPlan = qp.getPlan();
            int planSize = queryPlan.size();
            for(int i=0;i<planSize;i++){
                Component component = queryPlan.get(i);
                if(i == planSize - 1){
                    //very last element
                    component.makeBolts(builder, killer, flusher, trafficLight, conf, partitioningType, StormComponent.FINAL_COMPONENT);
                }else{
                    component.makeBolts(builder, killer, flusher, trafficLight, conf, partitioningType, StormComponent.INTERMEDIATE);
                }  
            }

            // printing infoID information and returning the result
            printInfoID(killer, flusher, trafficLight, queryPlan);
            return builder;
        }

        private static void printInfoID(TopologyKiller killer,
                Flusher flusher,
                TrafficLight trafficLight,
                List<Component> queryPlan) {

            StringBuilder infoID = new StringBuilder("\n");
            if(killer!=null){
                infoID.append(killer.getInfoID());
                infoID.append("\n");
            }
            if(flusher!=null){
                infoID.append(flusher.getInfoID());
                infoID.append("\n");
            }
            if(trafficLight!=null){
                infoID.append(trafficLight.getInfoID());
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
            }else if(queryName.equalsIgnoreCase("TPCH3")){
                queryPlan = new TPCH3Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH4")){
                queryPlan = new TPCH4Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH5")){
                queryPlan = new TPCH5Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH7")){
                queryPlan = new TPCH7Plan(dataPath, extension, conf).getQueryPlan();
            }else if(queryName.equalsIgnoreCase("TPCH8")){
                queryPlan = new TPCH8Plan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("Hyracks")){
                queryPlan = new HyracksPlan(dataPath, extension, conf).getQueryPlan();
            }else if (queryName.equalsIgnoreCase("HyracksPreAgg")){
                queryPlan = new HyracksPreAggPlan(dataPath, extension, conf).getQueryPlan();
            }
            // ... this line

            if (queryPlan == null){
                throw new RuntimeException("QueryPlan " + queryName + " doesn't exist in Main.java");
            }
            return queryPlan;
        }
}
