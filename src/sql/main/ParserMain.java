package sql.main;

import java.util.Map;
import plan_runner.main.Main;
import plan_runner.queryPlans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.optimizers.index.IndexRuleOptimizer;
import sql.optimizers.index.IndexSimpleOptimizer;
import sql.optimizers.name.NameCostOptimizer;
import sql.optimizers.name.NameManualOptimizer;
import sql.optimizers.name.NameManualParOptimizer;
import sql.optimizers.name.NameRuleOptimizer;
import sql.util.ParserUtil;

public class ParserMain{
    //private final int CLUSTER_WORKERS = 176;
    private static int CLUSTER_ACKERS = 17; //could be 10% of CLUSTER_WORKERS, but this is a magic number in our system

    private static int LOCAL_ACKERS = 1;
 
    public static void main(String[] args){     
        String parserConfPath = args[0];
        ParserMain pm = new ParserMain();
        
        Map map = pm.createConfig(parserConfPath);
        QueryPlan plan = pm.generatePlan(map);
        
        System.out.println(ParserUtil.toString(plan));
        System.out.println(ParserUtil.parToString(plan, map));
        
        new Main(plan, map);
    }
    
    //String[] sizes: {"1G", "2G", "4G", ...}
    public Map createConfig(String parserConfPath){
        Map map = SystemParameters.fileToMap(parserConfPath);

        if(!SystemParameters.getBoolean(map, "DIP_ACK_EVERY_TUPLE")){
            //we don't ack after each tuple is sent, 
            //  so we don't need any node to be dedicated for acking
            CLUSTER_ACKERS = 0;
            LOCAL_ACKERS = 0;
        }

        String mode;
        if (SystemParameters.getBoolean(map, "DIP_DISTRIBUTED")){
            mode = "parallel";
            //default value is already set, but for scheduling we might need to change that
            //SystemParameters.putInMap(map, "DIP_NUM_WORKERS", CLUSTER_WORKERS);
            SystemParameters.putInMap(map, "DIP_NUM_ACKERS", CLUSTER_ACKERS);
        }else{
            mode = "serial";
            SystemParameters.putInMap(map, "DIP_NUM_ACKERS", LOCAL_ACKERS);
        }

        String dbSize = SystemParameters.getString(map, "DIP_DB_SIZE") + "G";
        String dataRoot = SystemParameters.getString(map, "DIP_DATA_ROOT");
        String dataPath = dataRoot + "/" + dbSize + "/";

        String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
        SystemParameters.putInMap(map, "DIP_DATA_PATH" , dataPath);
        String topologyName = dbSize + "_" + queryName + "_" + mode;
        SystemParameters.putInMap(map, "DIP_TOPOLOGY_NAME", topologyName);

        return map;
    }

    public QueryPlan generatePlan(Map map){
        Optimizer opt = pickOptimizer(map);
        return opt.generate();
    }

    private Optimizer pickOptimizer(Map map) {
        String optStr = SystemParameters.getString(map, "DIP_OPTIMIZER_TYPE");
        System.out.println("Selected optimizer: " + optStr);
        if("INDEX_SIMPLE".equalsIgnoreCase(optStr)){
            //Simple optimizer provides lefty plans
            return new IndexSimpleOptimizer(map); 
        }else if("INDEX_RULE_BUSHY".equalsIgnoreCase(optStr)){
            return new IndexRuleOptimizer(map);
        }else if("NAME_MANUAL_PAR_LEFTY".equalsIgnoreCase(optStr)){
            return new NameManualParOptimizer(map);
        }else if("NAME_MANUAL_COST_LEFTY".equalsIgnoreCase(optStr)){
            return new NameManualOptimizer(map);
        }else if("NAME_RULE_LEFTY".equalsIgnoreCase(optStr)){
            return new NameRuleOptimizer(map);
        }else if("NAME_COST_LEFTY".equalsIgnoreCase(optStr)){
            return new NameCostOptimizer(map);
        }
        throw new RuntimeException("Unknown " + optStr + " optimizer!");
    }
    
}