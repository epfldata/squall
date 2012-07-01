package sql.main;

import plan_runner.components.DataSourceComponent;
import java.io.StringReader;
import java.util.Map;
import plan_runner.main.Main;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import plan_runner.components.Component;
import sql.optimizers.Optimizer;
import sql.optimizers.cost.CostOptimizer;
import sql.optimizers.cost.NameComponentGenerator;
import sql.optimizers.rule.RuleOptimizer;
import plan_runner.queryPlans.QueryPlan;
import sql.util.ParserUtil;
import plan_runner.utilities.SystemParameters;
import sql.visitors.jsql.SQLVisitor;

public class ParserMain{
    //private final int CLUSTER_WORKERS = 176;
    private static int CLUSTER_ACKERS = 17; //could be 10% of CLUSTER_WORKERS, but this is a magic number in our system

    private static int LOCAL_ACKERS = 1;

    private final static String sqlExtension = ".sql";
 
    public static void main(String[] args){     
        String parserConfPath = args[0];
        ParserMain pm = new ParserMain();
        
        Map map = pm.createConfig(parserConfPath);
        SQLVisitor parsedQuery = pm.parseQuery(map);
        QueryPlan plan = pm.generatePlan(parsedQuery, map);
        
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
        String srcParallelism = SystemParameters.getString(map, "DIP_MAX_SRC_PAR");
        String dataRoot = SystemParameters.getString(map, "DIP_DATA_ROOT");
        String dataPath = dataRoot + "/" + dbSize + "/";

        String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
        SystemParameters.putInMap(map, "DIP_DATA_PATH" , dataPath);
        String topologyName = dbSize + "_" + queryName + "_" + mode + "_" + srcParallelism;
        SystemParameters.putInMap(map, "DIP_TOPOLOGY_NAME", topologyName);

        return map;
    }
    
    public SQLVisitor parseQuery(Map map){
        String sqlString = readSQL(map);
        
        CCJSqlParserManager pm = new CCJSqlParserManager();
        Statement statement=null;
        try {
            statement = pm.parse(new StringReader(sqlString));
        } catch (JSQLParserException ex) {
            System.out.println("JSQLParserException");
        }

        if (statement instanceof Select) {
            Select selectStatement = (Select) statement;
            SQLVisitor parsedQuery = new SQLVisitor();

            //visit whole SELECT statement
            parsedQuery.visit(selectStatement);
            parsedQuery.doneVisiting();

            return parsedQuery;
        }
        throw new RuntimeException("Please provide SELECT statement!");
    }
    
    private static String readSQL(Map map){
        String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
        String sqlPath = SystemParameters.getString(map, "DIP_SQL_ROOT") + queryName + sqlExtension;
        return ParserUtil.readSQLFromFile(sqlPath);
    }    

    public QueryPlan generatePlan(SQLVisitor pq, Map map){
        //Simple optimizer provides lefty plans
        //Optimizer opt = new SimpleOpt(pq, map);
        //Dynamic programming query plan
        Optimizer opt = new RuleOptimizer(pq, map);

        return opt.generate();
    }
    
}