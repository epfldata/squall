package main;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import optimizers.ComponentGenerator;
import optimizers.rule.RuleOptimizer;
import optimizers.Optimizer;
import optimizers.OptimizerTranslator;
import optimizers.IndexTranslator;
import queryPlans.QueryPlan;
import schema.Schema;
import schema.TPCH_Schema;
import util.ParserUtil;
import util.TableAliasName;
import utilities.SystemParameters;
import visitors.jsql.SQLVisitor;

public class ParserMain{
    private final int CLUSTER_WORKERS = 176;
    private int CLUSTER_ACKERS = 17;
    
    private final int LOCAL_WORKERS = 5;
    private int LOCAL_ACKERS = 1;

    private final String sqlExtension = ".sql";

    public static void main(String[] args){
        String parserConfPath = args[0];
        new ParserMain(parserConfPath);
    }

    //String[] sizes: {"1G", "2G", "4G", ...}
    public ParserMain(String parserConfPath){
        Map map = SystemParameters.fileToMap(parserConfPath);

        if(!SystemParameters.getBoolean(map, "DIP_ACK_EVERY_TUPLE")){
            //we don't ack after each tuple is sent, 
            //  so we don't need any node to be dedicated for acking
            CLUSTER_ACKERS = 0;
            LOCAL_ACKERS = 0;
        }

        String mode = "";
        if (SystemParameters.getBoolean(map, "DIP_DISTRIBUTED")){
            mode = "parallel";
            SystemParameters.putInMap(map, "DIP_NUM_PARALLELISM", CLUSTER_WORKERS);
            SystemParameters.putInMap(map, "DIP_NUM_ACKERS", CLUSTER_ACKERS);
        }else{
            mode = "serial";
            SystemParameters.putInMap(map, "DIP_NUM_PARALLELISM", LOCAL_WORKERS);
            SystemParameters.putInMap(map, "DIP_NUM_ACKERS", LOCAL_ACKERS);
        }

        String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
        String sqlPath = SystemParameters.getString(map, "DIP_SQL_ROOT") + queryName + sqlExtension;
        String sqlstring = ParserUtil.readStringFromFile(sqlPath);

        String dbSize = SystemParameters.getString(map, "DIP_DB_SIZE") + "G";
        String srcParallelism = SystemParameters.getString(map, "DIP_MAX_SRC_PAR");
        String dataRoot = SystemParameters.getString(map, "DIP_DATA_ROOT");
        String dataPath = dataRoot + "/" + dbSize + "/";

        SystemParameters.putInMap(map, "DIP_DATA_PATH" , dataPath);
        String topologyName = dbSize + "_" + queryName + "_" + mode + "_" + srcParallelism;
        SystemParameters.putInMap(map, "DIP_TOPOLOGY_NAME", topologyName);

        String extension = SystemParameters.getString(map, "DIP_EXTENSION");
        QueryPlan plan = sqlToPlan(sqlstring, dataPath, extension, map);
        new Main(plan, map);
    }

    private QueryPlan sqlToPlan(String sql, String dataPath, String extension, Map map){
        CCJSqlParserManager pm = new CCJSqlParserManager();
        Statement statement=null;
        try {
            statement = pm.parse(new StringReader(sql));
        } catch (JSQLParserException ex) {
            System.out.println("JSQLParserException");
        }

        if (statement instanceof Select) {
            Select selectStatement = (Select) statement;
            SQLVisitor parser = new SQLVisitor();

            //visit whole SELECT statement
            parser.visit(selectStatement);

            // print out all the tables
            List<Table> tableList = parser.getTableList();
            for(Table table: tableList){
                String tableStr = ParserUtil.toString(table);
                System.out.println(tableStr);
            }

            //print all the joins
            List<Join> joinList = parser.getJoinList();
            for(Join join: joinList){
                String joinStr = ParserUtil.toString(join);
                System.out.println(joinStr);
            }

            List<SelectItem> selectItems = parser.getSelectItems();

            Expression whereExpr = parser.getWhereExpr();

            double scallingFactor = SystemParameters.getDouble(map, "DIP_DB_SIZE");
            return generatePlan(tableList, joinList, selectItems, whereExpr, new TPCH_Schema(scallingFactor), dataPath, extension, map);
        }
        throw new RuntimeException("Please provide SELECT statement!");
    }

    private final QueryPlan generatePlan(List<Table> tableList,
            List<Join> joinList,
            List<SelectItem> selectItems,
            Expression whereExpr,
            Schema schema,
            String dataPath,
            String extension,
            Map map){
        TableAliasName tan = new TableAliasName(tableList);

        //works both for simple and rule-based optimizer
        OptimizerTranslator ot = new IndexTranslator(schema, tan);

        //Simple optimizer provides lefty plans
        //Optimizer opt = new SimpleOpt(schema, tan, dataPath, extension, ot, map);
        //Dynamic programming query plan
        Optimizer opt = new RuleOptimizer(schema, tan, dataPath, extension, ot, map);

        return opt.generate(tableList, joinList, selectItems, whereExpr);
        
    }
   
}