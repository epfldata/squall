
package sql.optimizers.name;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import plan_runner.components.Component;
import plan_runner.query_plans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

/*
 * For lefty plans, parallelism obtained from cost formula
 */
public class NameManualOptimizer implements Optimizer{
    private Map _map;
    private SQLVisitor _pq;
    
    private List<String> _compNames = new ArrayList<String>(); // all the sources in the appropriate order
    
    public NameManualOptimizer(Map map) {
        _map = map;
        _pq = ParserUtil.parseQuery(map);

        parse();
    }
    
    public QueryPlan generate() {
        int totalParallelism = SystemParameters.getInt(_map, "DIP_TOTAL_SRC_PAR");
        NameCompGenFactory factory = new NameCompGenFactory(_map, _pq.getTan(), totalParallelism);
        NameCompGen ncg = factory.create();
        
        // YANNIS: FIX FOR PLANS HAVING ONLY ONE DATA SOURCE
        //  look at costOptimizer
        Component first = ncg.generateDataSource(_compNames.get(0));
        for(int i=1; i<_compNames.size();i++){
            Component second = ncg.generateDataSource(_compNames.get(i));
            first = ncg.generateEquiJoin(first, second);
        }
        
        ParserUtil.parallelismToMap(ncg, _map);
        
        return ncg.getQueryPlan();
    }    

    //HELPER methods
    private void parse() {
        String plan = SystemParameters.getString(_map, "DIP_PLAN");
        String[] components = plan.split(",");
        
        _compNames.addAll(Arrays.asList(components));
    }

}