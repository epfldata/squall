
package sql.optimizers.name;

import java.util.List;
import java.util.Map;
import plan_runner.components.Component;
import plan_runner.queryPlans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.util.ParserUtil;

/*
 * For lefty plans, parallelism obtained from cost formula
 */
public class NameRuleOptimizer implements Optimizer{
    private Map _map;
    
    public NameRuleOptimizer(Map map) {
        _map = map;
    }
    
    public QueryPlan generate() {
        int totalParallelism = SystemParameters.getInt(_map, "DIP_TOTAL_SRC_PAR");
        NameCompGenFactory factory = new NameCompGenFactory(_map, totalParallelism);
        
        //sorted by increasing cardinalities
        List<String> sourceNames = factory.getParAssigner().getSortedSourceNames();
        
        NameCompGen ncg = factory.create();
        Component first = ncg.generateDataSource(sourceNames.get(0));
        for(int i=1; i<sourceNames.size();i++){
            Component second = ncg.generateDataSource(sourceNames.get(i));
            first = ncg.generateEquiJoin(first, second);
        }
        
        ParserUtil.parallelismToMap(ncg, _map);
        
        return ncg.getQueryPlan();
    }    

}