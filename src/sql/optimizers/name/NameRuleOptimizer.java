
package sql.optimizers.name;

import java.util.List;
import java.util.Map;
import plan_runner.components.Component;
import plan_runner.queryPlans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

/*
 * For lefty plans, parallelism obtained from cost formula
 */
public class NameRuleOptimizer implements Optimizer{
    private Map _map;
    private SQLVisitor _pq;
    
    public NameRuleOptimizer(Map map) {
        _map = map;
        _pq = ParserUtil.parseQuery(map);
    }
    
    public QueryPlan generate() {
        int totalParallelism = SystemParameters.getInt(_map, "DIP_TOTAL_SRC_PAR");
        NameCompGenFactory factory = new NameCompGenFactory(_map, totalParallelism);
        
        //sorted by increasing cardinalities
        List<String> sourceNames = factory.getParAssigner().getSortedSourceNames();
        
        NameCompGen ncg = factory.create();
        Component first = ncg.generateDataSource(sourceNames.remove(0));
        int numSources = sourceNames.size(); //first component is already removed
        for(int i=0; i < numSources; i++){
            String secondStr = chooseSmallestSource(first, sourceNames);
            Component second = ncg.generateDataSource(secondStr);
            first = ncg.generateEquiJoin(first, second);
        }
        
        ParserUtil.parallelismToMap(ncg, _map);
        
        return ncg.getQueryPlan();
    }

    
    /*
     * Take last component(LC) from ncg, 
     * and remove the Source with the smallest cardinality which can be joined with LC.
     * 
     * This method has side effects: removing from sourceNames collection
     */
    private String chooseSmallestSource(Component lastComp, List<String> sourceNames) {
        for(int i=0; i< sourceNames.size(); i++){
            String candidateComp = sourceNames.get(i);
            if(_pq.getJte().joinExistsBetween(candidateComp, ParserUtil.getSourceNameList(lastComp))){
                sourceNames.remove(i);
                return candidateComp;
            }
        }
        throw new RuntimeException("Should not be here! No components to join with!");
    }

}