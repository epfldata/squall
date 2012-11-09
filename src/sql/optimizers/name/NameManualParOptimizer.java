
package sql.optimizers.name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import plan_runner.components.Component;
import plan_runner.query_plans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

/*
 * For lefty plans with explicitly specified parallelism
 */
public class NameManualParOptimizer implements Optimizer{
    private Map _map;
    private SQLVisitor _pq;
    
    private List<String> _compNames = new ArrayList<String>(); // all the sources in the appropriate order
    private Map<String, Integer> _compNamePar = new HashMap<String, Integer>(); // all components(including joins) with its parallelism
    
    public NameManualParOptimizer(Map map) {
        _map = map;
        _pq = ParserUtil.parseQuery(map);

        try{
            parse();
        }catch(ArrayIndexOutOfBoundsException a){
            throw new RuntimeException("Invalid DIP_PLAN setting in config file!");
        }
    }
    
    public QueryPlan generate() {
        NameCompGenFactory factory = new NameCompGenFactory(_map, _pq.getTan());
        NameCompGen ncg = factory.create();
        // YANNIS: FIX FOR PLANS HAVING ONLY ONE DATA SOURCE
	boolean isOnlyComp = _compNames.size() == 1;
        
        Component first = ncg.generateDataSource(_compNames.get(0), isOnlyComp);
        for(int i=1; i<_compNames.size();i++){
            Component second = ncg.generateDataSource(_compNames.get(i), isOnlyComp);
            first = ncg.generateEquiJoin(first, second);
        }
        
        ParserUtil.parallelismToMap(_compNamePar, _map);
        
        return ncg.getQueryPlan();
    }    

    //HELPER methods
    private void parse() {
        String plan = SystemParameters.getString(_map, "DIP_PLAN");
        String[] components = plan.split(",");
        
        String firstComponent = components[0];
        String[] firstParts = firstComponent.split(":");
        String firstCompName = firstParts[0];
        int firstParallelism = Integer.valueOf(firstParts[1]);
        
        putSource(firstCompName, firstParallelism);
        
        for(int i=1; i<components.length;i++){
            String secondComponent = components[i];
            String[] secondParts = secondComponent.split(":");
            String secondCompName = secondParts[0];
            int secondPar = Integer.valueOf(secondParts[1]);
            int secondJoinPar = Integer.valueOf(secondParts[2]);
            putSource(secondCompName, secondPar);
            String joinCompName = firstCompName + "_" + secondCompName;
            putJoin(joinCompName, secondJoinPar);
            firstCompName = joinCompName;
        }
    }

    private void putSource(String compName, int par) {
        _compNames.add(compName);
        _compNamePar.put(compName, par);
    }
    
    private void putJoin(String compName, int par) {
        _compNamePar.put(compName, par);
    }

}
