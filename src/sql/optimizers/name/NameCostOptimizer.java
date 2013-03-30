
package sql.optimizers.name;

import java.util.*;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.query_plans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.util.ImproperParallelismException;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

/*
 * For lefty plans, parallelism obtained from cost formula
 */
public class NameCostOptimizer implements Optimizer{
	private static Logger LOG = Logger.getLogger(NameCostOptimizer.class);
	
    private SQLVisitor _pq;
    private Map _map;
    
    public NameCostOptimizer(Map map) {
        _map = map;
        _pq = ParserUtil.parseQuery(map);
    }
    
    public QueryPlan generate() {
        int totalSourcePar = SystemParameters.getInt(_map, "DIP_TOTAL_SRC_PAR");
        NameCompGenFactory factory = new NameCompGenFactory(_map, _pq.getTan(), totalSourcePar);
        List<String> sourceNames = factory.getParAssigner().getSortedSourceNames();
        int numSources = sourceNames.size();
        NameCompGen optimal = null;
        
        //**************creating single-relation plans********************
        if(numSources == 1){
            optimal = factory.create();
		// YANNIS: FIX FOR PLANS HAVING ONLY ONE DATA SOURCE
            optimal.generateDataSource(sourceNames.get(0), true);
        }
        
        //**************creating 2-way joins********************
        List<NameCompGen> ncgListFirst = new ArrayList<NameCompGen>();
        for(int i=0; i<numSources; i++){
            String firstCompName = sourceNames.get(i);
            List<String> joinedWith = _pq.getJte().getJoinedWithSingleDir(firstCompName);
            if(joinedWith != null){
                for(String secondCompName: joinedWith){
                    NameCompGen ncg = factory.create();
                    Component first = ncg.generateDataSource(firstCompName);
                    Component second = ncg.generateDataSource(secondCompName);
                    addEquiJoinNotSuboptimal(first, second, ncg, ncgListFirst);
                }
            }
        }
        if(numSources == 2){
            optimal = chooseBest(ncgListFirst);
        }
        
        //**************creating multi-way joins********************
        for(int level = 2; level < numSources; level++){ 
            List<NameCompGen> ncgListSecond = new ArrayList<NameCompGen>();
            for(int i = 0; i < ncgListFirst.size(); i++){
                NameCompGen ncg = ncgListFirst.get(i);
                Component firstComp = ncg.getQueryPlan().getLastComponent();
                List<String> ancestors = ParserUtil.getSourceNameList(firstComp);
                List<String> joinedWith = _pq.getJte().getJoinedWith(ancestors);
                for(String compName: joinedWith){
                    NameCompGen newNcg = ncg; 
                    if(joinedWith.size() > 1){
                        //doing deepCopy only if there are multiple tables to be joined with
                        newNcg = ncg.deepCopy();
                        firstComp = newNcg.getQueryPlan().getLastComponent();
                    }
                
                    Component secondComp = newNcg.generateDataSource(compName);            
                    addEquiJoinNotSuboptimal(firstComp, secondComp, newNcg, ncgListSecond);
                }
            }
        
            if(level == numSources - 1){
                //last level, chooseOptimal
                optimal = chooseBest(ncgListSecond);
            }else{
                //filtering - for NCGs with the same ancestor set, choose the one with the smallest totalParallelism
                ncgListSecond = pruneSubplans(ncgListSecond);
            }
            
            ncgListFirst = ncgListSecond;
        }
        
        ParserUtil.parallelismToMap(optimal, _map);
        
        return optimal.getQueryPlan();
    }
    
    private void addEquiJoinNotSuboptimal(Component firstComp, Component secondComp, 
            NameCompGen ncg,
            List<NameCompGen> listNcg) {
        
        boolean isExc = false;
        try{
            ncg.generateEquiJoin(firstComp, secondComp);
        }catch(ImproperParallelismException exc){
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("This subplan will never generated the optimal query plan, so it's thrown:").append("\n");
            errorMsg.append(exc.getMessage()).append("\n");
            LOG.info(errorMsg.toString());
            isExc = true;
        }
        if(!isExc){
            //if this subplan is somewhat suboptimal, it's not added to the list
            listNcg.add(ncg);
        }
    } 

    /*
     * best is the one with the smallest total nodes used
     */
    private NameCompGen chooseBest(List<NameCompGen> ncgList) {
        if(ncgList.isEmpty()){
            String errorMsg = "No query plans can be efficiently executed with specified parallelisms.\n"
                    + "Try to reduce DIP_TOTAL_SRC_PAR in config file.";
            LOG.info(errorMsg);
            System.exit(1);
        }
        
        int index = getMinTotalParIndex(ncgList);
        return ncgList.get(index);
    }

    private List<NameCompGen> pruneSubplans(List<NameCompGen> ncgList) {
        Map<Set<String>, List<NameCompGen>> collection = new HashMap<Set<String>, List<NameCompGen>>();
        
        //filling in the collection with the appropriate key-value structure
        for(NameCompGen ncg: ncgList){
            Set<String> ancestors = ParserUtil.getSourceNameSet(ncg.getQueryPlan().getLastComponent());
            ParserUtil.addToCollection(ancestors, ncg, collection);
        }
        
        List<NameCompGen> pruned = new ArrayList<NameCompGen>();
        //for each key(which is set of ancestors) choose only the best one
        for(Map.Entry<Set<String>, List<NameCompGen>> entrySet:collection.entrySet()){
            List<NameCompGen> valueList = entrySet.getValue();
            
            //all the equivalent plans having minimum totalParallelism are added
            //  there might be muptiple of them
            int minTotalPar = getMinTotalPar(valueList);
            for(NameCompGen ncg: valueList){
                int totalPar = ParserUtil.getTotalParallelism(ncg);
                if(totalPar == minTotalPar){
                    pruned.add(ncg);
                }
            }
        }
        return pruned;
    }

    private int getMinTotalParIndex(List<NameCompGen> ncgList) {
        int totalPar = ParserUtil.getTotalParallelism(ncgList.get(0));
        int minParIndex = 0;
        for(int i = 1; i< ncgList.size(); i++){
            int currentTotalPar = ParserUtil.getTotalParallelism(ncgList.get(i));
            if(currentTotalPar < totalPar){
                minParIndex = i;
                totalPar = currentTotalPar;
            }
        }
        return minParIndex;
    }
    
    private int getMinTotalPar(List<NameCompGen> ncgList){
        int minParIndex = getMinTotalParIndex(ncgList);
        return ParserUtil.getTotalParallelism(ncgList.get(minParIndex));
    }
    
}
