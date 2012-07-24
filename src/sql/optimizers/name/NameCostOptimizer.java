
package sql.optimizers.name;

import java.util.ArrayList;
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
public class NameCostOptimizer implements Optimizer{
    private SQLVisitor _pq;
    private Map _map;
    
    public NameCostOptimizer(Map map) {
        _map = map;
        _pq = ParserUtil.parseQuery(map);
    }
    
    public QueryPlan generate() {
        int totalParallelism = SystemParameters.getInt(_map, "DIP_TOTAL_SRC_PAR");
        NameCompGenFactory factory = new NameCompGenFactory(_map, totalParallelism);
        List<String> sourceNames = factory.getParAssigner().getSortedSourceNames();
        int numSources = sourceNames.size();
        NameCompGen optimal = null;
        
        //**************creating single-relation plans********************
        if(numSources == 1){
            optimal = factory.create();
            optimal.generateDataSource(sourceNames.get(0));
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
                joinedWith = ParserUtil.getDifference(joinedWith, ancestors);
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
        
            //filtering
            if(level == numSources - 1){
                //last level, chooseOptimal
                optimal = chooseBest(ncgListSecond);
            }else{
                
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
        }catch(RuntimeException exc){
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("This subplan will never generated the optimal query plan, so it's thrown:").append("\n");
            errorMsg.append(exc.getMessage()).append("\n");
            System.out.println(errorMsg.toString());
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
            System.out.println(errorMsg);
            System.exit(1);
        }
        int parallelism = ParserUtil.getTotalParallelism(ncgList.get(0).getCompCost());
        int index = 0;
        for(int i = 0; i< ncgList.size(); i++){
            NameCompGen ncg = ncgList.get(i);
            if(ParserUtil.getTotalParallelism(ncg.getCompCost()) < parallelism){
                index = i;
            }
        }
        return ncgList.get(index);
    }

}