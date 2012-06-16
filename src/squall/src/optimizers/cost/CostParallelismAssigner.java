package optimizers.cost;

import components.DataSourceComponent;
import components.EquiJoinComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import schema.Schema;
import util.JoinTablesExprs;
import util.ParserUtil;
import util.TableAliasName;
import utilities.SystemParameters;


public class CostParallelismAssigner {
    private final Schema _schema;
    private final TableAliasName _tan;
    private final NameTranslator _nt = new NameTranslator();
    private final String _dataPath;
    private final String _extension;
    private final Map _map;
    private final Map<String, Expression> _compNamesAndExprs;
    private final Map<Set<String>, Expression> _compNamesOrExprs;

    private final ProjGlobalCollect _globalCollect;
    private final JoinTablesExprs _jte;

    public CostParallelismAssigner(Schema schema,
            TableAliasName tan,
            String dataPath,
            String extension,
            Map map,
            Map<String, Expression> compNamesAndExprs,
            Map<Set<String>, Expression> compNamesOrExprs,
            ProjGlobalCollect globalCollect,
            JoinTablesExprs jte) {

        _schema = schema;
        _tan = tan;
        _dataPath = dataPath;
        _extension = extension;
        _map = map;
        _compNamesAndExprs = compNamesAndExprs;
        _compNamesOrExprs = compNamesOrExprs;

        _globalCollect = globalCollect;
        _jte = jte;
    }

    /*
     * This is done on fake ComponentGenerator, so there is no setSourceParallelism
     *   parallelism on source components is input variable
     * This method is idempotent, no side effects, can be called multiple times.
     */
    public Map<String, Integer> getSourceParallelism(List<Table> tableList, int totalSourcePar){
        /*
         * We need a way to generate parallelism for all the DataSources
         *   This depends on its selectivity/cardinality of each dataSource
         *   So we will generate all the sources witihin a fake sourceCG,
         *   and then proportionally assign parallelism.
         */
        NameComponentGenerator sourceCG = new NameComponentGenerator(_schema, _tan,
                 _dataPath, _extension, _map, this, _compNamesAndExprs, _compNamesOrExprs, _globalCollect, _jte);

         List<OrderedCostParams> sourceCostParams = new ArrayList<OrderedCostParams>();
         long totalCardinality = 0;
         for(Table table: tableList){
             DataSourceComponent source = sourceCG.generateDataSource(ParserUtil.getComponentName(table));
             String compName = source.getName();
             long cardinality = sourceCG.getCostParameters(compName).getCardinality();
             totalCardinality += cardinality;
             sourceCostParams.add(new OrderedCostParams(compName, cardinality));
         }

         /* Sort by cardinalities, so that the smallest tables does not end up with parallelism = 0
          * We divide by its output cardinality, because network traffic is the dominant cost
          */
         int availableNodes = totalSourcePar;
         Collections.sort(sourceCostParams);
         for(OrderedCostParams cnc: sourceCostParams){
             long cardinality = cnc.getCardinality();
             double ratioNodes = (double)cardinality/totalCardinality;
             double dblNumNodes = ratioNodes * totalSourcePar;
             int numNodes = (int) (dblNumNodes + 0.5); // rounding effect out of the default flooring

             if(availableNodes == 0){
                 throw new RuntimeException("There is not enought nodes such that all the sources get at least parallelism = 1");
             }
             if(numNodes == 0){
                 //lower bounds
                 numNodes = 1;
             }
             if(numNodes > availableNodes){
                 //upper bounds, we already checked if _totalSourcePar is not 0
                 numNodes = availableNodes;
             }

             cnc.setParallelism(numNodes);
             availableNodes -= numNodes;
         }

         /*
          * Now convert it to a Map, so that parallelism for source can be easier obtained
          */
         Map<String, Integer> compParallelism = convertListToMap(sourceCostParams);

         return compParallelism;
    }

    private Map<String, Integer> convertListToMap(List<OrderedCostParams> sourceCostParams) {
         Map<String, Integer> compParallelism = new HashMap<String, Integer>();
         for(OrderedCostParams cnc: sourceCostParams){
             String compName = cnc.getComponentName();
             int parallelism = cnc.getParallelism();
             compParallelism.put(compName, parallelism);
         }
         return compParallelism;
    }

    /*
     * cost-function
     * also idempotent, no changes to `this`
     *   changes only compCost
     */
    public void setParallelism(EquiJoinComponent joinComponent, Map<String, CostParams> compCost) {
        String leftParent = joinComponent.getParents()[0].getName();
        String rightParent = joinComponent.getParents()[1].getName();

        CostParams leftParentParams = compCost.get(leftParent);
        CostParams rightParentParams = compCost.get(rightParent);

        int leftParallelism = leftParentParams.getParallelism();
        int rightParallelism = rightParentParams.getParallelism();

        //compute
        int parallelism = parallelismFormula(leftParentParams, rightParentParams);

        //upper bound
        int maxParallelism = estimateDistinctHashes(leftParentParams, rightParentParams);
        if(parallelism > maxParallelism){
            if(leftParallelism == 1 && rightParallelism == 1){
                //if parallelism of both parents is 1, then we should not raise an exception
                //  exception serves to force smaller parallelism at sources
                parallelism = maxParallelism;
            }else{
                throw new RuntimeException("Component " + joinComponent.getName() + " cannot have parallelism more than " + maxParallelism);
            }
        }

        //setting
        String currentComp = joinComponent.getName();
        compCost.get(currentComp).setParallelism(parallelism);
    }

    private int parallelismFormula(CostParams leftParentParams, CostParams rightParentParams) {
        //computing TODO: does not take into account when joinComponent send tuples further down
        double dblParallelism = leftParentParams.getSelectivity() * leftParentParams.getParallelism() +
                            rightParentParams.getSelectivity() * rightParentParams.getParallelism() +
                            1.0/8 * (leftParentParams.getParallelism() + rightParentParams.getParallelism());
        int parallelism = (int) dblParallelism;
        if(parallelism != dblParallelism){
            //parallelism is ceil of dblParallelism
            parallelism++;
        }
        return parallelism;
    }

    /*
     * We take the number of tuples as the upper limit.
     *   The real number of distinct hashes may be much smaller,
     *   for example when having multiple tuples with the very same hash value.
     *   This is rare in practice, in the example we tried,
     *      it occurs only for the final aggregation when join and final aggregation are not on the last node.
     */
    private int estimateDistinctHashes(CostParams leftParentParams, CostParams rightParentParams) {
        /* TODO: to implement this properly, we need to:
         *   - find all the parent column appearing in joinCondition
         *         - column are found by using ParserUtil.getJSQLColumns(joinCondition)
         *         - joinCondition related to the parents is available in CostOptimizer (obtained byJoinTableExpr.getExpression(table1, table2))
         *   - check if this column is the key, or its functional dependency
         *          (for example NATION.NATIONNAME is a functional dependency of NATION.NATIONKEY)
         */
        long leftCardinality = leftParentParams.getCardinality();
        long rightCardinality = rightParentParams.getCardinality();

        long distinctValues = leftCardinality;
        if(distinctValues > rightCardinality){
            //we return the smaller one
            distinctValues = rightCardinality;
        }

        int result = (int) distinctValues;
        if(result != distinctValues){
            //so huge that cannot fit in int, we will set to MAX_PARALLELISM
            result = SystemParameters.getInt(_map, "DIP_NUM_PARALLELISM");
        }
        return result;
    }

    /*
     * we need separate class from CostParams, because here we want to order them based on cardinality
     * This class will contain all the parallelism for DataSourceComponents
     */
    private class OrderedCostParams extends CostParams implements Comparable<OrderedCostParams>{
        private String _componentName;

        public OrderedCostParams(String componentName, long cardinality){
            _componentName = componentName;
            setCardinality(cardinality);
        }

        public String getComponentName() {
            return _componentName;
        }

        @Override
        public int compareTo(OrderedCostParams t) {
            long myCardinality = getCardinality();
            long otherCardinality = t.getCardinality();
            return (new Long(myCardinality)).compareTo(new Long(otherCardinality));
        }
    }
}
