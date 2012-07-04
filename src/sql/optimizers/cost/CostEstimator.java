
package sql.optimizers.cost;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import sql.estimators.SelingerSelectivityEstimator;
import sql.schema.Schema;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

/*
 * Responsible for computing selectivities, cardinalities and parallelism for NCG._compCost
 * Instantiates CostParallelismAssigner
 */
public class CostEstimator {
    private SQLVisitor _pq;
    private Schema _schema;
    
    //needed because NameCompGen sets parallelism for all the components
    private final CostParallelismAssigner _parAssigner;
    
    private Map<String, CostParams> _compCost;
    private SelingerSelectivityEstimator _selEstimator;    
    
    public CostEstimator(Schema schema,
            SQLVisitor pq, 
            Map<String, CostParams> compCost, 
            CostParallelismAssigner parAssigner){
        _pq = pq;
        _schema = schema;
        _compCost = compCost;
        
        _parAssigner = parAssigner;
        _selEstimator = new SelingerSelectivityEstimator(schema, _pq.getTan());
    }
    
    //***********OPERATORS***********
    public void processWhereCost(Component component, Expression whereCompExpr) {
        if(whereCompExpr != null){
            //this is going to change selectivity
            String compName = component.getName();
            CostParams costParams = _compCost.get(compName);
            double previousSelectivity = costParams.getSelectivity();

            double selectivity = previousSelectivity * _selEstimator.estimate(whereCompExpr);
            costParams.setSelectivity(selectivity);
        }
    }
 
    //***********COMMON***********
    //for now only cardinalities
    private void setOutputParams(Component comp){
        CostParams costParams = _compCost.get(comp.getName());
        long currentCardinality = costParams.getCardinality();
        double selectivity = costParams.getSelectivity();
        long cardinality = (long) (selectivity * currentCardinality);
        costParams.setCardinality(cardinality);
    }
    
    //***********SOURCES***********
    //for now only cardinalities
    public void setInputParams(DataSourceComponent source){
        String compName = source.getName();
        String schemaName = _pq.getTan().getSchemaName(compName);
        
        CostParams costParams = _compCost.get(compName);
        costParams.setCardinality(_schema.getTableSize(schemaName));
    }
    
    public void setOutputParamsAndPar(DataSourceComponent source){
        setOutputParams(source);
        
        _parAssigner.setParallelism(source, _compCost);
    }

    
    //***********EquiJoinComponent***********    
    public void setInputParams(EquiJoinComponent joinComponent, List<Expression> joinCondition){
        CostParams costParams = _compCost.get(joinComponent.getName());
        Component[] parents = joinComponent.getParents();
        
        //********* set initial (join) selectivity and initial cardinality
        long leftCardinality = _compCost.get(parents[0].getName()).getCardinality();
        long rightCardinality = _compCost.get(parents[1].getName()).getCardinality();

        //compute
        long inputCardinality = leftCardinality + rightCardinality;
        double selectivity = computeJoinSelectivity(joinComponent, joinCondition, leftCardinality, rightCardinality);

        //setting
        costParams.setCardinality(inputCardinality);
        costParams.setSelectivity(selectivity);
        //*********
    }
    
    public void setOutputParamsAndPar(EquiJoinComponent joinComponent){
        setOutputParams(joinComponent);
        
        _parAssigner.setParallelism(joinComponent, _compCost);
    }
    
    //***********HELPER methods***********    
    private double computeJoinSelectivity(EquiJoinComponent joinComponent, List<Expression> joinCondition, 
            long leftCardinality, long rightCardinality){

        Component[] parents = joinComponent.getParents();
        double selectivity = 1;

        List<Column> joinColumns = ParserUtil.getJSQLColumns(joinCondition);
        List<String> joinCompNames = ParserUtil.getCompNamesFromColumns(joinColumns);

        List<String> leftJoinTableSchemaNames = getJoinSchemaNames(joinCompNames, parents[0]);
        List<String> rightJoinTableSchemaNames = getJoinSchemaNames(joinCompNames, parents[1]);

        if(rightJoinTableSchemaNames.size() > 1){
            throw new RuntimeException("Currently, this support only lefty plans!");
        }
        String rightJoinTableSchemaName = rightJoinTableSchemaNames.get(0);

        int i=0;
        for(String leftJoinTableSchemaName: leftJoinTableSchemaNames){
            double hashSelectivity = computeHashSelectivity(leftJoinTableSchemaName, rightJoinTableSchemaName, leftCardinality, rightCardinality);
            if(i > 0 && hashSelectivity > 1){
                //having multiple hashSelectivities means that we have AndCondition between them,
                //  so they cannot amplify each other.
                hashSelectivity = 1;
            }
            selectivity *= hashSelectivity;
            i++;
        }

        return selectivity;
    }

    /*
     * @allJoinCompNames - all the component names from the join condition
     * joinCompNames - all the component names from the join condition corresponding to parent
     */
    private List<String> getJoinSchemaNames(List<String> allJoinCompNames, Component parent) {
        List<String> ancestors = ParserUtil.getSourceNameList(parent);
        List<String> joinCompNames = ParserUtil.getIntersection(ancestors, allJoinCompNames);

        List<String> joinSchemaNames = new ArrayList<String>();
        for(String joinCompName: joinCompNames){
            joinSchemaNames.add(_pq.getTan().getSchemaName(joinCompName));
        }
        return joinSchemaNames;
    }

    private double computeHashSelectivity(String leftJoinTableSchemaName, String rightJoinTableSchemaName, 
            long leftCardinality, long rightCardinality){
        long inputCardinality = leftCardinality + rightCardinality;
        double selectivity;

        if(leftJoinTableSchemaName.equals(rightJoinTableSchemaName)){
            //we treat this as a cross-product on which some selections are performed
            //IMPORTANT: selectivity is the output/input rate in the case of EquiJoin
            selectivity = (leftCardinality * rightCardinality) / inputCardinality;
        }else{
            double ratio = _schema.getRatio(leftJoinTableSchemaName, rightJoinTableSchemaName);
            if(ratio < 1){
                //if we are joining bigger and smaller relation, the size of join does not decrease
                //if has to be 1
                ratio = 1;
            }
            //in case of bushy plans it's proportion of sizes
            //for lefty plans it's enough to be selectivity of the right parent component (from compCost)
            double rightSelectivity = ((double) rightCardinality) / _schema.getTableSize(rightJoinTableSchemaName);
            selectivity = (leftCardinality * ratio * rightSelectivity) / inputCardinality;
        }
        return selectivity;
    }
    
}