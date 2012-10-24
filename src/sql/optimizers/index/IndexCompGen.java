package sql.optimizers.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.expressions.ValueExpression;
import plan_runner.query_plans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.CompGen;
import sql.schema.Schema;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.IndexJoinHashVisitor;

/*
 * It is necessary that this class operates with Tables,
 *   since we don't want multiple CG sharing the same copy of DataSourceComponent.
 */
public class IndexCompGen implements CompGen{
    private SQLVisitor _pq;

    private Schema _schema;
    private String _dataPath;
    private String _extension;

    private QueryPlan _queryPlan = new QueryPlan();

    //List of Components which are already added throughEquiJoinComponent and OperatorComponent
    private List<Component> _subPlans = new ArrayList<Component>();

    public IndexCompGen(Schema schema, SQLVisitor pq, Map map){
        _schema = schema;
        _pq = pq;
        _dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
        _extension = SystemParameters.getString(map, "DIP_EXTENSION");
    }

    @Override
    public QueryPlan getQueryPlan(){
        return _queryPlan;
    }

    @Override
    public List<Component> getSubPlans(){
        return _subPlans;
    }

    /*
     * adding a DataSourceComponent to the list of components
     */
    @Override
    public DataSourceComponent generateDataSource(String tableCompName){
        String tableSchemaName = _pq.getTan().getSchemaName(tableCompName);
        String sourceFile = tableSchemaName.toLowerCase();

        DataSourceComponent relation = new DataSourceComponent(
                                        tableCompName,
                                        _dataPath + sourceFile + _extension,
                                        _queryPlan);
        _subPlans.add(relation);
        return relation;
    }

    /*
     * Join between two components
     * List<Expression> is a set of join conditions between two components.
     */
    @Override
    public Component generateEquiJoin(Component left, Component right){
        EquiJoinComponent joinComponent = new EquiJoinComponent(
                    left,
                    right,
                    _queryPlan);

        //compute join condition
        List<Expression> joinCondition = ParserUtil.getJoinCondition(_pq, left, right);
        if(joinCondition == null){
            throw new RuntimeException("There is no join conditition between components " 
                    + left.getName() + " and " + right.getName());
        }        
        
        //set hashes for two parents
        addHash(left, joinCondition);
        addHash(right, joinCondition);

        _subPlans.remove(left);
        _subPlans.remove(right);
        _subPlans.add(joinComponent);

        return joinComponent;
    }



    //set hash for this component, knowing its position in the query plan.
    //  Conditions are related only to parents of join,
    //  but we have to filter who belongs to my branch in IndexJoinHashVisitor.
    //  We don't want to hash on something which will be used to join with same later component in the hierarchy.
    private void addHash(Component component, List<Expression> joinCondition) {
            IndexJoinHashVisitor joinOn = new IndexJoinHashVisitor(_schema, component, _pq.getTan());
            for(Expression exp: joinCondition){
                exp.accept(joinOn);
            }
            List<ValueExpression> hashExpressions = joinOn.getExpressions();

            if(ParserUtil.isAllColumnRefs(hashExpressions)){
                //all the join conditions are represented through columns, no ValueExpression (neither in joined component)
                //guaranteed that both joined components will have joined columns visited in the same order
                //i.e R.A=S.A and R.B=S.B, the columns are (R.A, R.B), (S.A, S.B), respectively
                List<Integer> hashIndexes = ParserUtil.extractColumnIndexes(hashExpressions);

                //hash indexes in join condition
                component.setHashIndexes(hashIndexes);
            }else{
                //hahs expressions in join condition
                component.setHashExpressions(hashExpressions);
            }
    }

}