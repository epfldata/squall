package sql.optimizers.index;

import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.queryPlans.QueryPlan;
import plan_runner.utilities.DeepCopy;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.schema.Schema;
import sql.schema.TPCH_Schema;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.IndexSelectItemsVisitor;
import sql.visitors.squall.IndexWhereVisitor;

/*
 * Generate a query plan as it was parsed from the SQL.
 * SELECT and WHERE clause are attached to the final component.
 */
public class IndexSimpleOptimizer implements Optimizer {
    private SQLVisitor _pq;
    private Schema _schema;
    private Map _map;
    
    private IndexCompGen _cg;
    private IndexTranslator _it;
    
    public IndexSimpleOptimizer(Map map){
        _map = map;
        _pq = ParserUtil.parseQuery(map);        
        
        double scallingFactor = SystemParameters.getDouble(map, "DIP_DB_SIZE");
        _schema = new TPCH_Schema(scallingFactor);
        _it = new IndexTranslator(_schema, _pq.getTan());
    }

    @Override
    public QueryPlan generate(){
        _cg = generateTableJoins();

        //selectItems might add OperatorComponent, this is why it goes first
        processSelectClause(_pq.getSelectItems());
        processWhereClause(_pq.getWhereExpr());

        ParserUtil.orderOperators(_cg.getQueryPlan());

        RuleParallelismAssigner parAssign = new RuleParallelismAssigner(_cg.getQueryPlan(), _pq.getTan(), _schema, _map);
        parAssign.assignPar();

        return _cg.getQueryPlan();
    }

    private IndexCompGen generateTableJoins() {
        List<Table> tableList = _pq.getTableList();
        
        IndexCompGen cg = new IndexCompGen(_schema, _pq, _map);
        Component firstParent = cg.generateDataSource(ParserUtil.getComponentName(tableList.get(0)));

        //a special case
        if(tableList.size()==1){    
            return cg;
        }

        // This generates a lefty query plan.
        for(int i=0; i<tableList.size() - 1; i++){
            DataSourceComponent secondParent = cg.generateDataSource(ParserUtil.getComponentName(tableList.get(i+1)));
            firstParent = cg.generateEquiJoin(firstParent, secondParent);
        }
        return cg;
    }

    private int processSelectClause(List<SelectItem> selectItems) {
        //TODO: take care in nested case
        IndexSelectItemsVisitor selectVisitor = new IndexSelectItemsVisitor(_cg.getQueryPlan(), _schema, _pq.getTan(), _map);
        for(SelectItem elem: selectItems){
            elem.accept(selectVisitor);
        }
        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
        List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();

        Component affectedComponent = _cg.getQueryPlan().getLastComponent();
        attachSelectClause(aggOps, groupByVEs, affectedComponent);
        return (aggOps.isEmpty() ? IndexSelectItemsVisitor.NON_AGG : IndexSelectItemsVisitor.AGG);
    }

    private void attachSelectClause(List<AggregateOperator> aggOps, List<ValueExpression> groupByVEs, Component affectedComponent) {
        if (aggOps.isEmpty()){
            ProjectOperator project = new ProjectOperator(groupByVEs);
            affectedComponent.addOperator(project);
        }else if (aggOps.size() == 1){
            //all the others are group by
            AggregateOperator firstAgg = aggOps.get(0);

            if(ParserUtil.isAllColumnRefs(groupByVEs)){
                //plain fields in select
                List<Integer> groupByColumns = ParserUtil.extractColumnIndexes(groupByVEs);
                firstAgg.setGroupByColumns(groupByColumns);

                //Setting new level of components is necessary for correctness only for distinct in aggregates
                    //  but it's certainly pleasant to have the final result grouped on nodes by group by columns.
                boolean newLevel = !(_it.isHashedBy(affectedComponent, groupByColumns));
                if(newLevel){
                    affectedComponent.setHashIndexes(groupByColumns);
                    OperatorComponent newComponent = new OperatorComponent(affectedComponent,
                                                                      ParserUtil.generateUniqueName("OPERATOR"),
                                                                      _cg.getQueryPlan()).addOperator(firstAgg);

                }else{
                    affectedComponent.addOperator(firstAgg);
                }
             }else{
                //Sometimes groupByVEs contains other functions, so we have to use projections instead of simple groupBy
                //always new level

                if (affectedComponent.getHashExpressions()!=null && !affectedComponent.getHashExpressions().isEmpty()){
                    //TODO: probably will be solved in cost-based optimizer
                    throw new RuntimeException("Too complex: cannot have hashExpression both for joinCondition and groupBy!");
                }

                //WARNING: groupByVEs cannot be used on two places: that's why we do deep copy
                ProjectOperator groupByProj = new ProjectOperator((List<ValueExpression>)DeepCopy.copy(groupByVEs));
                firstAgg.setGroupByProjection(groupByProj);

                //current component
                affectedComponent.setHashExpressions((List<ValueExpression>)DeepCopy.copy(groupByVEs));

                //next component
                OperatorComponent newComponent = new OperatorComponent(affectedComponent,
                                                                      ParserUtil.generateUniqueName("OPERATOR"),
                                                                      _cg.getQueryPlan()).addOperator(firstAgg);

            }
        }else{
            throw new RuntimeException("For now only one aggregate function supported!");
        }
    }

    private void processWhereClause(Expression whereExpr) {
        // TODO: in non-nested case, there is a single Expression
        
        //all the selection are performed on the last component
        Component affectedComponent = _cg.getQueryPlan().getLastComponent();
        IndexWhereVisitor whereVisitor = new IndexWhereVisitor(affectedComponent, _schema, _pq.getTan());
        if(whereExpr != null){
            whereExpr.accept(whereVisitor);
            attachWhereClause(whereVisitor.getSelectOperator(), affectedComponent);
        }
    }

    private void attachWhereClause(SelectOperator select, Component affectedComponent) {
        affectedComponent.addOperator(select);
    }

}
