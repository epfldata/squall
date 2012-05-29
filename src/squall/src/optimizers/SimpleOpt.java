/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers;

import optimizers.ruleBased.ParallelismAssigner;
import components.Component;
import components.OperatorComponent;
import expressions.ValueExpression;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.AggregateOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import predicates.Predicate;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;
import utilities.DeepCopy;
import visitors.squall.SelectItemsVisitor;
import visitors.squall.WhereVisitor;


public class SimpleOpt implements Optimizer {
    private Schema _schema;
    private String _dataPath;
    private String _extension;
    private ComponentGenerator _cg;
    private TableAliasName _tan;
    private OptimizerTranslator _ot;
    private Map _map;
    
    public SimpleOpt(Schema schema, TableAliasName tan, String dataPath, String extension, OptimizerTranslator ot, Map map){
        _schema = schema;
        _tan = tan;
        _dataPath = dataPath;
        _extension = extension;
        _ot = ot;
        _map = map;
    }

    @Override
    public ComponentGenerator generate(List<Table> tableList, List<Join> joinList, List<SelectItem> selectItems, Expression whereExpr){
        _cg = generateTableJoins(tableList, joinList);

        //selectItems might add OperatorComponent, this is why it goes first
        processSelectClause(selectItems);
        processWhereClause(whereExpr);

        ParallelismAssigner parAssign = new ParallelismAssigner(_cg.getQueryPlan(), _tan, _schema, _map);
        parAssign.assignPar();

        return _cg;
    }

    private ComponentGenerator generateTableJoins(List<Table> tableList, List<Join> joinList) {
        ComponentGenerator cg = new ComponentGenerator(_schema, _tan, _ot, _dataPath, _extension);

        //special case
        if(tableList.size()==1){
            cg.generateDataSource(tableList.get(0));
            return cg;
        }

        // This generates a lefty query plan.
        Table firstTable = tableList.get(0);
        Table secondTable = tableList.get(1);
        Join firstJoin = joinList.get(0);
        Component comp = cg.generateSubplan(firstTable, secondTable, firstJoin);

        for(int i=1; i<joinList.size(); i++){
            Table currentTable = tableList.get(i+1);
            Join currentJoin = joinList.get(i);
            comp = cg.generateSubplan(comp, currentTable, currentJoin);
        }
        return cg;
    }

    private int processSelectClause(List<SelectItem> selectItems) {
        //TODO: take care in nested case
        SelectItemsVisitor selectVisitor = new SelectItemsVisitor(_cg.getQueryPlan(), _schema, _tan, _ot, _map);
        for(SelectItem elem: selectItems){
            elem.accept(selectVisitor);
        }
        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
        List<ValueExpression> selectVEs = selectVisitor.getSelectVEs();

        Component affectedComponent = _cg.getQueryPlan().getLastComponent();
        attachSelectClause(aggOps, selectVEs, affectedComponent);
        return (aggOps.isEmpty() ? SelectItemsVisitor.NON_AGG : SelectItemsVisitor.AGG);
    }

    private void attachSelectClause(List<AggregateOperator> aggOps, List<ValueExpression> selectVEs, Component affectedComponent) {
        if (aggOps.isEmpty()){
            ProjectionOperator project = new ProjectionOperator(selectVEs);
            affectedComponent.setProjection(project);
        }else if (aggOps.size() == 1){
            //all the others are group by
            AggregateOperator firstAgg = aggOps.get(0);

            if(ParserUtil.isAllColumnRefs(selectVEs)){
                //plain fields in select
                List<Integer> groupByColumns = ParserUtil.extractColumnIndexes(selectVEs);
                firstAgg.setGroupByColumns(groupByColumns);

                //Setting new level of components is necessary for correctness only for distinct in aggregates
                    //  but it's certainly pleasant to have the final result grouped on nodes by group by columns.
                boolean newLevel = !(_ot.isHashedBy(affectedComponent, groupByColumns));
                if(newLevel){
                    affectedComponent.setHashIndexes(groupByColumns);
                    OperatorComponent newComponent = new OperatorComponent(affectedComponent,
                                                                      ParserUtil.generateUniqueName("OPERATOR"),
                                                                      _cg.getQueryPlan()).setAggregation(firstAgg);

                }else{
                    affectedComponent.setAggregation(firstAgg);
                }
            }else{
                //Sometimes selectExpr contains other functions, so we have to use projections instead of simple groupBy
                //Check for complexity
                if (affectedComponent.getHashExpressions()!=null && !affectedComponent.getHashExpressions().isEmpty()){
                    throw new RuntimeException("Too complex: cannot have hashExpression both for joinCondition and groupBy!");
                }

                //WARNING: _selectExpr cannot be used on two places: that's why we do deep copy
               affectedComponent.setHashExpressions((List<ValueExpression>)DeepCopy.copy(selectVEs));

                //always new level
                ProjectionOperator groupByProj = new ProjectionOperator((List<ValueExpression>)DeepCopy.copy(selectVEs));
                firstAgg.setGroupByProjection(groupByProj);
                OperatorComponent newComponent = new OperatorComponent(affectedComponent,
                                                                      ParserUtil.generateUniqueName("OPERATOR"),
                                                                      _cg.getQueryPlan()).setAggregation(firstAgg);

            }
        }else{
            throw new RuntimeException("For now only one aggregate function supported!");
        }
    }

    private void processWhereClause(Expression whereExpr) {
        // TODO: in non-nested case, there is a single Expression
        
        //all the selection are performed on the last component
        Component affectedComponent = _cg.getQueryPlan().getLastComponent();
        WhereVisitor whereVisitor = new WhereVisitor(_cg.getQueryPlan(), affectedComponent, _schema, _tan, _ot);
        if(whereExpr != null){
            whereExpr.accept(whereVisitor);
        }

        attachWhereClause(whereVisitor.getPredicate(), affectedComponent);
    }

    private void attachWhereClause(Predicate predicate, Component affectedComponent) {
        affectedComponent.setSelection(new SelectionOperator(predicate));
    }

}
