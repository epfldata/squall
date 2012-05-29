/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.ruleBased;

import components.Component;
import components.OperatorComponent;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import queryPlans.QueryPlan;
import schema.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.AggregateOperator;
import operators.ProjectOperator;
import operators.SelectOperator;
import optimizers.ComponentGenerator;
import optimizers.Optimizer;
import optimizers.OptimizerTranslator;
import predicates.Predicate;
import util.HierarchyExtractor;
import util.JoinTablesExp;
import util.ParserUtil;
import util.TableAliasName;
import utilities.DeepCopy;
import visitors.jsql.AndVisitor;
import visitors.jsql.ColumnCollectVisitor;
import visitors.jsql.JoinTablesExpVisitor;
import visitors.squall.SelectItemsVisitor;
import visitors.squall.WhereVisitor;

/*
 * Does not take relation cardinalities into account.
 * Assume no projections before the aggregation, so that EarlyProjection may impose some projections.
 * Aggregation only on the last level.
 */
public class RuleBasedOpt implements Optimizer {
    private Schema _schema;
    private String _dataPath;
    private String _extension;
    private TableAliasName _tan;
    private ComponentGenerator _cg;
    private OptimizerTranslator _ot;
    private Map _map; //map is updates in place

    public RuleBasedOpt(Schema schema, TableAliasName tan, String dataPath, String extension, OptimizerTranslator ot, Map map){
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

        System.out.println("Before WHERE, SELECT and EarlyProjection: ");
        ParserUtil.printQueryPlan(_cg.getQueryPlan());

        //selectItems might add OperatorComponent, this is why it goes first
        int queryType = processSelectClause(selectItems);
        processWhereClause(whereExpr);
        if(queryType == SelectItemsVisitor.NON_AGG){
            System.out.println("Early projection will not be performed since the query is NON_AGG type (contains projections)!");
        }else{
            earlyProjection(_cg.getQueryPlan());
        }
        
        ParserUtil.orderOperators(_cg.getQueryPlan());

        ParallelismAssigner parAssign = new ParallelismAssigner(_cg.getQueryPlan(), _tan, _schema, _map);
        parAssign.assignPar();

        return _cg;
    }

    private ComponentGenerator generateTableJoins(List<Table> tableList, List<Join> joinList) {
        ComponentGenerator cg = new ComponentGenerator(_schema, _tan, _ot, _dataPath, _extension);
        TableSelector ts = new TableSelector(tableList, _schema, _tan);

        //From a list of joins, create collection of elements like {R->{S, R.A=S.A}}
        JoinTablesExpVisitor jteVisitor = new JoinTablesExpVisitor(_tan);
        for(Join join: joinList){
            join.getOnExpression().accept(jteVisitor);
        }
        JoinTablesExp jte = jteVisitor.getJoinTablesExp();

        //first phase
        //make high level pairs
        List<Table> skippedBestTables = new ArrayList<Table>();
        int numTables = tableList.size();
        if(numTables == 1){
            cg.generateDataSource(tableList.get(0));
            return cg;
        }else{
            int highLevelPairs = getNumHighLevelPairs(numTables);

            for(int i=0; i<highLevelPairs; i++){
                Table bestTable = ts.removeBestTable();

                //enumerates all the tables it has joinCondition to join with
                List<String> joinedWith = jte.getJoinedWith(bestTable);
                //dependent on previously used tables, so might return null
                Table bestPairedTable = ts.removeBestPairedTable(joinedWith);
                if(bestPairedTable != null){
                    //we found a pair
                    List<Expression> listExp = jte.getExpressions(bestTable, bestPairedTable);
                    cg.generateSubplan(bestTable, bestPairedTable, listExp);
                }else{
                    //we have to keep this table for latter processing
                    skippedBestTables.add(bestTable);
                }
            }
        }

        //second phase
        //join (2-way join components) with unused tables, until there is no more tables
        List<Component> subPlans = cg.getSubPlans();
        LinkedHashMap<Component, List<String>> subPlanAncestors = new LinkedHashMap<Component, List<String>>();
        for(Component comp: subPlans){
            subPlanAncestors.put(comp, HierarchyExtractor.getAncestorNames(comp));
        }

        /*
         * Why outer loop is unpairedTables, and inner is subPlans:
         * 1) We first take care of small tables
         * 2) In general, there is smaller number of unpaired tables than tables
         * 3) Number of ancestors always grow, while number of joinedTables is a constant
         * Bad side is updating of subPlanAncestors, but than has to be done anyway
         * LinkedHashMap guarantees in order iterator
         */
        List<Table> unpairedTables = ts.removeAll();
        unpairedTables.addAll(skippedBestTables);
        while(!unpairedTables.isEmpty()){
            List<Table> stillUnprocessed = new ArrayList<Table>();
            //we will try to join all the tables, but some of them cannot be joined before some other tables
            //that's why we have while outer loop
            for(Table unpaired: unpairedTables){
                boolean processed = false;
                List<String> joinedWith = jte.getJoinedWith(unpaired);
                for(Map.Entry<Component, List<String>> entry: subPlanAncestors.entrySet()){
                    Component currentComp = entry.getKey();
                    List<String> ancestors = entry.getValue();
                    List<String> intersection = ParserUtil.getIntersection(joinedWith, ancestors);
                    if(!intersection.isEmpty()){
                        String unpairedName = ParserUtil.getComponentName(unpaired);
                        List<Expression> listExp = jte.getExpressions(unpairedName, intersection);
                        Component newComp = cg.generateSubplan(currentComp, unpaired, listExp);

                        //update subPlanAncestor
                        subPlanAncestors.remove(currentComp);
                        //an alternative is concatenation of ancestors + unpairedName
                        subPlanAncestors.put(newComp, HierarchyExtractor.getAncestorNames(newComp));

                        processed = true;
                        break;
                    }
                }
                if(!processed){
                    stillUnprocessed.add(unpaired);
                }
            }
            unpairedTables = stillUnprocessed;
        }

        //third phase: joining Components until there is a single component
        subPlans = cg.getSubPlans();
        while(subPlans.size() > 1){
                //this is joining of components having approximately the same number of ancestors - the same level
                Component firstComp = subPlans.get(0);
                List<String> firstAncestors = HierarchyExtractor.getAncestorNames(firstComp);
                List<String> firstJoinedWith = jte.getJoinedWith(firstAncestors);
                for(int i=1; i<subPlans.size(); i++){
                    Component otherComp = subPlans.get(i);
                    List<String> otherAncestors = HierarchyExtractor.getAncestorNames(otherComp);
                    List<String> intersection = ParserUtil.getIntersection(firstJoinedWith, otherAncestors);
                    if(!intersection.isEmpty()){
                        List<Expression> listExp = jte.getExpressions(firstAncestors, intersection);
                        Component newComp = cg.generateSubplan(firstComp, otherComp, listExp);
                        break;
                    }
                }
            //until this point, we change subPlans by locally remove operations
            //when going to the next level, whesh look over subPlans is taken
            subPlans = cg.getSubPlans();
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
            ProjectOperator project = new ProjectOperator(selectVEs);
            affectedComponent.addOperator(project);
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
                                                                      _cg.getQueryPlan()).addOperator(firstAgg);

                }else{
                    affectedComponent.addOperator(firstAgg);
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
                ProjectOperator groupByProj = new ProjectOperator((List<ValueExpression>)DeepCopy.copy(selectVEs));
                firstAgg.setGroupByProjection(groupByProj);
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
        if(whereExpr==null){
            return;
        }
        AndVisitor andVisitor = new AndVisitor();
        whereExpr.accept(andVisitor);
        List<Expression> atomicExprs = andVisitor.getAtomicExprs();
        List<OrExpression> orExprs = andVisitor.getOrExprs();

        //atomic expression are those who have AND between them. Still, we need to put same columns together
        HashMap<String, Expression> collocatedExprs = new HashMap<String, Expression>();
        for(Expression atomicExpr: atomicExprs){
            List<Column> columns = invokeColumnVisitor(atomicExpr);
            if(columns.size()!=1){
                throw new RuntimeException("Number of expected columns for atomic expression is exactly one!");
            }
            Column column = columns.get(0);
            String columnName = column.getWholeColumnName();
            if(collocatedExprs.containsKey(columnName)){
                Expression oldExpr = collocatedExprs.get(columnName);
                Expression newExpr = new AndExpression(oldExpr, atomicExpr);
                collocatedExprs.put(columnName, newExpr);
            }else{
                collocatedExprs.put(columnName, atomicExpr);
            }
        }

        for(Map.Entry<String, Expression> entry: collocatedExprs.entrySet()){
            String columnName = entry.getKey();
            Expression exprPerColumn = entry.getValue();
            Component affectedComponent = HierarchyExtractor.getDSCwithColumn(columnName, _cg);
            processWhereForComponent(exprPerColumn, affectedComponent);
        }

        for(OrExpression orExpr: orExprs){
            List<Column> columns = invokeColumnVisitor(orExpr);
            List<Component> compList = HierarchyExtractor.getDSCwithColumn(columns, _cg);
            Component affectedComponent = HierarchyExtractor.getLCM(compList);
            processWhereForComponent(orExpr, affectedComponent);
        }
    }

    private void processWhereForComponent(Expression whereExpression, Component affectedComponent){
        WhereVisitor whereVisitor = new WhereVisitor(_cg.getQueryPlan(), affectedComponent, _schema, _tan, _ot);
        whereExpression.accept(whereVisitor);
        attachWhereClause(whereVisitor.getPredicate(), affectedComponent);
    }

    private void attachWhereClause(Predicate predicate, Component affectedComponent) {
        affectedComponent.addOperator(new SelectOperator(predicate));
    }

    //HELPER
    private List<Column> invokeColumnVisitor(Expression expr) {
        ColumnCollectVisitor columnCollect = new ColumnCollectVisitor();
        expr.accept(columnCollect);
        return columnCollect.getColumns();
    }

    private int getNumHighLevelPairs(int numTables) {
        int highLevelPairs=0;
        if(numTables == 2){
            highLevelPairs = 1;
        }else if(numTables > 2){
            highLevelPairs = (numTables % 2==0 ? numTables/2 - 1 : numTables/2);
        }
        return highLevelPairs;
    }

    private void earlyProjection(QueryPlan queryPlan) {
        EarlyProjection early = new EarlyProjection();
        early.operate(queryPlan);
    }


}