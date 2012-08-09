package sql.optimizers.index;

import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
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
import sql.util.HierarchyExtractor;
import sql.util.JoinTablesExprs;
import sql.util.ParserUtil;
import sql.visitors.jsql.AndVisitor;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.IndexSelectItemsVisitor;
import sql.visitors.squall.IndexWhereVisitor;

/*
 * It generates a single query plan, adds a final aggregation,
 *   adds selections (WHERE clause) and do early projections (all unused columns are projected away)
 *
 * Does not take relation cardinalities into account.
 * Assume no projections before the aggregation, so that EarlyProjection may impose some projections.
 * Aggregation only on the last level.
 */
public class IndexRuleOptimizer implements Optimizer {
    private Schema _schema;
    private SQLVisitor _pq;
    private IndexCompGen _cg;
    private IndexTranslator _it;
    private Map _map; //map is updates in place

    public IndexRuleOptimizer(Map map){
        _map = map;
        _pq = ParserUtil.parseQuery(map);
        
        _schema = new Schema(map);
        _it = new IndexTranslator(_schema, _pq.getTan());
    }

    @Override
    public QueryPlan generate(){
        _cg = generateTableJoins();

        System.out.println("Before WHERE, SELECT and EarlyProjection: ");
        System.out.println(ParserUtil.toString(_cg.getQueryPlan()));

        //selectItems might add OperatorComponent, this is why it goes first
        int queryType = processSelectClause(_pq.getSelectItems());
        processWhereClause(_pq.getWhereExpr());
        if(queryType == IndexSelectItemsVisitor.NON_AGG){
            System.out.println("Early projection will not be performed since the query is NON_AGG type (contains projections)!");
        }else{
            earlyProjection(_cg.getQueryPlan());
        }
        
        ParserUtil.orderOperators(_cg.getQueryPlan());

        RuleParallelismAssigner parAssign = new RuleParallelismAssigner(_cg.getQueryPlan(), _pq.getTan(), _schema, _map);
        parAssign.assignPar();

        return _cg.getQueryPlan();
    }

    private IndexCompGen generateTableJoins() {
        List<Table> tableList = _pq.getTableList();
        TableSelector ts = new TableSelector(tableList, _schema, _pq.getTan());
        JoinTablesExprs jte = _pq.getJte();
        
        IndexCompGen cg = new IndexCompGen(_schema, _pq, _map);
        
        //first phase
        //make high level pairs
        List<String> skippedBestTableNames = new ArrayList<String>();
        int numTables = tableList.size();
        if(numTables == 1){
            cg.generateDataSource(ParserUtil.getComponentName(tableList.get(0)));
            return cg;
        }else{
            int highLevelPairs = getNumHighLevelPairs(numTables);

            for(int i=0; i<highLevelPairs; i++){
                String bestTableName = ts.removeBestTableName();

                //enumerates all the tables it has joinCondition to join with
                List<String> joinedWith = jte.getJoinedWith(bestTableName);
                //dependent on previously used tables, so might return null
                String bestPairedTable = ts.removeBestPairedTableName(joinedWith);
                if(bestPairedTable != null){
                    //we found a pair
                    DataSourceComponent bestSource = cg.generateDataSource(bestTableName);
                    DataSourceComponent bestPairedSource = cg.generateDataSource(bestPairedTable);
                    cg.generateEquiJoin(bestSource, bestPairedSource);
                }else{
                    //we have to keep this table for latter processing
                    skippedBestTableNames.add(bestTableName);
                }
            }
        }

        //second phase
        //join (2-way join components) with unused tables, until there is no more tables
        List<Component> subPlans = cg.getSubPlans();

        /*
         * Why outer loop is unpairedTables, and inner is subPlans:
         * 1) We first take care of small tables
         * 2) In general, there is smaller number of unpaired tables than tables
         * 3) Number of ancestors always grow, while number of joinedTables is a constant
         * Bad side is updating of subPlanAncestors, but than has to be done anyway
         * LinkedHashMap guarantees in order iterator
         */
        List<String> unpairedTableNames = ts.removeAll();
        unpairedTableNames.addAll(skippedBestTableNames);
        while(!unpairedTableNames.isEmpty()){
            List<String> stillUnprocessed = new ArrayList<String>();
            //we will try to join all the tables, but some of them cannot be joined before some other tables
            //that's why we have while outer loop
            for(String unpaired: unpairedTableNames){
                boolean processed = false;
                for(Component currentComp: subPlans){
                    if(_pq.getJte().joinExistsBetween(unpaired, ParserUtil.getSourceNameList(currentComp))){
                        DataSourceComponent unpairedSource = cg.generateDataSource(unpaired);
                        Component newComp = cg.generateEquiJoin(currentComp, unpairedSource);

                        processed = true;
                        break;
                    }
                }
                if(!processed){
                    stillUnprocessed.add(unpaired);
                }
            }
            unpairedTableNames = stillUnprocessed;
        }

        //third phase: joining Components until there is a single component
        subPlans = cg.getSubPlans();
        while(subPlans.size() > 1){
                //this is joining of components having approximately the same number of ancestors - the same level
                Component firstComp = subPlans.get(0);
                List<String> firstAncestors = ParserUtil.getSourceNameList(firstComp);
                for(int i=1; i<subPlans.size(); i++){
                    Component otherComp = subPlans.get(i);
                    List<String> otherAncestors = ParserUtil.getSourceNameList(otherComp);
                    if(_pq.getJte().joinExistsBetween(firstAncestors, otherAncestors)){
                        Component newComp = cg.generateEquiJoin(firstComp, otherComp);
                        break;
                    }
                }
            //until this point, we change subPlans by locally remove operations
            //when going to the next level, whesh look over subPlans is taken
            subPlans = cg.getSubPlans();
        }
        return cg;
    }

    /*************************************************************************************
     * SELECT clause - Final Aggregation
     *************************************************************************************/

    private int processSelectClause(List<SelectItem> selectItems) {
        //TODO: take care in nested case
        IndexSelectItemsVisitor selectVisitor = new IndexSelectItemsVisitor(_cg.getQueryPlan(), _schema, _pq.getTan(), _map);
        for(SelectItem elem: selectItems){
            elem.accept(selectVisitor);
        }
        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
        List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();

        Component affectedComponent = _cg.getQueryPlan().getLastComponent();
        attachSelectClause(affectedComponent, aggOps, groupByVEs);
        return (aggOps.isEmpty() ? IndexSelectItemsVisitor.NON_AGG : IndexSelectItemsVisitor.AGG);
    }

    private void attachSelectClause(Component lastComponent, List<AggregateOperator> aggOps, List<ValueExpression> groupByVEs) {
        if (aggOps.isEmpty()){
            ProjectOperator project = new ProjectOperator(groupByVEs);
            lastComponent.addOperator(project);
        }else if (aggOps.size() == 1){
            //all the others are group by
            AggregateOperator firstAgg = aggOps.get(0);

            if(ParserUtil.isAllColumnRefs(groupByVEs)){
                //plain fields in select
                List<Integer> groupByColumns = ParserUtil.extractColumnIndexes(groupByVEs);
                firstAgg.setGroupByColumns(groupByColumns);

                //Setting new level of components is necessary for correctness only for distinct in aggregates
                    //  but it's certainly pleasant to have the final result grouped on nodes by group by columns.
                boolean newLevel = !(_it.isHashedBy(lastComponent, groupByColumns));
                if(newLevel){
                    lastComponent.setHashIndexes(groupByColumns);
                    OperatorComponent newComponent = new OperatorComponent(lastComponent,
                                                                      ParserUtil.generateUniqueName("OPERATOR"),
                                                                      _cg.getQueryPlan()).addOperator(firstAgg);

                }else{
                    lastComponent.addOperator(firstAgg);
                }
            }else{
                //Sometimes groupByVEs contains other functions, so we have to use projections instead of simple groupBy
                //always new level

                //WARNING: groupByVEs cannot be used on two places: that's why we do deep copy
                ProjectOperator groupByProj = new ProjectOperator((List<ValueExpression>)DeepCopy.copy(groupByVEs));
                firstAgg.setGroupByProjection(groupByProj);

                //current component
                lastComponent.setHashExpressions((List<ValueExpression>)DeepCopy.copy(groupByVEs));

                //next component
                OperatorComponent newComponent = new OperatorComponent(lastComponent,
                                                                      ParserUtil.generateUniqueName("OPERATOR"),
                                                                      _cg.getQueryPlan()).addOperator(firstAgg);
            }
        }else{
            throw new RuntimeException("For now only one aggregate function supported!");
        }
    }

    /*************************************************************************************
     * WHERE clause - SelectOperator
     *************************************************************************************/

    private void processWhereClause(Expression whereExpr) {
        // TODO: in non-nested case, there is a single Expression
        if (whereExpr == null) return;

        //assinging JSQL expressions to Components
        Map<String, Expression> whereCompExprPairs = getWhereForComponents(whereExpr);

        //Each component process its own part of JSQL whereExpression
        for(Map.Entry<String, Expression> whereCompExprPair: whereCompExprPairs.entrySet()){
            Component affectedComponent = _cg.getQueryPlan().getComponent(whereCompExprPair.getKey());
            Expression whereCompExpr = whereCompExprPair.getValue();
            processWhereForComponent(affectedComponent, whereCompExpr);
        }

    }

    /*
     * this method returns a list of <ComponentName, whereCompExpression>
     * @whereCompExpression part of JSQL expression which relates to the corresponding Component
     */
    private Map<String, Expression> getWhereForComponents(Expression whereExpr) {
        AndVisitor andVisitor = new AndVisitor();
        whereExpr.accept(andVisitor);
        List<Expression> atomicExprs = andVisitor.getAtomicExprs();
        List<OrExpression> orExprs = andVisitor.getOrExprs();

        /*
         * we have to group atomicExpr (conjuctive terms) by ComponentName
         *   there might be mutliple columns from a single DataSourceComponent, and we want to group them
         *
         * conditions such as R.A + R.B = 10 are possible
         *   not possible to have ColumnReference from multiple tables,
         *   because than it would be join condition
         */
        Map<String, Expression> collocatedExprs = new HashMap<String, Expression>();
        ParserUtil.addAndExprsToComps(collocatedExprs, atomicExprs);

        Map<Set<String>, Expression> collocatedOrs = new HashMap<Set<String>, Expression>();
        ParserUtil.addOrExprsToComps(collocatedOrs, orExprs);

        for(Map.Entry<Set<String>, Expression> orEntry: collocatedOrs.entrySet()){
            List<String> compNames = new ArrayList<String>(orEntry.getKey());
            List<Component> compList = ParserUtil.getComponents(compNames, _cg);
            Component affectedComponent = HierarchyExtractor.getLCM(compList);

            Expression orExpr = orEntry.getValue();
            ParserUtil.addAndExprToComp(collocatedExprs, orExpr, affectedComponent.getName());
        }

        return collocatedExprs;
    }

    /*
     * whereCompExpression is the part of WHERE clause which refers to affectedComponent
     * This is the only method in this class where IndexWhereVisitor is actually instantiated and invoked
     */
    private void processWhereForComponent(Component affectedComponent, Expression whereCompExpression){
        IndexWhereVisitor whereVisitor = new IndexWhereVisitor(affectedComponent, _schema, _pq.getTan());
        whereCompExpression.accept(whereVisitor);
        attachWhereClause(affectedComponent, whereVisitor.getSelectOperator());
    }

    private void attachWhereClause(Component affectedComponent, SelectOperator select) {
        affectedComponent.addOperator(select);
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
        EarlyProjection early = new EarlyProjection(_schema, _pq.getTan());
        early.operate(queryPlan);
    }

}