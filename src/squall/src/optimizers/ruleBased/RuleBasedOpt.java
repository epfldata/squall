/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.ruleBased;

import components.Component;
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
import optimizers.ComponentGenerator;
import optimizers.Optimizer;
import optimizers.OptimizerTranslator;
import util.HierarchyExtractor;
import util.JoinTablesExp;
import util.ParserUtil;
import util.TableAliasName;
import visitors.jsql.AndVisitor;
import visitors.jsql.ColumnCollectVisitor;
import visitors.jsql.JoinTablesExpVisitor;
import visitors.squall.SelectItemsVisitor;
import visitors.squall.WhereVisitor;


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
        int queryType = selectItemsVisitor(selectItems);
        processWhereClause(whereExpr);
        if(queryType == SelectItemsVisitor.NON_AGG){
            System.out.println("Early projection will not be performed since the query is NON_AGG type (contains projections)!");
        }else{
            earlyProjection(_cg.getQueryPlan());
        }
        
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

    private int selectItemsVisitor(List<SelectItem> selectItems) {
        //TODO: take care in nested case
        SelectItemsVisitor selectVisitor = new SelectItemsVisitor(_cg.getQueryPlan(), _schema, _tan, _ot, _map);
        for(SelectItem elem: selectItems){
            elem.accept(selectVisitor);
        }
        return selectVisitor.doneVisiting();
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
            invokeWhereVisitor(exprPerColumn, affectedComponent, _cg);
        }

        for(OrExpression orExpr: orExprs){
            List<Column> columns = invokeColumnVisitor(orExpr);
            List<Component> compList = HierarchyExtractor.getDSCwithColumn(columns, _cg);
            Component affectedComponent = HierarchyExtractor.getLCM(compList);
            invokeWhereVisitor(orExpr, affectedComponent, _cg);
        }
    }
    
    private List<Column> invokeColumnVisitor(Expression expr) {
        ColumnCollectVisitor columnCollect = new ColumnCollectVisitor();
        expr.accept(columnCollect);
        return columnCollect.getColumns();
    }

    private void invokeWhereVisitor(Expression expr, Component affectedComponent, ComponentGenerator cg){
        WhereVisitor whereVisitor = new WhereVisitor(cg.getQueryPlan(), affectedComponent, _schema, _tan, _ot);
        expr.accept(whereVisitor);
        whereVisitor.doneVisiting();
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