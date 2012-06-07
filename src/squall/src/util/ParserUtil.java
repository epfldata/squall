/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import components.Component;
import components.DataSourceComponent;
import components.ThetaJoinComponent;
import expressions.ColumnReference;
import expressions.ValueExpression;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import operators.ChainOperator;
import operators.Operator;
import optimizers.ComponentGenerator;
import queryPlans.QueryPlan;
import schema.Schema;
import utilities.MyUtilities;
import visitors.jsql.ColumnCollectVisitor;


public class ParserUtil {
     private static HashMap<String, Integer> _uniqueNumbers = new HashMap<String, Integer>();

     public static String generateUniqueName(String nameBase) {
        if(_uniqueNumbers.containsKey(nameBase)){
            int available = _uniqueNumbers.remove(nameBase);
            _uniqueNumbers.put(nameBase, available + 1);
            return nameBase + available;
        }else{
            _uniqueNumbers.put(nameBase, 1);
            return nameBase + "0";
        }
     }

     public static String toString(Table table){
        return table.getWholeTableName();
     }

     public static String toString(Join join){
        StringBuilder joinSB = new StringBuilder();

        joinSB.append("Join: my right table is ").append(join.getRightItem()).append(".");

        String type ="";
        if(join.isFull()){
            type += "Full ";
        }
        if(join.isInner()){
            type += " Inner";
        }
        if(join.isLeft()){
            type += " Left";
        }
        if(join.isNatural()){
            type += " Natural";
        }
        if(join.isOuter()){
            type += " Outer";
        }
        if(join.isRight()){
            type += " Right";
        }
        if(join.isSimple()){
            type += " Simple";
        }
        joinSB.append("\nThe join type(s): ").append(type).append(".");
        joinSB.append("\nThe join condition(s): ").append(join.getOnExpression()).append(".\n");

        String joinStr = joinSB.toString();
        return joinStr;
    }

    public static void printQueryPlan(QueryPlan queryPlan) {
        StringBuilder sb = new StringBuilder("QUERY PLAN");
        for(Component comp: queryPlan.getPlan()){
            sb.append("\n\nComponent ").append(comp.getName());

            sb.append("\n").append(comp.getChainOperator());
            if(comp.getHashIndexes()!=null && !comp.getHashIndexes().isEmpty()){
                sb.append("\n HashIndexes: ").append(listToStr(comp.getHashIndexes()));
            }
            if(comp.getHashExpressions()!=null && !comp.getHashExpressions().isEmpty()){
                sb.append("\n HashExpressions: ").append(listToStr(comp.getHashExpressions()));
            }
        }
        System.out.println(sb.toString());
    }

    public static <T> String listToStr(List<T> list){
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for(int i=0; i<list.size(); i++){
            sb.append(list.get(i));
            if(i==list.size()-1){
                sb.append(")");
            }else{
                sb.append(", ");
            }
        }
        return sb.toString();
   }

    public static boolean equals(Table table1, Table table2) {
        if (table1==null){
            return table2==null;
        }else{
            if(table2!=null){
                return ParserUtil.getComponentName(table1).equals(ParserUtil.getComponentName(table2));
            }else{
                return false;
            }
        }
    }

    public static String getComponentName(Table table){
         String tableName = table.getAlias();
         if(tableName == null){
            tableName = table.getName();
         }
         return tableName;
    }

    public static String getComponentName(Column column) {
        return getComponentName(column.getTable());
    }

    public static List<String> getCompNamesFromColumns(List<Column> columns) {
        List<String> compNameList = new ArrayList<String>();
        for(Column column: columns){
            String compName = getComponentName(column);
            compNameList.add(compName);
        }
        return compNameList;
    }

    /*
     * We assume all the columns refer to the same DataSourceComponent
     */
    public static String getComponentName(List<Column> columns){
        //all the columns will be from the same table, so we can choose any of them
        //  (here we choose the first one)
        Column column = columns.get(0);
        return getComponentName(column);
    }

    public static Set<String> getSourceNameSet(List<DataSourceComponent> components){
        Set<String> compNames = new HashSet<String>();
        for(Component component: components){
            compNames.add(component.getName());
        }
        return compNames;
    }

    /*
     * Find component in a query name with a given name
     */
    public static List<Component> getComponents(List<String> compNameList, ComponentGenerator cg) {
        List<Component> compList = new ArrayList<Component>();
        for(String compName: compNameList){
            compList.add(getComponent(compName, cg));
        }
        return compList;
    }

    /*
     * Find component in a query name with a given name
     */
    public static Component getComponent(String compName, ComponentGenerator cg){
        return cg.getQueryPlan().getComponent(compName);
    }


    public static List<Integer> extractColumnIndexes(List<ValueExpression> veList) {
        List<Integer> indexes = new ArrayList<Integer>();
        for(ValueExpression ve: veList){
            if(ve instanceof ColumnReference){
                ColumnReference cr = (ColumnReference) ve;
                indexes.add(cr.getColumnIndex());
//          }else if(ve instanceof IntegerYearFromDate){
//                //SUPPORT FOR EXTRACT_YEAR
//                IntegerYearFromDate iyfd = (IntegerYearFromDate) ve;
//                ColumnReference<Date> veDate = (ColumnReference<Date>) iyfd.getInnerExpressions().get(0);
//                indexes.add(veDate.getColumnIndex());
            }else{
                throw new RuntimeException("Should check with isAllColumnReferences before use!");
            }
        }
        return indexes;
    }

    public static boolean isAllColumnRefs(List<ValueExpression> veList) {
        for(ValueExpression ve: veList){
            if (!(ve instanceof ColumnReference)){
                return false;
            }
        }
        return true;
    }

    /*
     * get the number of elements with the value in a segment [0, endIndex).
     *   Used to get the number of hashIndexes(elements) which are smaller than index(endIndex).
     */
    public static int getNumElementsBefore(int endIndex, List<Integer> elements) {
        int numBefore = 0;
        for(int i=0; i<endIndex; i++){
            if(elements.contains(i)){
                numBefore++;
            }
        }
        return numBefore;
    }

    //template method
    public static <T> List<T> createListExp(T elem) {
        List<T> result = new ArrayList<T>();
        result.add(elem);
        return result;
    }

    public static int[] listToArr(List<Integer> list){
        int[] arr = new int[list.size()];
        for(int i=0; i<list.size(); i++){
            arr[i] = list.get(i);
        }
        return arr;
    }

    public static <T> List<T> getIntersection(List<T> list1, List<T> list2) {
        List<T> result = new ArrayList<T>();
        for(T elem1: list1){
            for(T elem2: list2){
                if (elem1.equals(elem2)){
                    result.add(elem1);
                }
            }
        }
        return result;
    }

    public static String readStringFromFile(String sqlPath) {
        StringBuilder sqlstring = new StringBuilder();
        try {
            String line="";
            BufferedReader reader = new BufferedReader(new FileReader(new File(sqlPath)));
            while( (line = reader.readLine()) != null){

                // remove leading and trailing whitespaces
                line = line.trim();
            	if (line.length()!=0 && line.charAt(0)!='\n' //empty line
                        && line.charAt(0)!='\r' //empty line
                        && line.charAt(0)!='#'){  //commented line
                    sqlstring.append(line).append(" ");
                }
            }
            reader.close();
	} catch (Exception e) {
            String error=MyUtilities.getStackTrace(e);
            throw new RuntimeException(error);
	}
        return sqlstring.toString();
    }

    /*
     * On each component order the Operators as Select, Distinct, Project, Aggregation
     */
    public static void orderOperators(QueryPlan queryPlan) {
        List<Component> comps = queryPlan.getPlan();
        for(Component comp: comps){
            ChainOperator chain = comp.getChainOperator();
            chain.setOperators(orderOperators(chain));
        }
    }

    private static List<Operator> orderOperators(ChainOperator chain) {
        List<Operator> result = new ArrayList<Operator>();

        Operator selection = chain.getSelection();
        if (selection!=null) result.add(selection);

        Operator distinct = chain.getDistinct();
        if (distinct!=null) result.add(distinct);

        Operator projection = chain.getProjection();
        if (projection!=null) result.add(projection);

        Operator agg = chain.getAggregation();
        if (agg!=null) result.add(agg);

        return result;
    }

    /*
     * Used in Simple and Rule optimizer
     * Used when index of a column has to be obtained
     *   before EarlyProjection is performed.
     */
    public static int getPreOpsOutputSize(DataSourceComponent source, Schema schema, TableAliasName tan){
        String tableName = tan.getSchemaName(source.getName());
        return schema.getColumnNameTypes(tableName).size();
    }

    public static int getPreOpsOutputSize(Component component, Schema schema, TableAliasName tan){
        if(component instanceof ThetaJoinComponent){
            throw new RuntimeException("SQL generator with Theta does not work for now!");
            //TODO similar to Equijoin, but not subtracting joinColumnsLength
        }

        Component[] parents = component.getParents();
        if(parents == null){
            //this is a DataSourceComponent
            return getPreOpsOutputSize((DataSourceComponent)component, schema, tan);
        }else if(parents.length == 1){
            return getPreOpsOutputSize(parents[0], schema, tan);
        }else if(parents.length == 2){
            Component firstParent = parents[0];
            Component secondParent = parents[1];
            int joinColumnsLength = firstParent.getHashIndexes().size();
            return getPreOpsOutputSize(firstParent, schema, tan) + getPreOpsOutputSize(secondParent, schema, tan) - joinColumnsLength;
        }
        throw new RuntimeException("More than two parents for a component " + component);
    }

    /*
     * append each expr to the corresponding componentName
     * componentName is the key for collocatedExprs
     */
    public static void addAndExprsToComps(Map<String, Expression> collocatedExprs,
            List<Expression> exprs) {

        for(Expression expr: exprs){
            List<Column> columns = invokeColumnVisitor(expr);
            String componentName = ParserUtil.getComponentName(columns.get(0).getTable());
            addAndExprToComp(collocatedExprs, expr, componentName);
        }

    }

    /*
     * append expr to the corresponding component
     * Used outside this class, that's why compName is not extracted from expr
     */
    public static void addAndExprToComp(Map<String, Expression> collocatedExprs,
            Expression expr,
            String compName) {

        if(collocatedExprs.containsKey(compName)){
            Expression oldExpr = collocatedExprs.get(compName);
            Expression newExpr = new AndExpression(oldExpr, expr);
            collocatedExprs.put(compName, newExpr);
        }else{
            collocatedExprs.put(compName, expr);
        }

    }

    /*
     * This has to same semantic as addAndExprToComps
     * the only difference is that collocatedExprs is a different type
     *   we need to keep all the ancestor DataSources which correspond to appropriate orExpr
     */
    public static void addOrExprsToComps(Map<Set<String>, Expression> collocatedExprs, List<OrExpression> orExprs) {
        for(OrExpression orExpr: orExprs){
            addOrExprToComp(collocatedExprs, orExpr);
        }
    }

    public static void addOrExprToComp(Map<Set<String>, Expression> collocatedExprs, OrExpression expr) {
        List<Column> columns = ParserUtil.invokeColumnVisitor(expr);
        Set<String> compNameSet = new HashSet<String>(ParserUtil.getCompNamesFromColumns(columns));

        if(collocatedExprs.containsKey(compNameSet)){
            Expression oldExpr = collocatedExprs.get(compNameSet);
            Expression newExpr = new AndExpression(oldExpr, expr);
            collocatedExprs.put(compNameSet, newExpr);
        }else{
            collocatedExprs.put(compNameSet, expr);
        }
    }

    public static List<Column> invokeColumnVisitor(Expression expr) {
        ColumnCollectVisitor columnCollect = new ColumnCollectVisitor();
        expr.accept(columnCollect);
        return columnCollect.getColumns();
    }

}