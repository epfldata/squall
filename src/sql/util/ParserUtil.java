package sql.util;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.ThetaJoinComponent;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import sql.optimizers.ComponentGenerator;
import sql.optimizers.cost.CostParams;
import sql.optimizers.cost.NameComponentGenerator;
import plan_runner.queryPlans.QueryPlan;
import sql.schema.ColumnNameType;
import sql.schema.Schema;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import sql.visitors.jsql.ColumnCollectVisitor;
import sql.visitors.jsql.PrintVisitor;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.ColumnRefCollectVisitor;


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
     
    /*
     * JSQL printing
     */
    public static void printParsedQuery(SQLVisitor pq){
        for(Table table: pq.getTableList()){
            String tableStr = ParserUtil.toString(table);
            System.out.println(tableStr);
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
     
    //We couldn't change toString methods without invasion to JSQL classes
    public static String getStringExpr(Expression expr) {
        PrintVisitor printer = new PrintVisitor();
        expr.accept(printer);
        return printer.getString();
    }

    public static String getStringExpr(ExpressionList params) {
        PrintVisitor printer = new PrintVisitor();
        params.accept(printer);
        return printer.getString();
    }
    
    //we use this method for List<OrExpression> as well
    public static <T extends Expression> String getStringExpr(List<T> listExpr){
        StringBuilder sb = new StringBuilder();
        int size = listExpr.size();
        for(int i=0; i<size; i++){
            sb.append(getStringExpr(listExpr.get(i)));
            if(i != size - 1){
                //not the last element
                sb.append(", ");
            }
        }
        return sb.toString();
    }     

    /*
     * Squall query plan printing - Indexes
     */    
    public static String toString(QueryPlan queryPlan) {
        StringBuilder sb = new StringBuilder("QUERY PLAN");
        for(Component comp: queryPlan.getPlan()){
            sb.append("\n\nComponent ").append(comp.getName());

            ChainOperator chain = comp.getChainOperator();
            if(!chain.isEmpty()){
                sb.append("\n").append(chain);
            }
            
            if(comp.getHashIndexes()!=null && !comp.getHashIndexes().isEmpty()){
                sb.append("\n HashIndexes: ").append(listToStr(comp.getHashIndexes()));
            }
            if(comp.getHashExpressions()!=null && !comp.getHashExpressions().isEmpty()){
                sb.append("\n HashExpressions: ").append(listToStr(comp.getHashExpressions()));
            }
        }
        sb.append("\n\nEND of QUERY PLAN");
        return sb.toString();
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
                return getComponentName(table1).equals(getComponentName(table2));
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

    public static Set<String> getSourceNameSet(Component component){
        List<DataSourceComponent> sources = component.getAncestorDataSources();
        Set<String> compNames = new HashSet<String>();
        for(DataSourceComponent source: sources){
            compNames.add(source.getName());
        }
        return compNames;
    }

    public static List<String> getSourceNameList(Component component){
        List<DataSourceComponent> sources = component.getAncestorDataSources();
        List<String> compNames = new ArrayList<String>();
        for(DataSourceComponent source: sources){
            compNames.add(source.getName());
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

    /*
     * The result will have duplicates only if there are duplicates in list1
     */
    public static <T> List<T> getIntersection(List<T> list1, List<T> list2) {
        List<T> result = new ArrayList<T>();
        for(T elem1: list1){
            if(list2.contains(elem1)){
                result.add(elem1);
            }
        }
        return result;
    }

    public static <T> List<T> getDifference(List<T> bigger, List<T> smaller) {
        List<T> result = new ArrayList<T>();
        for(T elem1: bigger){
            if(!smaller.contains(elem1)){
                result.add(elem1);
            }
        }
        return result;
    }

    public static String readFile(String path) {
        StringBuilder sb = new StringBuilder();
        try {
            String line;
            BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
            while( (line = reader.readLine()) != null){
                sb.append(line).append("\n");
            }
            sb.deleteCharAt(sb.length() - 1); //last \n is unnecessary 
            reader.close();
	} catch (Exception e) {
            String error=MyUtilities.getStackTrace(e);
            throw new RuntimeException(error);
	}
        return sb.toString();
    }
    
    public static String readSQLFromFile(String sqlPath) {
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
        String tableSchemaName = tan.getSchemaName(source.getName());
        return schema.getTableSchema(tableSchemaName).size();
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
     * append each expr to the corresponding componentName.
     * componentName is the key for collocatedExprs.
     */
    public static void addAndExprsToComps(Map<String, Expression> collocatedExprs,
            List<Expression> exprs) {

        for(Expression expr: exprs){
            //first we determine which component it belongs to
            //  In "R.A = S.A AND T.A = 4", "R.A = S.A" is not WHERE clause, it's a join condition
            List<Column> columns = getJSQLColumns(expr);
            String componentName = getComponentName(columns.get(0).getTable());
            
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
        List<Column> columns = getJSQLColumns(expr);
        Set<String> compNameSet = new HashSet<String>(getCompNamesFromColumns(columns));

        if(collocatedExprs.containsKey(compNameSet)){
            Expression oldExpr = collocatedExprs.get(compNameSet);
            Expression newExpr = new AndExpression(oldExpr, expr);
            collocatedExprs.put(compNameSet, newExpr);
        }else{
            collocatedExprs.put(compNameSet, expr);
        }
    }

    public static List<Column> getJSQLColumns(List<Expression> exprs){
        List<Column> result = new ArrayList<Column>();
        for(Expression expr: exprs){
            result.addAll(getJSQLColumns(expr));
        }
        return result;
    }

    public static List<Column> getJSQLColumns(Expression expr) {
        ColumnCollectVisitor columnCollect = new ColumnCollectVisitor();
        expr.accept(columnCollect);
        return columnCollect.getColumns();
    }

    public static List<ColumnReference> getColumnRefFromVEs(List<ValueExpression> veList){
        List<ColumnReference> crList = new ArrayList<ColumnReference>();
        for(ValueExpression ve: veList){
            ColumnRefCollectVisitor colVisitor = new ColumnRefCollectVisitor();
            ve.accept(colVisitor);
            crList.addAll(colVisitor.getColumnRefs());
        }
        return crList;
    }

    public static List<Integer> getColumnRefIndexes(List<ColumnReference> crList){
        List<Integer> intList = new ArrayList<Integer>();
        for(ColumnReference cr: crList){
            intList.add(cr.getColumnIndex());
        }
        return intList;
    }

    //throw away join hash indexes from the right parent
    public static List<ColumnNameType> joinSchema(Component[] parents, Map<String, CostParams> compCost) {
        Component leftParent = parents[0];
        Component rightParent = parents[1];
        List<ColumnNameType> leftSchema = compCost.get(leftParent.getName()).getSchema();
        List<ColumnNameType> rightSchema = compCost.get(rightParent.getName()).getSchema();

        //when HashExpressions are used the schema is a simple appending
        List<Integer> rightHashIndexes = rightParent.getHashIndexes();
        
        //******************** similar to MyUtilities.createOutputTuple
        List<ColumnNameType> outputSchema = new ArrayList<ColumnNameType>();

        for (int i = 0; i < leftSchema.size(); i++){ // add all elements of the first relation (R)
            outputSchema.add(leftSchema.get(i));
        }
        for (int i = 0; i < rightSchema.size(); i++) { // now add those
            if((rightHashIndexes == null) || (!rightHashIndexes.contains(i))){ //if does not exits add the column!! (S)
                outputSchema.add(rightSchema.get(i));
            }
        }
        return outputSchema;
        //******************** end of similar

    }

    /*
     * returns N1.NATIONNAME
     */
    public static String getFullAliasedName(Column column) {
        return ParserUtil.getComponentName(column) + "." + column.getColumnName();
    }

    public static List<ColumnNameType> getProjectedSchema(List<ColumnNameType> schema, List<Integer> hashIndexes) {
        List<ColumnNameType> result = new ArrayList<ColumnNameType>();
        for(Integer hashIndex: hashIndexes){
            //the order is improtant
            result.add(schema.get(hashIndex));
        }
        return result;
    }

    public static TypeConversion getColumnType(Column column, TableAliasName tan, Schema schema) {
        String tableCompName = ParserUtil.getComponentName(column);
        String tableSchemaName = tan.getSchemaName(tableCompName);
        String columnName = column.getColumnName();
        return schema.getType(tableSchemaName, columnName);
    }

       /*
     * From a list of <NATIONNAME, StringConversion>
     *   it creates a list of <N1.NATIONNAME, StringConversion>
     */
    public static List<ColumnNameType> createAliasedSchema(List<ColumnNameType> originalSchema, String aliasName) {
        List<ColumnNameType> result = new ArrayList<ColumnNameType>();

        for(ColumnNameType cnt: originalSchema){
            String name = cnt.getName();
            name = aliasName + "." + name;
            TypeConversion tc = cnt.getType();
            result.add(new ColumnNameType(name, tc));
        }

        return result;
    }
    
    public static List<Expression> getSubExpressions(Expression expr){
        List<Expression> result = new ArrayList<Expression>();
        if(expr instanceof BinaryExpression){
            BinaryExpression be = (BinaryExpression) expr;
            result.add(be.getLeftExpression());
            result.add(be.getRightExpression());
        }else if(expr instanceof Parenthesis){
            Parenthesis prnths = (Parenthesis) expr;
            result.add(prnths.getExpression());
        }else if(expr instanceof Function){
            Function fun = (Function) expr;
            ExpressionList params = fun.getParameters();
            if(params != null){
                result.addAll(params.getExpressions());
            }
        }else{
            return null;
        }
        return result;
    }

    /*
     * is joinComponent the last component in the query plan, in terms of no more joins to perform
     */
    public static boolean isFinalJoin(Component comp, SQLVisitor _pq) {
        Set<String> allSources = new HashSet<String>(_pq.getTan().getComponentNames());
        Set<String> actuallPlanSources = getSourceNameSet(comp);
        return allSources.equals(actuallPlanSources);
    }

    public static boolean isSameSchema(List<ColumnNameType> listSchema1, List<ColumnNameType> listSchema2) {
        Set<ColumnNameType> setSchema1 = new HashSet<ColumnNameType>(listSchema1);
        Set<ColumnNameType> setSchema2 = new HashSet<ColumnNameType>(listSchema2);
        return setSchema1.equals(setSchema2);
    }

    public static List<Expression> getJoinCondition(SQLVisitor _pq, Component left, Component right) {
        List<String> leftAncestors = ParserUtil.getSourceNameList(left);
        List<String> rightAncestors = ParserUtil.getSourceNameList(right);
        return _pq.getJte().getExpressions(leftAncestors, rightAncestors);
    }

    public static int parallelismToMap(NameComponentGenerator cg, Map _map) {
        int totalParallelism = 0;
        for(Component comp: cg.getQueryPlan().getPlan()){
            String compName = comp.getName();
            int parallelism = cg.getCostParameters(compName).getParallelism();
            totalParallelism += parallelism;
            SystemParameters.putInMap(_map, compName + "_PAR", parallelism);
        }
        return totalParallelism;
    }
}