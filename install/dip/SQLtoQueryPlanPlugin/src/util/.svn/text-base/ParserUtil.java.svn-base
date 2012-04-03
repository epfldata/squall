/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import components.Component;
import expressions.ColumnReference;
import expressions.IntegerYearFromDate;
import expressions.ValueExpression;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import optimizers.ComponentGenerator;
import queryPlans.QueryPlan;
import utilities.MyUtilities;


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

            if(comp.getSelection()!=null){
                sb.append("\n").append(comp.getSelection().toString());
            }
            if(comp.getProjection()!=null){
                sb.append("\n").append(comp.getProjection().toString());
            }
            if(comp.getDistinct()!=null){
                sb.append("\n").append(comp.getDistinct().toString());
            }
            if(comp.getAggregation()!=null){
                sb.append("\n").append(comp.getAggregation().toString());
            }
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

    public static List<Integer> extractColumnIndexes(List<ValueExpression> veList) {
        ArrayList<Integer> indexes = new ArrayList<Integer>();
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

    //columnName is in form table.column, i.e. N2.NATIONKEY
    public static Component columnToComp(String columnName, ComponentGenerator cg){
        String tableCompName = columnName.split("\\.")[0];
        Component affectedComponent = cg.getQueryPlan().getComponent(tableCompName);
        return affectedComponent;
    }

    public static Component columnToComp(Column column, ComponentGenerator cg) {
        String tableCompName = ParserUtil.getComponentName(column.getTable());
        Component affectedComponent = cg.getQueryPlan().getComponent(tableCompName);
        return affectedComponent;
    }

    public static List<Component> columnToComp(List<Column> columns, ComponentGenerator cg) {
        List<Component> compList = new ArrayList<Component>();
        for(Column column: columns){
            Component newComp = columnToComp(column, cg);
            compList.add(newComp);
        }
        return compList;
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
}