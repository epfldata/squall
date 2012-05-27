/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;


public class JoinTablesExp {
    //R,S: R.A=S.A is represented as:
    //{R, {S, R.A = S.A}} and {S, {R, R.A = S.A}}
    //so that we can inquire for both tables
                                                //this Expressions are EqualsTo
     private HashMap<String, HashMap<String, List<Expression>>> _tablesJoinExp = new HashMap<String, HashMap<String, List<Expression>>>();
     private TableAliasName _tan;

     public JoinTablesExp(TableAliasName tan){
         _tan = tan;
     }

     public void addEntry(Table leftTable, Table rightTable, EqualsTo exp){
         String leftName = ParserUtil.getComponentName(leftTable);
         String rightName = ParserUtil.getComponentName(rightTable);

         addToJoinMap(leftName, rightName, exp);
         addToJoinMap(rightName, leftName, exp);
     }

    private void addToJoinMap(String tblName1, String tblName2, Expression exp) {
        if(_tablesJoinExp.containsKey(tblName1)){
             HashMap<String, List<Expression>> inner = _tablesJoinExp.get(tblName1);
             if(inner.containsKey(tblName2)){
                List<Expression> expList = inner.get(tblName2);
                expList.add(exp);
             }else{
                 List<Expression> expList = new ArrayList<Expression>();
                 expList.add(exp);
                 inner.put(tblName2, expList);
             }
         }else{
             List<Expression> expList = new ArrayList<Expression>();
             expList.add(exp);
             HashMap<String, List<Expression>> newInner = new HashMap<String, List<Expression>>();
             newInner.put(tblName2, expList);
             _tablesJoinExp.put(tblName1, newInner);
         }
    }

    public List<String> getJoinedWith(Table table){
        return getJoinedWith(ParserUtil.getComponentName(table));
    }

    public List<String> getJoinedWith(List<String> ancestors) {
        List<String> result = new ArrayList<String>();
        for(String ancestor: ancestors){
            List<String> singleJoinedWith = getJoinedWith(ancestor);
            result.addAll(singleJoinedWith);
        }
        return result;
    }

    public List<String> getJoinedWith(String tblCompName){
        if(!_tablesJoinExp.containsKey(tblCompName)){
            throw new RuntimeException("Table doesn't exist in JoinTablesExp: "+tblCompName);
        }

        List<String> joinedWith = new ArrayList<String>();
        HashMap<String, List<Expression>> innerMap = _tablesJoinExp.get(tblCompName);
        for(Map.Entry<String, List<Expression>> entry: innerMap.entrySet()){
            joinedWith.add(entry.getKey());
        }
        return joinedWith;
    }

   public List<Expression> getExpressions(List<String> tables1, List<String> tables2) {
        List<Expression> result = new ArrayList<Expression>();
        for(String table1: tables1){
            result.addAll(getExpressions(table1, tables2));
        }
        return result;
    }

    public List<Expression> getExpressions(String table1, List<String> tables2){
        List<Expression> result = new ArrayList<Expression>();
        for(String table2: tables2){
            List<Expression> delta = getExpressions(table1, table2);
            if(delta != null){
                result.addAll(delta);
            }
        }
        return result;
    }


    public List<Expression> getExpressions(Table table1, Table table2){
        String tableName1 = ParserUtil.getComponentName(table1);
        String tableName2 = ParserUtil.getComponentName(table2);

        return getExpressions(tableName1, tableName2);
    }

    public List<Expression> getExpressions(String tableName1, String tableName2){
        HashMap<String, List<Expression>> inner =_tablesJoinExp.get(tableName1);
        return inner.get(tableName2);
    }
}