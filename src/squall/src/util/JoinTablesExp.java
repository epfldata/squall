/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

    /*
     *  **Only conjunctive join conditions are supported!**
     *
     *  R,S: R.A=S.A is represented as:
     *  {R, {S, R.A = S.A}} and {S, {R, R.A = S.A}}
     *    so that we can inquire for both tables
     *
     *  Expression are always EqualsTo, unless from ThetaJoinComponent
     */
public class JoinTablesExp {
    
     private HashMap<String, HashMap<String, List<Expression>>> _tablesJoinExp = new HashMap<String, HashMap<String, List<Expression>>>();
     private TableAliasName _tan;

     public JoinTablesExp(TableAliasName tan){
         _tan = tan;
     }

     public void addEntry(Table leftTable, Table rightTable, Expression exp){
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

    /*
     * Get a list of tables joinable form a set of DataSourceComponents(ancestors)
     *   This might return duplicates: For example, R.A=S.A and S.B=T.B and R.A=T.C
     *   If we want to join R-S with T, then getJoinedWith(R, S) will return (T, T)
     *   To fix the problem, we used sets, and then we converted it back to List<String>
     */
    public List<String> getJoinedWith(List<String> ancestors) {
        Set<String> result = new HashSet<String>();
        for(String ancestor: ancestors){
            List<String> singleJoinedWith = getJoinedWith(ancestor);
            result.addAll(singleJoinedWith);
        }
        return new ArrayList<String>(result);
    }

    /*
     * Get a list of tables DataSourceComponent named tblCompName can join with
     */
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

    /*
     * Get a list of join condition expressions.
     * For EquiJoin, it's in form of EqualsTo.
     * We support only conjunctive join conditions.
     */
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

    public List<Expression> getExpressions(String tableName1, String tableName2){
        HashMap<String, List<Expression>> inner =_tablesJoinExp.get(tableName1);
        return inner.get(tableName2);
    }
}