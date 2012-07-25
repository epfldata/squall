package sql.util;

import java.util.*;
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
public class JoinTablesExprs {
    
     private Map<String, Map<String, List<Expression>>> _tablesJoinExp = new HashMap<String, Map<String, List<Expression>>>();
     private Map<String, Map<String, List<Expression>>> _singleDirTablesJoinExp = new HashMap<String, Map<String, List<Expression>>>();

     public void addEntry(Table leftTable, Table rightTable, Expression exp){
         String leftName = ParserUtil.getComponentName(leftTable);
         String rightName = ParserUtil.getComponentName(rightTable);

         //adding to bi-directional data structure - structure used by default in methods of this class
         addToJoinMap(leftName, rightName, exp, _tablesJoinExp);
         addToJoinMap(rightName, leftName, exp, _tablesJoinExp);
         
         //adding to a single-directional data strucure
         addToJoinMap(leftName, rightName, exp, _singleDirTablesJoinExp);
     }

    private void addToJoinMap(String tblName1, String tblName2, Expression exp, Map<String, Map<String, List<Expression>>> collection) {
        if(collection.containsKey(tblName1)){
             Map<String, List<Expression>> inner = collection.get(tblName1);
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
             Map<String, List<Expression>> newInner = new HashMap<String, List<Expression>>();
             newInner.put(tblName2, expList);
             collection.put(tblName1, newInner);
         }
    }

    /*
     * Get a list of tables joinable form a set of DataSourceComponents(ancestors)
     *   This might return duplicates: For example, R.A=S.A and S.B=T.B and R.A=T.C
     *   If we want to join R-S with T, then getJoinedWith(R, S) will return (T, T)
     *   To fix the problem, we used sets, and then we converted it back to List<String>
     * For the same example R-S joined T, getJoinedWith(R, S) could (among other results) return R, S,
     *   but this is filtered at the end of this method.
     *   We don't want Sources in the results which are already in ancestors.
     */
    public List<String> getJoinedWith(List<String> ancestors) {
        Set<String> result = new HashSet<String>();
        for(String ancestor: ancestors){
            List<String> singleJoinedWith = getJoinedWith(ancestor);
            result.addAll(singleJoinedWith);
        }
        List<String> resultList = new ArrayList<String>(result);
        return ParserUtil.getDifference(resultList, ancestors);
    }

    /*
     * Get a list of tables DataSourceComponent named tblCompName can join with
     */
    public List<String> getJoinedWith(String tblCompName){
        List<String> result = getJoinedWith(_tablesJoinExp, tblCompName);
        if(result == null){
            throw new RuntimeException("Table doesn't exist in JoinTablesExp: "+tblCompName);
        }
        return result;
    }
    
    //single-directional method
    public List<String> getJoinedWithSingleDir(String tblCompName){
        return getJoinedWith(_singleDirTablesJoinExp, tblCompName);
    }
    
    private List<String> getJoinedWith(Map<String, Map<String, List<Expression>>> collection, String tblCompName) {
        if(!collection.containsKey(tblCompName)){
            return null;
        }

        List<String> joinedWith = new ArrayList<String>();
        Map<String, List<Expression>> innerMap = collection.get(tblCompName);
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
            List<Expression> delta = getExpressions(table1, tables2);
            if(delta != null){
                result.addAll(delta);
            }
        }
        if (result.isEmpty()){
            return null;
        }else{
            return result;
        }
    }

    public List<Expression> getExpressions(String table1, List<String> tables2){
        List<Expression> result = new ArrayList<Expression>();
        for(String table2: tables2){
            List<Expression> delta = getExpressions(table1, table2);
            if(delta != null){
                result.addAll(delta);
            }
        }
        if (result.isEmpty()){
            return null;
        }else{
            return result;
        }
    }

    public List<Expression> getExpressions(String tableName1, String tableName2){
        Map<String, List<Expression>> inner =_tablesJoinExp.get(tableName1);
        return inner.get(tableName2);
    }

    public boolean joinExistsBetween(String firstSource, List<String> secondAncestors) {
        List<String> joinedWith = getJoinedWith(firstSource);
        List<String> intersection = ParserUtil.getIntersection(joinedWith, secondAncestors);
        return !intersection.isEmpty();
    }

    public boolean joinExistsBetween(List<String> firstAncestors, List<String> secondAncestors) {
        for(String firstSource: firstAncestors){
            if (joinExistsBetween(firstSource, secondAncestors)){
                return true;
            }
        }
        return false;
    }

}