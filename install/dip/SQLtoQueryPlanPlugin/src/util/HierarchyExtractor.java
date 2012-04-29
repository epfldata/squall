/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import components.Component;
import components.DataSourceComponent;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import queryPlans.QueryPlan;
import schema.Schema;


public class HierarchyExtractor {

    public static List<String> getAncestorNames(Component component){
        List<DataSourceComponent> ancestors = component.getAncestorDataSources();
        List<String> ancestorNames = new ArrayList<String>();
        for (DataSourceComponent ancestor: ancestors){
            ancestorNames.add(ancestor.getName());
        }
        return ancestorNames;
    }

    public static List<Table> getAncestorTables(Component component, TableAliasName tan){
        List<String> ancestorNames = getAncestorNames(component);
        List<Table> ancestorTables = new ArrayList<Table>();
        for(String ancestorName: ancestorNames){
            ancestorTables.add(tan.getTable(ancestorName));
        }
        return ancestorTables;
    }

    public static Component getLCM(List<Component> compList) {
        Component resultLCM = getLCM(compList.get(0), compList.get(1));
        for(int i=2; i<compList.size(); i++){
            resultLCM = getLCM(resultLCM, compList.get(i));
        }
        return resultLCM;
    }

    public static Component getLCM(Component first, Component second){
        //TODO problem nested: we have multiple children
        Component resultComp = first;
        List<String> resultAnc = getAncestorNames(resultComp);
        while (!resultAnc.contains(second.getName())){
            resultComp = resultComp.getChild();
            resultAnc = getAncestorNames(resultComp);
        }
        return resultComp;
    }

    //not meant to be used with projections - EarlyProjection is the very last thing done on the plan
    public static int extractComponentIndex(Column column, Component requestor, QueryPlan queryPlan, Schema schema, TableAliasName tan){
        Table table = column.getTable();
        String tableSchemaName = tan.getSchemaName(ParserUtil.getComponentName(table));
        String tblCompName = ParserUtil.getComponentName(table);
        Component originator = queryPlan.getComponent(tblCompName);
        String columnName = column.getColumnName();

        int originalIndex = schema.indexOf(tableSchemaName, columnName);

        if (originator.equals(requestor)){
            return originalIndex;
        }else{
            return getChildIndex(originalIndex, originator, requestor);
        }
    }

    public static int getChildIndex(int originalIndex, Component originator, Component requestor){
        Component child = originator.getChild();
        Component[] parents = child.getParents();

        if(child.getProjection()!=null){
            throw new RuntimeException("Cannot use getChildIndex method on the component with Projection! getOutputSize does not work anymore!");
        }

        int index = originalIndex;

        if(parents.length < 2){
            //no changes, only one parent
            return index;
        }

        //only right parent changes the index
        Component leftParent = parents[0];
        Component rightParent = parents[1];

        if (rightParent.equals(originator)){
            if(!rightParent.getHashIndexes().contains(originalIndex)){
                //requested column is *not* in joinColumns
                int indexesBefore = getNumHashIndexesBefore(originalIndex, rightParent.getHashIndexes());
                index = leftParent.getOutputSize() - indexesBefore + originalIndex;
            }else{
                //requested column is in joinColumns
                //if in the keys have to find lhs index
                int joinIndex = rightParent.getHashIndexes().indexOf(originalIndex);
                index = leftParent.getHashIndexes().get(joinIndex);
            }
        }

        if(child.equals(requestor)){
            return index;
        }else{
            return getChildIndex(index, originator.getChild(), requestor);
        }
    }

    public static int getNumHashIndexesBefore(int originalIndex, List<Integer> hashIndexes) {
        int numBefore = 0;
        for(int i=0; i<originalIndex; i++){
            if(hashIndexes.contains(i)){
                numBefore++;
            }
        }
        return numBefore;
    }

    //is affectedComponent already hashed by hashColumns
    public static boolean isHashedBy(Component affectedComponent, List<Integer> affectedHashIndexes) {
        Component[] parents = affectedComponent.getParents();
        if(parents!=null){
            //if both parents have only hashIndexes, they point to the same indexes in the child
            //so we choose arbitrarily first parent
            Component parent = parents[0];
            List<Integer> parentHashes = parent.getHashIndexes();
            if(parent.getHashExpressions() == null){
                List<Integer> parentHashIndexes = new ArrayList<Integer>();
                for(int parentHash: parentHashes){
                    parentHashIndexes.add(HierarchyExtractor.getChildIndex(parentHash, parent, affectedComponent));
                }
                return isSuperset(parentHashIndexes, affectedHashIndexes);
            }
        }
        return false;
    }

    /*
     * Return true if hashes are equivalent, or parent groups tuples exactly the same as the affected component,
     *   with addition of some more columns. This means that Join and Aggregation can be performed on the same node.
     * Inspiration taken from the Nephele paper.
     */
    private static boolean isSuperset(List<Integer> parentHashIndexes, List<Integer> affectedHashIndexes) {
        int parentSize = parentHashIndexes.size();
        int affectedSize = affectedHashIndexes.size();

        if (parentSize < affectedSize){
            return false;
        }else if(parentSize == affectedSize){
            return parentHashIndexes.equals(affectedHashIndexes);
        }else{
            for(int i=0; i<affectedSize; i++){
                if (!(affectedHashIndexes.get(i).equals(parentHashIndexes.get(i)))){
                    return false;
                }
            }
            return true;
        }
    }
}