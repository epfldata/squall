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
import optimizers.IndexComponentGenerator;

/*
 * A utility class for extracting different hierarchy-(topology-)related information
 */
public class HierarchyExtractor {

    public static List<String> getAncestorNames(Component component){
        List<DataSourceComponent> ancestors = component.getAncestorDataSources();
        List<String> ancestorNames = new ArrayList<String>();
        for (DataSourceComponent ancestor: ancestors){
            ancestorNames.add(ancestor.getName());
        }
        return ancestorNames;
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

    /*
     * This method finds a DataSourceComponent a column refers to
     *   columnName is in form table.column, i.e. N2.NATIONKEY
     */
    public static Component getSourcewithColumn(String columnName, IndexComponentGenerator cg){
        String tableCompName = columnName.split("\\.")[0];
        Component affectedComponent = cg.getQueryPlan().getComponent(tableCompName);
        return affectedComponent;
    }

    public static Component getSourcewithColumn(Column column, IndexComponentGenerator cg) {
        String tableCompName = ParserUtil.getComponentName(column.getTable());
        Component affectedComponent = cg.getQueryPlan().getComponent(tableCompName);
        return affectedComponent;
    }

    public static List<Component> getSourcewithColumn(List<Column> columns, IndexComponentGenerator cg) {
        List<Component> compList = new ArrayList<Component>();
        for(Column column: columns){
            Component newComp = getSourcewithColumn(column, cg);
            compList.add(newComp);
        }
        return compList;
    }

}