/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import components.Component;
import components.DataSourceComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
     * Is component a child (not necessarily first generation) of all orCompNames?
     * Used in Cost-based optimizer
     */
    public static boolean isLCM(Component component, Set<String> orCompNames) {
        //dealing with parents
        Component[] parents = component.getParents();
        int numParents = parents.length;
        if(parents == null){
            //if I don't have parents I can't be LCM (I am DataSourceComponent)
            return false;
        }

        for(int i=0; i<numParents; i++){
            Component parent = parents[i];
            Set<String> parentAncestors = ParserUtil.getSourceNameSet(parent.getAncestorDataSources());
            if(contains(parentAncestors, orCompNames)){
                //my parent is LCM (or its parent)
                return false;
            }
        }

        //if I contain all the mentioned sources, and none of my parent does so, than I am LCM
        Set<String> compAncestors = ParserUtil.getSourceNameSet(component.getAncestorDataSources());
        return contains(compAncestors, orCompNames);
    }

    private static boolean contains(Set<String> biggerSet, Set<String> smallerSet) {
        if (biggerSet.size() < smallerSet.size()){
            return false;
        }
        for(String smallerElem: smallerSet){
            if (!biggerSet.contains(smallerElem)){
                return false;
            }
        }
        return true;
    }

}