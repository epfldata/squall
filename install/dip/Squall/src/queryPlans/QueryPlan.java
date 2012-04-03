/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.Component;
import java.util.ArrayList;
import java.util.List;

public class QueryPlan {
    private List<Component> _plan = new ArrayList<Component>();

    public void add(Component component){
        _plan.add(component);
    }

    public List<Component> getPlan(){
        return _plan;
    }

    // Component names are unique - alias is used for tables
    public boolean contains(String name){
        for(Component component:_plan){
            if(component.getName().equals(name)){
                return true;
            }
        }
        return false;
    }

    public Component getComponent(String name){
        for(Component component:_plan){
            if(component.getName().equals(name)){
                return component;
            }
        }
        return null;
    }
    
    public Component getLastComponent(){
        return _plan.get(_plan.size()-1);
    }

}
