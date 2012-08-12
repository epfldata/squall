package plan_runner.query_plans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import plan_runner.components.Component;
import plan_runner.operators.AggregateOperator;

public class QueryPlan implements Serializable{
    private static final long serialVersionUID = 1L;
    private List<Component> _plan = new ArrayList<Component>();

    //this is aggregation performed on the results from multiple tasks of the same last component
    //used for automatic check
    private AggregateOperator _overallAgg;

    public void add(Component component){
        _plan.add(component);
    }

    public List<Component> getPlan(){
        return _plan;
    }
    
    public void setOverallAggregation(AggregateOperator overallAgg){
        _overallAgg = overallAgg;
    }

    public AggregateOperator getOverallAggregation(){
        return _overallAgg;
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

    public List<String> getComponentNames(){
        List<String> result = new ArrayList<String>();
        for(Component component:_plan){
            result.add(component.getName());
        }
        return result;
    }

}
