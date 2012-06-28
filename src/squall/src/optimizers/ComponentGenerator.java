package optimizers;

import components.Component;
import components.DataSourceComponent;
import java.util.List;
import queryPlans.QueryPlan;


public interface ComponentGenerator {

     public QueryPlan getQueryPlan();

    public List<Component> getSubPlans();

    /*
     * adding a DataSourceComponent to the list of components
     * Necessary to call only when only one table is addresses in WHERE clause of a SQL query
     */
    public DataSourceComponent generateDataSource(String tableCompName);

    /*
     * Join between two components
     */
    public Component generateEquiJoin(Component left, Component right);


}
