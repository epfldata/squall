package sql.optimizers;

import java.util.List;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.queryPlans.QueryPlan;


public interface CompGen {

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
