/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers;

import components.Component;
import components.DataSourceComponent;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
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
     * List<Expression> is a set of join conditions between two components.
     */
    public Component generateEquiJoin(Component left, Component right, List<Expression> joinCondition);


}
