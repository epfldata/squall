/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.EquiJoinComponent;
import conversion.IntegerConversion;
import expressions.ColumnReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectOperator;

import org.apache.log4j.Logger;
import schema.TPCH_Schema;

public class HyracksPlan {
    private static Logger LOG = Logger.getLogger(HyracksPlan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public HyracksPlan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 6});
            List<Integer> hashCustomer = Arrays.asList(0);
            DataSourceComponent relationCustomer = new DataSourceComponent(
                                            "CUSTOMER",
                                            dataPath + "customer" + extension,
                                            TPCH_Schema.customer,
                                            _queryPlan).addOperator(projectionCustomer)
                                                       .setHashIndexes(hashCustomer);

            //-------------------------------------------------------------------------------------
            ProjectOperator projectionOrders = new ProjectOperator(new int[]{1});
            List<Integer> hashOrders = Arrays.asList(0);
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "ORDERS",
                                            dataPath + "orders" + extension,
                                            TPCH_Schema.orders,
                                            _queryPlan).addOperator(projectionOrders)
                                                       .setHashIndexes(hashOrders);
                                                       
            //-------------------------------------------------------------------------------------

            AggregateCountOperator agg = new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(1));

            EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).addOperator(agg);

            //-------------------------------------------------------------------------------------

            AggregateOperator overallAgg =
                    new AggregateSumOperator(_ic, new ColumnReference(_ic, 1), conf)
                        .setGroupByColumns(Arrays.asList(0));

            _queryPlan.setOverallAggregation(overallAgg);
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
    
}