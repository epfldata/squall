/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans.debug;

import components.DataSourceComponent;
import components.JoinComponent;
import components.OperatorComponent;
import conversion.IntegerConversion;
import expressions.ColumnReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectionOperator;

import org.apache.log4j.Logger;
import queryPlans.QueryPlan;
import schema.TPCH_Schema;

public class HyracksL3Plan {
    private static Logger LOG = Logger.getLogger(HyracksL3Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public HyracksL3Plan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{0, 6});
            List<Integer> hashCustomer = Arrays.asList(0);
            DataSourceComponent relationCustomer = new DataSourceComponent(
                                            "CUSTOMER",
                                            dataPath + "customer" + extension,
                                            TPCH_Schema.customer,
                                            _queryPlan).setProjection(projectionCustomer)
                                                       .setHashIndexes(hashCustomer);

            //-------------------------------------------------------------------------------------
            ProjectionOperator projectionOrders = new ProjectionOperator(new int[]{1});
            List<Integer> hashOrders = Arrays.asList(0);
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "ORDERS",
                                            dataPath + "orders" + extension,
                                            TPCH_Schema.orders,
                                            _queryPlan).setProjection(projectionOrders)
                                                       .setHashIndexes(hashOrders);
                                                       

            //-------------------------------------------------------------------------------------
            List<Integer> hashIndexes = Arrays.asList(1);
            JoinComponent CUSTOMER_ORDERSjoin = new JoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).setHashIndexes(hashIndexes);

            //-------------------------------------------------------------------------------------
            AggregateCountOperator agg = new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(1));

            OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG", _queryPlan)
                                        .setAggregation(agg)
                                        .setFullHashList(Arrays.asList("FURNITURE", "BUILDING", "MACHINERY", "HOUSEHOLD", "AUTOMOBILE"));

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