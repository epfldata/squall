/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.JoinComponent;
import components.OperatorComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.ProjectionOperator;

import org.apache.log4j.Logger;
import schema.TPCH_Schema;

public class HyracksPlan {
    private static Logger LOG = Logger.getLogger(HyracksPlan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    //this is aggregation performed on the results from multiple tasks of the same last component
    //used for automatic check
    private AggregateOperator _overallAgg;

    public HyracksPlan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{0, 6});
            ArrayList<Integer> hashCustomer = new ArrayList<Integer>(Arrays.asList(0));
            DataSourceComponent relationCustomer = new DataSourceComponent(
                                            "CUSTOMER",
                                            dataPath + "customer" + extension,
                                            TPCH_Schema.customer,
                                            _queryPlan).setProjection(projectionCustomer)
                                                       .setHashIndexes(hashCustomer);

            //-------------------------------------------------------------------------------------
            ProjectionOperator projectionOrders = new ProjectionOperator(new int[]{1});
            ArrayList<Integer> hashOrders = new ArrayList<Integer>(Arrays.asList(0));
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "ORDERS",
                                            dataPath + "orders" + extension,
                                            TPCH_Schema.orders,
                                            _queryPlan).setProjection(projectionOrders)
                                                       .setHashIndexes(hashOrders);
                                                       

            //-------------------------------------------------------------------------------------
            ArrayList<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(1));
            JoinComponent CUSTOMER_ORDERSjoin = new JoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).setHashIndexes(hashIndexes);

            //-------------------------------------------------------------------------------------
            AggregateCountOperator agg = new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(1));

            OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG", _queryPlan)
                                        .setAggregation(agg);

            //-------------------------------------------------------------------------------------

//            AggregateCountOperator agg = new AggregateCountOperator().setGroupByColumns(Arrays.asList(1));
//
//            JoinComponent CUSTOMER_ORDERSjoin = new JoinComponent(
//                    relationCustomer,
//                    relationOrders,
//                    _queryPlan).setAggregation(agg);
//
//            //-------------------------------------------------------------------------------------

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}