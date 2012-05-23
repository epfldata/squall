/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans.debug;

import components.DataSourceComponent;
import conversion.IntegerConversion;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import operators.ProjectionOperator;

import org.apache.log4j.Logger;
import queryPlans.QueryPlan;
import schema.TPCH_Schema;

public class HyracksL1Plan {
    private static Logger LOG = Logger.getLogger(HyracksL1Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public HyracksL1Plan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{0, 6});
            List<Integer> hashCustomer = Arrays.asList(0);
            DataSourceComponent relationCustomer = new DataSourceComponent(
                                            "CUSTOMER",
                                            dataPath + "customer" + extension,
                                            TPCH_Schema.customer,
                                            _queryPlan).setProjection(projectionCustomer)
                                                       .setHashIndexes(hashCustomer)
                                                       .setPrintOut(false);

            //-------------------------------------------------------------------------------------
            ProjectionOperator projectionOrders = new ProjectionOperator(new int[]{1});
            List<Integer> hashOrders = Arrays.asList(0);
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "ORDERS",
                                            dataPath + "orders" + extension,
                                            TPCH_Schema.orders,
                                            _queryPlan).setProjection(projectionOrders)
                                                       .setHashIndexes(hashOrders)
                                                       .setPrintOut(false);
                                                       

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
    
}