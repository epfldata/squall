/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.JoinComponent;
import components.OperatorComponent;
import conversion.DoubleConversion;
import conversion.IntegerConversion;
import conversion.StringConversion;
import expressions.ColumnReference;
import expressions.ValueSpecification;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectionOperator;

import org.apache.log4j.Logger;
import schema.TPCH_Schema;
import stormComponents.JoinAggStorage;
import stormComponents.JoinStorage;

public class HyracksPreAggPlan {
    private static Logger LOG = Logger.getLogger(HyracksPlan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final DoubleConversion _dc = new DoubleConversion();
    private static final StringConversion _sc = new StringConversion();

    public HyracksPreAggPlan(String dataPath, String extension, Map conf){
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
            ProjectionOperator projFirstOut = new ProjectionOperator(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, "1"));
            ProjectionOperator projSecondOut = new ProjectionOperator(new int[]{1, 2});
            JoinStorage secondJoinStorage = new JoinAggStorage(new AggregateCountOperator(conf), conf);
            
            ArrayList<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(0));
            JoinComponent CUSTOMER_ORDERSjoin = new JoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).setFirstPreAggProj(projFirstOut)
                               .setSecondPreAggProj(projSecondOut)
                               .setSecondPreAggStorage(secondJoinStorage)
                               .setHashIndexes(hashIndexes);

            //-------------------------------------------------------------------------------------           
            AggregateSumOperator agg = new AggregateSumOperator(_dc, new ColumnReference(_dc, 1), conf)
                    .setGroupByColumns(Arrays.asList(0));

            OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG", _queryPlan)
                                        .setAggregation(agg);

            //-------------------------------------------------------------------------------------
            
            AggregateOperator overallAgg =
                    new AggregateSumOperator(_dc, new ColumnReference(_dc, 1), conf)
                        .setGroupByColumns(Arrays.asList(0));

            _queryPlan.setOverallAggregation(overallAgg);

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }

}
