/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.ThetaJoinComponent;
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
import predicates.ComparisonPredicate;

public class ThetaHyracksPlan {
    private static Logger LOG = Logger.getLogger(ThetaHyracksPlan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public ThetaHyracksPlan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 6});
            List<Integer> hashCustomer = Arrays.asList(0);
            DataSourceComponent relationCustomer = new DataSourceComponent(
                                            "CUSTOMER",
                                            dataPath + "customer" + extension,
                                            _queryPlan).addOperator(projectionCustomer)
                                                       .setHashIndexes(hashCustomer);

            //-------------------------------------------------------------------------------------
            ProjectOperator projectionOrders = new ProjectOperator(new int[]{1});
            List<Integer> hashOrders = Arrays.asList(0);
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "ORDERS",
                                            dataPath + "orders" + extension,
                                            _queryPlan).addOperator(projectionOrders)
                                                       .setHashIndexes(hashOrders);
                                                       
            //-------------------------------------------------------------------------------------

            ColumnReference colCustomer = new ColumnReference(_ic, 0);
            ColumnReference colOrders = new ColumnReference(_ic, 0);
            ComparisonPredicate comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colCustomer, colOrders);

            AggregateCountOperator agg = new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(1));

            ThetaJoinComponent CUSTOMER_ORDERSjoin = new ThetaJoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).addOperator(agg)
                               .setJoinPredicate(comp);

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