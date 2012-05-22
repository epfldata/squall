/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.JoinComponent;
import components.OperatorComponent;
import components.ThetaJoinComponent;
import conversion.DoubleConversion;
import conversion.IntegerConversion;
import conversion.NumericConversion;
import expressions.ColumnReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectionOperator;

import org.apache.log4j.Logger;

import predicates.ComparisonPredicate;
import schema.TPCH_Schema;

public class TestThetaJoin {
    private static Logger LOG = Logger.getLogger(TestThetaJoin.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public TestThetaJoin(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            DataSourceComponent relationCustomer = new DataSourceComponent(
                                            "NATION",
                                            dataPath + "nation" + extension,
                                            TPCH_Schema.customer,
                                            _queryPlan);

            //-------------------------------------------------------------------------------------
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "REGION",
                                            dataPath + "region" + extension,
                                            TPCH_Schema.orders,
                                            _queryPlan);
                                                       

            //-------------------------------------------------------------------------------------
            NumericConversion<Integer> intConv = new IntegerConversion();
            
            ColumnReference colNation = new ColumnReference(intConv, 2);
            ColumnReference colRegion = new ColumnReference(intConv, 4);
            ComparisonPredicate comp = new ComparisonPredicate(colNation, colRegion);
            ThetaJoinComponent CUSTOMER_ORDERSjoin = new ThetaJoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).setJoinPredicate(comp);

 

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