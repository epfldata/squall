package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryPlan;

public class HyracksL3Plan {
    private static Logger LOG = Logger.getLogger(HyracksL3Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public HyracksL3Plan(String dataPath, String extension, Map conf){
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
            List<Integer> hashIndexes = Arrays.asList(1);
            EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).setHashIndexes(hashIndexes);

            //-------------------------------------------------------------------------------------
            AggregateCountOperator agg = new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(1));

            OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG", _queryPlan)
                                        .addOperator(agg)
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