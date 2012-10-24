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
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryPlan;

public class HyracksL3BatchPlan {
    private static Logger LOG = Logger.getLogger(HyracksL3BatchPlan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public HyracksL3BatchPlan(String dataPath, String extension, Map conf){
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

            AggregateCountOperator postAgg = new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(1));
            List<Integer> hashIndexes = Arrays.asList(0);
            EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
                    relationCustomer,
                    relationOrders,
                    _queryPlan).addOperator(postAgg)
                               .setHashIndexes(hashIndexes)
                               .setBatchOutputMillis(1000);

            //-------------------------------------------------------------------------------------
            AggregateSumOperator agg = new AggregateSumOperator(new ColumnReference(_ic, 1), conf)
                    .setGroupByColumns(Arrays.asList(0));

            OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG", _queryPlan)
                                        .addOperator(agg)
                                        .setFullHashList(Arrays.asList("FURNITURE", "BUILDING", "MACHINERY", "HOUSEHOLD", "AUTOMOBILE"));

            //-------------------------------------------------------------------------------------

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
    
}