package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryPlan;

public class HyracksL1Plan {
    private static Logger LOG = Logger.getLogger(HyracksL3Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public HyracksL1Plan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            ProjectOperator projectionOrders = new ProjectOperator(new int[]{1});
            List<Integer> hashOrders = Arrays.asList(0);
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "ORDERS",
                                            dataPath + "orders" + extension,
                                            _queryPlan).addOperator(projectionOrders)
                                                       .setHashIndexes(hashOrders);

            //-------------------------------------------------------------------------------------
            OperatorComponent oc = new OperatorComponent(relationOrders, "DUMMY", _queryPlan)
									  .setPrintOut(false);
            
            //-------------------------------------------------------------------------------------

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
    
}