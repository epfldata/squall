package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryPlan;

public class HyracksL1Plan {
    private static Logger LOG = Logger.getLogger(HyracksL1Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final IntegerConversion _ic = new IntegerConversion();

    public HyracksL1Plan(String dataPath, String extension, Map conf){

            //-------------------------------------------------------------------------------------
            ProjectOperator projectionOrders = new ProjectOperator(new int[]{1});
            List<Integer> hashOrders = Arrays.asList(0);
            DataSourceComponent relationOrders = new DataSourceComponent(
                                            "ORDERS",
                                            dataPath + "orders" + extension,
                                            _queryPlan).setHashIndexes(hashOrders)
                                                       .setPrintOut(false);
                                                       

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
    
}