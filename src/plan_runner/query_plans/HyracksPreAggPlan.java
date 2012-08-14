package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.storage.BasicStore;

public class HyracksPreAggPlan {
	private static Logger LOG = Logger.getLogger(HyracksPreAggPlan.class);

	private QueryPlan _queryPlan = new QueryPlan();

	private static final DoubleConversion _dc = new DoubleConversion();
	private static final StringConversion _sc = new StringConversion();

	public HyracksPreAggPlan(String dataPath, String extension, Map conf){
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
		ProjectOperator projFirstOut = new ProjectOperator(
				new ColumnReference(_sc, 1),
				new ValueSpecification(_sc, "1"));
		ProjectOperator projSecondOut = new ProjectOperator(new int[]{1, 2});
		// FIXME FIXME FIXME: This should be as below, but there is an incombatibility with current version.
		// Will be eventually, but for not don't run this query
		// JoinAggStorage secondJoinStorage = new JoinAggStorage(new AggregateCountOperator(conf), conf);
		BasicStore secondJoinStorage = null;

		List<Integer> hashIndexes = Arrays.asList(0);
		EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
				relationCustomer,
				relationOrders,
				_queryPlan).setFirstPreAggProj(projFirstOut)
                                           .setSecondPreAggProj(projSecondOut)
                                           .setSecondPreAggStorage(secondJoinStorage)
                                           .setHashIndexes(hashIndexes);

		//-------------------------------------------------------------------------------------           
		AggregateSumOperator agg = new AggregateSumOperator(new ColumnReference(_dc, 1), conf)
			.setGroupByColumns(Arrays.asList(0));

		OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG", _queryPlan)
			.addOperator(agg);

		//-------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}

}
