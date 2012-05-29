/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.EquiJoinComponent;
import components.OperatorComponent;
import conversion.DoubleConversion;
import conversion.StringConversion;
import expressions.ColumnReference;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectOperator;

import org.apache.log4j.Logger;
import schema.TPCH_Schema;
import storage.SquallStorage;

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
				TPCH_Schema.customer,
				_queryPlan).addOperator(projectionCustomer)
                                           .setHashIndexes(hashCustomer);

		//-------------------------------------------------------------------------------------
		ProjectOperator projectionOrders = new ProjectOperator(new int[]{1});
		List<Integer> hashOrders = Arrays.asList(0);
		DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS",
				dataPath + "orders" + extension,
				TPCH_Schema.orders,
				_queryPlan).addOperator(projectionOrders)
			                   .setHashIndexes(hashOrders);

		//-------------------------------------------------------------------------------------
		ProjectOperator projFirstOut = new ProjectOperator(
				new ColumnReference(_sc, 1),
				new ValueSpecification(_sc, "1"));
		ProjectOperator projSecondOut = new ProjectOperator(new int[]{1, 2});
		// FIXME FIXME FIXME: This should be as below, but there is an incombatibility with SquallStorage
		// Replace with wrong SquallStorage instantiation, but will be fixed eventually
		// JoinAggStorage secondJoinStorage = new JoinAggStorage(new AggregateCountOperator(conf), conf);
		SquallStorage secondJoinStorage = new SquallStorage();

		List<Integer> hashIndexes = Arrays.asList(0);
		EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
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
			.addOperator(agg);

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
