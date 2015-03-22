package ch.epfl.data.plan_runner.query_plans;

import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.operators.AggregateCountOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;

public class HyracksPlan extends QueryPlan {

	public HyracksPlan(Map conf) {
		super(conf);
	}

	@Override
	public Component createQueryPlan(Map conf) {
		// -------------------------------------------------------------------------------------
		Component customer = new DataSourceComponent("customer", conf)
				.add(new ProjectOperator(0, 6));

		// -------------------------------------------------------------------------------------
		Component orders = new DataSourceComponent("orders", conf)
				.add(new ProjectOperator(1));

		// -------------------------------------------------------------------------------------
		Component custOrders = new EquiJoinComponent(customer, 0, orders, 0)
				.add(new AggregateCountOperator(conf).setGroupByColumns(1));
		return custOrders;
		// -------------------------------------------------------------------------------------
	}

}