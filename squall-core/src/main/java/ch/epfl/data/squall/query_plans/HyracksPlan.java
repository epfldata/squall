package ch.epfl.data.squall.query_plans;

import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;

public class HyracksPlan extends QueryPlan {

	public HyracksPlan(String dataPath, String extension, Map conf) {
          super(dataPath, extension, conf);
	}

	@Override
	public Component createQueryPlan(String dataPath, String extension, Map conf) {
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
