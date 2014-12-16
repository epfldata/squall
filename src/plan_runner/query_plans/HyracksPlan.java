package plan_runner.query_plans;

import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.conversion.IntegerConversion;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;

public class HyracksPlan {
	
	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public HyracksPlan(Map conf) {
		// -------------------------------------------------------------------------------------
		Component relationCustomer = _queryBuilder.createDataSource("customer", conf)
				.add(new ProjectOperator(0, 6))
				.setOutputPartKey(0);

		// -------------------------------------------------------------------------------------
		Component relationOrders = _queryBuilder.createDataSource("orders", conf)
				.add(new ProjectOperator(1))
				.setOutputPartKey(0);

		// -------------------------------------------------------------------------------------
		Component joinCustOrders = _queryBuilder.createEquiJoin(relationCustomer, relationOrders)
				.add(new AggregateCountOperator(conf).setGroupByColumns(1));
		// -------------------------------------------------------------------------------------
	}

	public QueryBuilder getQueryBuilder() {
		return _queryBuilder;
	}
}