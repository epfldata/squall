package sql.optimizers;

import plan_runner.query_plans.QueryBuilder;

public interface Optimizer {

	public QueryBuilder generate();

}