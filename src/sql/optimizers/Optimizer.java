package sql.optimizers;

import plan_runner.query_plans.QueryPlan;

public interface Optimizer {

	public QueryPlan generate();

}