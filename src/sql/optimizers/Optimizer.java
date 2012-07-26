package sql.optimizers;

import plan_runner.queryPlans.QueryPlan;


public interface Optimizer {

    public QueryPlan generate();

}