package optimizers;

import queryPlans.QueryPlan;


public interface Optimizer {

    public QueryPlan generate();

}