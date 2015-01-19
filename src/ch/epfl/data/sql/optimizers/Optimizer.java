package ch.epfl.data.sql.optimizers;

import ch.epfl.data.plan_runner.query_plans.QueryBuilder;

public interface Optimizer {

    public QueryBuilder generate();

}