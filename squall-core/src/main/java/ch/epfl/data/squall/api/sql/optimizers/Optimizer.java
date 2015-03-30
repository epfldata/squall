package ch.epfl.data.squall.api.sql.optimizers;

import ch.epfl.data.squall.query_plans.QueryBuilder;

public interface Optimizer {

	public QueryBuilder generate();

}