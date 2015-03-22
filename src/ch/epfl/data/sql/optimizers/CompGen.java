package ch.epfl.data.sql.optimizers;

import java.util.List;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;

public interface CompGen {

	/*
	 * adding a DataSourceComponent to the list of components Necessary to call
	 * only when only one table is addresses in WHERE clause of a SQL query
	 */
	public DataSourceComponent generateDataSource(String tableCompName);

	/*
	 * Join between two components
	 */
	public Component generateEquiJoin(Component left, Component right);

	public QueryBuilder getQueryBuilder();

	public List<Component> getSubPlans();

}
