package ch.epfl.data.squall.api.sql.optimizers;

import java.util.List;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.query_plans.QueryBuilder;

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
