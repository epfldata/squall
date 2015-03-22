package ch.epfl.data.plan_runner.query_plans;

import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;

public abstract class QueryPlan {
	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public QueryPlan(Map conf) {
		build(createQueryPlan(conf));
	}

	// _queryBuilder expects components in the parent->child order
	// root is the leaf child
	protected void build(Component root) {
		if (root == null)
			return;
		Component[] parents = root.getParents();
		if (parents != null) {
			for (Component parent : parents) {
				build(parent);
			}
		}
		_queryBuilder.add(root);
	}

	// This returns the last component: it assumes there is only one last
	// component
	public abstract Component createQueryPlan(Map conf);

	// QueryBuilder is expected from the outside world
	public QueryBuilder getQueryBuilder() {
		return _queryBuilder;
	}
}