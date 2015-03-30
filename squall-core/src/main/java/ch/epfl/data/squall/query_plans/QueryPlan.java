package ch.epfl.data.squall.query_plans;

import java.util.Map;

import ch.epfl.data.squall.components.Component;

public abstract class QueryPlan {
	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public QueryPlan(String dataPath, String extension, Map conf) {
          build(createQueryPlan(dataPath, extension, conf));
	}

	public QueryPlan() { }

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
	// TODO: put it back to abstract and update plans
	//public abstract Component createQueryPlan(String dataPath, String extension, Map conf);
	public Component createQueryPlan(String dataPath, String extension, Map conf) {
		return null;
	}

	// QueryBuilder is expected from the outside world
	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
