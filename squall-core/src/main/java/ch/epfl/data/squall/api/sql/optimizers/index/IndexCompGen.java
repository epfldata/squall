package ch.epfl.data.squall.api.sql.optimizers.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import ch.epfl.data.squall.api.sql.optimizers.CompGen;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.squall.api.sql.visitors.squall.IndexJoinHashVisitor;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.utilities.SystemParameters;

/*
 * It is necessary that this class operates with Tables,
 *   since we don't want multiple CG sharing the same copy of DataSourceComponent.
 */
public class IndexCompGen implements CompGen {
	private final SQLVisitor _pq;

	private final Schema _schema;
	private final String _dataPath;
	private final String _extension;

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	// List of Components which are already added throughEquiJoinComponent and
	// OperatorComponent
	private final List<Component> _subPlans = new ArrayList<Component>();

	public IndexCompGen(Schema schema, SQLVisitor pq, Map map) {
		_schema = schema;
		_pq = pq;
		_dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
		_extension = SystemParameters.getString(map, "DIP_EXTENSION");
	}

	// set hash for this component, knowing its position in the query plan.
	// Conditions are related only to parents of join,
	// but we have to filter who belongs to my branch in IndexJoinHashVisitor.
	// We don't want to hash on something which will be used to join with same
	// later component in the hierarchy.
	private void addHash(Component component, List<Expression> joinCondition) {
		final IndexJoinHashVisitor joinOn = new IndexJoinHashVisitor(_schema,
				component, _pq.getTan());
		for (final Expression exp : joinCondition)
			exp.accept(joinOn);
		final List<ValueExpression> hashExpressions = joinOn.getExpressions();

		if (ParserUtil.isAllColumnRefs(hashExpressions)) {
			// all the join conditions are represented through columns, no
			// ValueExpression (neither in joined component)
			// guaranteed that both joined components will have joined columns
			// visited in the same order
			// i.e R.A=S.A and R.B=S.B, the columns are (R.A, R.B), (S.A, S.B),
			// respectively
			final List<Integer> hashIndexes = ParserUtil
					.extractColumnIndexes(hashExpressions);

			// hash indexes in join condition
			component.setOutputPartKey(hashIndexes);
		} else
			// hahs expressions in join condition
			component.setHashExpressions(hashExpressions);
	}

	/*
	 * adding a DataSourceComponent to the list of components
	 */
	@Override
	public DataSourceComponent generateDataSource(String tableCompName) {
		final String tableSchemaName = _pq.getTan()
				.getSchemaName(tableCompName);
		final String sourceFile = tableSchemaName.toLowerCase();

		final DataSourceComponent relation = new DataSourceComponent(
				tableCompName, _dataPath + sourceFile + _extension);
		_queryBuilder.add(relation);
		_subPlans.add(relation);
		return relation;
	}

	/*
	 * Join between two components List<Expression> is a set of join conditions
	 * between two components.
	 */
	@Override
	public Component generateEquiJoin(Component left, Component right) {
		final EquiJoinComponent joinComponent = new EquiJoinComponent(left,
				right);
		_queryBuilder.add(joinComponent);

		// compute join condition
		final List<Expression> joinCondition = ParserUtil.getJoinCondition(_pq,
				left, right);
		if (joinCondition == null)
			throw new RuntimeException(
					"There is no join conditition between components "
							+ left.getName() + " and " + right.getName());

		// set hashes for two parents
		addHash(left, joinCondition);
		addHash(right, joinCondition);

		_subPlans.remove(left);
		_subPlans.remove(right);
		_subPlans.add(joinComponent);

		return joinComponent;
	}

	@Override
	public QueryBuilder getQueryBuilder() {
		return _queryBuilder;
	}

	@Override
	public List<Component> getSubPlans() {
		return _subPlans;
	}

}