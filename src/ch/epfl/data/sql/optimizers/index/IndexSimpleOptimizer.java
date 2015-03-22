package ch.epfl.data.sql.optimizers.index;

import java.beans.Expression;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.OperatorComponent;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.DeepCopy;
import ch.epfl.data.sql.optimizers.Optimizer;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.sql.visitors.squall.IndexSelectItemsVisitor;
import ch.epfl.data.sql.visitors.squall.IndexWhereVisitor;

/*
 * Generate a query plan as it was parsed from the SQL.
 * SELECT and WHERE clause are attached to the final component.
 */
public class IndexSimpleOptimizer implements Optimizer {
	private final SQLVisitor _pq;
	private final Schema _schema;
	private final Map _map;

	private IndexCompGen _cg;
	private final IndexTranslator _it;

	public IndexSimpleOptimizer(Map map) {
		_map = map;
		_pq = ParserUtil.parseQuery(map);

		_schema = new Schema(map);
		_it = new IndexTranslator(_schema, _pq.getTan());
	}

	private void attachSelectClause(List<AggregateOperator> aggOps,
			List<ValueExpression> groupByVEs, Component affectedComponent) {
		if (aggOps.isEmpty()) {
			final ProjectOperator project = new ProjectOperator(groupByVEs);
			affectedComponent.add(project);
		} else if (aggOps.size() == 1) {
			// all the others are group by
			final AggregateOperator firstAgg = aggOps.get(0);

			if (ParserUtil.isAllColumnRefs(groupByVEs)) {
				// plain fields in select
				final List<Integer> groupByColumns = ParserUtil
						.extractColumnIndexes(groupByVEs);
				firstAgg.setGroupByColumns(groupByColumns);

				// Setting new level of components is necessary for correctness
				// only for distinct in aggregates
				// but it's certainly pleasant to have the final result grouped
				// on nodes by group by columns.
				final boolean newLevel = !(_it.isHashedBy(affectedComponent,
						groupByColumns));
				if (newLevel) {
					affectedComponent.setOutputPartKey(groupByColumns);
					OperatorComponent oc = new OperatorComponent(
							affectedComponent,
							ParserUtil.generateUniqueName("OPERATOR"))
							.add(firstAgg);
					_cg.getQueryBuilder().add(oc);
				} else
					affectedComponent.add(firstAgg);
			} else {
				// Sometimes groupByVEs contains other functions, so we have to
				// use projections instead of simple groupBy
				// always new level

				if (affectedComponent.getHashExpressions() != null
						&& !affectedComponent.getHashExpressions().isEmpty())
					throw new RuntimeException(
							"Too complex: cannot have hashExpression both for joinCondition and groupBy!");

				// WARNING: groupByVEs cannot be used on two places: that's why
				// we do deep copy
				final ProjectOperator groupByProj = new ProjectOperator(
						(List<ValueExpression>) DeepCopy.copy(groupByVEs));
				if (!(groupByProj.getExpressions() == null || groupByProj
						.getExpressions().isEmpty()))
					firstAgg.setGroupByProjection(groupByProj);

				// current component
				affectedComponent
						.setHashExpressions((List<ValueExpression>) DeepCopy
								.copy(groupByVEs));

				OperatorComponent oc = new OperatorComponent(affectedComponent,
						ParserUtil.generateUniqueName("OPERATOR"))
						.add(firstAgg);
				_cg.getQueryBuilder().add(oc);

			}
		} else
			throw new RuntimeException(
					"For now only one aggregate function supported!");
	}

	private void attachWhereClause(SelectOperator select,
			Component affectedComponent) {
		affectedComponent.add(select);
	}

	@Override
	public QueryBuilder generate() {
		_cg = generateTableJoins();

		// selectItems might add OperatorComponent, this is why it goes first
		processSelectClause(_pq.getSelectItems());
		processWhereClause(_pq.getWhereExpr());

		ParserUtil.orderOperators(_cg.getQueryBuilder());

		final RuleParallelismAssigner parAssign = new RuleParallelismAssigner(
				_cg.getQueryBuilder(), _pq.getTan(), _schema, _map);
		parAssign.assignPar();

		return _cg.getQueryBuilder();
	}

	private IndexCompGen generateTableJoins() {
		final List<Table> tableList = _pq.getTableList();

		final IndexCompGen cg = new IndexCompGen(_schema, _pq, _map);
		Component firstParent = cg.generateDataSource(ParserUtil
				.getComponentName(tableList.get(0)));

		// a special case
		if (tableList.size() == 1)
			return cg;

		// This generates a lefty query plan.
		for (int i = 0; i < tableList.size() - 1; i++) {
			final DataSourceComponent secondParent = cg
					.generateDataSource(ParserUtil.getComponentName(tableList
							.get(i + 1)));
			firstParent = cg.generateEquiJoin(firstParent, secondParent);
		}
		return cg;
	}

	private int processSelectClause(List<SelectItem> selectItems) {
		final IndexSelectItemsVisitor selectVisitor = new IndexSelectItemsVisitor(
				_cg.getQueryBuilder(), _schema, _pq.getTan(), _map);
		for (final SelectItem elem : selectItems)
			elem.accept(selectVisitor);
		final List<AggregateOperator> aggOps = selectVisitor.getAggOps();
		final List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();

		final Component affectedComponent = _cg.getQueryBuilder()
				.getLastComponent();
		attachSelectClause(aggOps, groupByVEs, affectedComponent);
		return (aggOps.isEmpty() ? IndexSelectItemsVisitor.NON_AGG
				: IndexSelectItemsVisitor.AGG);
	}

	private void processWhereClause(Expression whereExpr) {
		// all the selection are performed on the last component
		final Component affectedComponent = _cg.getQueryBuilder()
				.getLastComponent();
		final IndexWhereVisitor whereVisitor = new IndexWhereVisitor(
				affectedComponent, _schema, _pq.getTan());
		if (whereExpr != null) {
			whereExpr.accept(whereVisitor);
			attachWhereClause(whereVisitor.getSelectOperator(),
					affectedComponent);
		}
	}

}
