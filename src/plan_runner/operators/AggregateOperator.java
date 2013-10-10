package plan_runner.operators;

import java.util.List;

import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.storage.BasicStore;

public interface AggregateOperator<T> extends Operator {
	public void clearStorage();

	public DistinctOperator getDistinct();

	// this is null for AggregateCountOperator
	public List<ValueExpression> getExpressions();

	public List<Integer> getGroupByColumns();

	public ProjectOperator getGroupByProjection();

	// internal storage class
	public BasicStore getStorage();

	public TypeConversion getType();

	public boolean hasGroupBy();

	public T runAggregateFunction(T value, List<String> tuple);

	public T runAggregateFunction(T value1, T value2);

	// SUM(DISTINCT ValueExpression), COUNT(DISTINCT ValueExpression): a single
	// ValueExpression by SQL standard
	// MySQL supports multiple ValueExpression. Inside aggregation(SUM, COUNT),
	// there must be single ValueExpression.
	public AggregateOperator setDistinct(DistinctOperator distinct);

	// GROUP BY ValueExpression is not part of the SQL standard, only columns
	// can be sed.
	public AggregateOperator setGroupByColumns(List<Integer> groupByColumns);

	public AggregateOperator setGroupByProjection(ProjectOperator projection);

	// HAVING clause: Since HAVING depends on aggregate result,
	// it cannot be evaluated before all the tuples are processed.
	// This will be done by user manually, as well as ORDER BY clause.

	// COUNT(COLUMN_NAME): an AggregateCountOperator *preceeded* by a
	// SelectionOperator.
	// this counts non-null COLUMN_NAMEs
	// due to the order of operators, this is not doable in our system

}
