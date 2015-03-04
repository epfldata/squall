package sql.visitors.squall;

import java.util.ArrayList;
import java.util.List;

import plan_runner.components.Component;
import plan_runner.ewh.operators.SampleAsideAndForwardOperator;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.DistinctOperator;
import plan_runner.operators.Operator;
import plan_runner.operators.PrintOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SampleOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.Predicate;
import plan_runner.visitors.OperatorVisitor;

/*
 * Collects all the VE inside a component.
 * Used only from EarlyProjection(sql.optimizers.index package).
 */
public class VECollectVisitor implements OperatorVisitor {
	private final List<ValueExpression> _veList = new ArrayList<ValueExpression>();
	private final List<ValueExpression> _afterProjection = new ArrayList<ValueExpression>();
	private final List<ValueExpression> _beforeProjection = new ArrayList<ValueExpression>();

	public List<ValueExpression> getAfterProjExpressions() {
		return _afterProjection;
	}

	public List<ValueExpression> getAllExpressions() {
		return _veList;
	}

	public List<ValueExpression> getBeforeProjExpressions() {
		return _beforeProjection;
	}

	@Override
	public void visit(AggregateOperator aggregation) {
		if (aggregation != null) {
			final DistinctOperator distinct = aggregation.getDistinct();
			if (distinct != null)
				visitNested(aggregation.getDistinct());
			if (aggregation.getGroupByProjection() != null) {
				_afterProjection.addAll(aggregation.getGroupByProjection().getExpressions());
				_veList.addAll(aggregation.getGroupByProjection().getExpressions());
			}
			_afterProjection.addAll(aggregation.getExpressions());
			_veList.addAll(aggregation.getExpressions());
		}
	}

	@Override
	public void visit(ChainOperator chain) {
		for (final Operator op : chain.getOperators())
			op.accept(this);
	}

	public void visit(Component component) {
		final List<ValueExpression> hashExpressions = component.getHashExpressions();
		if (hashExpressions != null) {
			_afterProjection.addAll(hashExpressions);
			_veList.addAll(hashExpressions);
		}

		final List<Operator> operators = component.getChainOperator().getOperators();
		for (final Operator op : operators)
			op.accept(this);
	}

	// because it changes the output of the component
	@Override
	public void visit(DistinctOperator distinct) {
		throw new RuntimeException(
				"EarlyProjection cannon work if in bottom-up phase encounter Distinct!");
	}

	// unsupported
	// because we assing by ourselves to projection
	@Override
	public void visit(ProjectOperator projection) {
		// ignored because of topDown - makes no harm
	}

	@Override
	public void visit(SelectOperator selection) {
		final Predicate predicate = selection.getPredicate();
		final VECollectPredVisitor vecpv = new VECollectPredVisitor();
		predicate.accept(vecpv);
		_beforeProjection.addAll(vecpv.getExpressions());
		_veList.addAll(vecpv.getExpressions());
	}

	private void visitNested(DistinctOperator distinct) {
		final ProjectOperator project = distinct.getProjection();
		if (project != null)
			visitNested(project);
	}

	private void visitNested(ProjectOperator projection) {
		if (projection != null) {
			_afterProjection.addAll(projection.getExpressions());
			_veList.addAll(projection.getExpressions());
		}
	}

	@Override
	public void visit(PrintOperator printOperator) {
		// nothing to visit or add
	}

	@Override
	public void visit(SampleOperator sampleOperator) {
		// nothing to visit or add
	}

	@Override
	public void visit(SampleAsideAndForwardOperator sampleAsideAndForwardOperator) {
		// nothing to visit or add
	}
}