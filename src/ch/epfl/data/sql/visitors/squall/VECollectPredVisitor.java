package ch.epfl.data.sql.visitors.squall;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.predicates.AndPredicate;
import ch.epfl.data.plan_runner.predicates.BetweenPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.LikePredicate;
import ch.epfl.data.plan_runner.predicates.OrPredicate;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.visitors.PredicateVisitor;

public class VECollectPredVisitor implements PredicateVisitor {
	private final List<ValueExpression> _veList = new ArrayList<ValueExpression>();

	public List<ValueExpression> getExpressions() {
		return _veList;
	}

	@Override
	public void visit(AndPredicate and) {
		visit(and.getInnerPredicates());
	}

	@Override
	public void visit(BetweenPredicate between) {
		visit(between.getInnerPredicates());
	}

	@Override
	public void visit(ComparisonPredicate comparison) {
		_veList.addAll(comparison.getExpressions());
	}

	@Override
	public void visit(LikePredicate like) {
		_veList.addAll(like.getExpressions());
	}

	private void visit(List<Predicate> predList) {
		for (final Predicate pred : predList)
			pred.accept(this);
	}

	@Override
	public void visit(OrPredicate or) {
		visit(or.getInnerPredicates());
	}

}
