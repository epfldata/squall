package ch.epfl.data.squall.api.sql.visitors.squall;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.LikePredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.predicates.booleanPrimitive;
import ch.epfl.data.squall.visitors.PredicateVisitor;

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
	public void visit(booleanPrimitive bool) {

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
