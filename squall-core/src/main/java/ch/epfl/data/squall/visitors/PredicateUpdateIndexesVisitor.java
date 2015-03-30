package ch.epfl.data.squall.visitors;

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

public class PredicateUpdateIndexesVisitor implements PredicateVisitor {

	private final List<String> _tuple;

	private final boolean _comeFromFirstEmitter;

	public ArrayList<String> _valuesToIndex;
	public ArrayList<Object> _typesOfValuesToIndex;

	public PredicateUpdateIndexesVisitor(boolean comeFromFirstEmitter,
			List<String> tuple) {
		_comeFromFirstEmitter = comeFromFirstEmitter;
		_tuple = tuple;

		_valuesToIndex = new ArrayList<String>();
		_typesOfValuesToIndex = new ArrayList<Object>();
	}

	@Override
	public void visit(AndPredicate and) {
		for (final Predicate pred : and.getInnerPredicates())
			visit(pred);
	}

	@Override
	public void visit(BetweenPredicate between) {
		// In between there is only an and predicate
		final Predicate p = (Predicate) between.getInnerPredicates().get(0);
		visit(p);
	}

	@Override
	public void visit(booleanPrimitive bool) {

	}

	@Override
	public void visit(ComparisonPredicate comparison) {
		ValueExpression val;

		if (_comeFromFirstEmitter)
			val = (ValueExpression) comparison.getExpressions().get(0);
		else
			val = (ValueExpression) comparison.getExpressions().get(1);

		_valuesToIndex.add(val.eval(_tuple).toString());
		_typesOfValuesToIndex.add(val.getType().getInitialValue());
	}

	@Override
	public void visit(LikePredicate like) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(OrPredicate or) {
		for (final Predicate pred : or.getInnerPredicates())
			visit(pred);
	}

	public void visit(Predicate pred) {
		pred.accept(this);
	}

}
