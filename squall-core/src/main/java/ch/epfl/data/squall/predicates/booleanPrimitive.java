package ch.epfl.data.squall.predicates;

import java.util.List;

import ch.epfl.data.squall.visitors.PredicateVisitor;

public class booleanPrimitive implements Predicate {

	private boolean _bool;

	public booleanPrimitive(boolean bool) {
		_bool = bool;
	}

	@Override
	public void accept(PredicateVisitor pv) {
		pv.visit(this);

	}

	@Override
	public List<Predicate> getInnerPredicates() {
		return null;
	}

	@Override
	public boolean test(List<String> tupleValues) {
		return _bool;
	}

	@Override
	public boolean test(List<String> firstTupleValues,
			List<String> secondTupleValues) {
		return _bool;
	}

}
