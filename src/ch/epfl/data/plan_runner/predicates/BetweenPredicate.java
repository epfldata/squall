package ch.epfl.data.plan_runner.predicates;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.visitors.PredicateVisitor;

/* This class is syntactic sugar for complex AndPredicate
 */
public class BetweenPredicate<T extends Comparable<T>> implements Predicate {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger LOG = Logger.getLogger(BetweenPredicate.class);

	private final Predicate _and;

	public BetweenPredicate(ValueExpression<T> ve, boolean includeLower,
			ValueExpression<T> veLower, boolean includeUpper, ValueExpression<T> veUpper) {

		// set up boundaries correctly
		int opLower = ComparisonPredicate.GREATER_OP;
		if (includeLower)
			opLower = ComparisonPredicate.NONLESS_OP;
		int opUpper = ComparisonPredicate.LESS_OP;
		if (includeUpper)
			opUpper = ComparisonPredicate.NONGREATER_OP;

		// create syntactic sugar
		final Predicate lower = new ComparisonPredicate(opLower, ve, veLower);
		final Predicate upper = new ComparisonPredicate(opUpper, ve, veUpper);
		_and = new AndPredicate(lower, upper);
	}

	@Override
	public void accept(PredicateVisitor pv) {
		pv.visit(this);
	}

	@Override
	public List<Predicate> getInnerPredicates() {
		final List<Predicate> result = new ArrayList<Predicate>();
		result.add(_and);
		return result;
	}

	@Override
	public boolean test(List<String> tupleValues) {
		return _and.test(tupleValues);
	}

	@Override
	public boolean test(List<String> firstTupleValues, List<String> secondTupleValues) {
		return _and.test(firstTupleValues, secondTupleValues);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("BETWEEN implemented as AND: ").append(_and.toString());
		return sb.toString();
	}

}